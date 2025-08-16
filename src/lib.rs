//! Hierarchical read-write lock.
//!
//! # Usage
//!
//! Given a hierarchical path defined as a sequence of progressively nested segments separated by
//! slashes—as in `a/b/c` where `c` is nested in `a` and `b` and `b` in `a`—the locking mechanism
//! ensures that
//!
//! * a writer is given access to a segment only when the segment is not currently being read from
//!   or written into and is not nested in a segment that is currently being written into, and that
//!
//! * a reader is given access to a segment only when the segment is not currently being written
//!   into and is not nested in a segment that is currently being written into.
//!
//! For instance, one can concurrently write into `a/b/c` and `a/b/d` and read from `a` and `a/b`.
//! However, reading from or writing into `a/b/c` or `a/b/d` would have to wait for `a/b` if the
//! latter was acquired for writing, but one could freely read from `a`.

use std::sync::Arc;

use papaya::HashMap;
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

const SEPARATOR: u8 = b'/';

/// A lock.
#[derive(Default)]
pub struct Lock {
    inner: HashMap<Vec<u8>, Arc<RwLock<()>>>,
}

/// A guard.
pub enum Guard {
    Read(ReadGuard),
    Write(WriteGuard),
}

/// A read guard.
pub type ReadGuard = OwnedRwLockReadGuard<()>;

/// A write guard.
pub type WriteGuard = OwnedRwLockWriteGuard<()>;

impl Lock {
    /// Acquire the lock for reading.
    pub async fn read<T: AsRef<[u8]>>(&self, path: T) -> Vec<Guard> {
        let path = path.as_ref();
        let paths = partition(path);
        let mut guards = Vec::with_capacity(paths.len());
        for path in paths {
            let lock = self
                .inner
                .pin()
                .get_or_insert_with(path, Default::default)
                .clone();
            guards.push(Guard::Read(lock.read_owned().await));
        }
        guards
    }

    /// Acquire the lock for writing.
    pub async fn write<T: AsRef<[u8]>>(&self, path: T) -> Vec<Guard> {
        let path = path.as_ref();
        let paths = partition(path);
        let mut guards = Vec::with_capacity(paths.len());
        for (index, path) in paths.into_iter().enumerate() {
            let lock = self
                .inner
                .pin()
                .get_or_insert_with(path, Default::default)
                .clone();
            if index == 0 {
                guards.push(Guard::Write(lock.write_owned().await));
            } else {
                guards.push(Guard::Read(lock.read_owned().await));
            }
        }
        guards
    }
}

impl Guard {
    /// Downgrade to reading.
    pub fn downgrade(self) -> Self {
        match self {
            Self::Read(guard) => Self::Read(guard),
            Self::Write(guard) => Self::Read(guard.downgrade()),
        }
    }
}

fn partition(value: &[u8]) -> Vec<Vec<u8>> {
    let count = value.len();
    value
        .iter()
        .rev()
        .enumerate()
        .filter_map(|(index, character)| {
            if index == 0 {
                Some(value.to_vec())
            } else if *character == SEPARATOR {
                Some((&value[..(count - index - 1)]).to_vec())
            } else {
                None
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::Lock;

    macro_rules! ok(($result:expr) => ($result.unwrap()));

    #[tokio::test(flavor = "multi_thread")]
    async fn read_independent() {
        let lock = Lock::default();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();

        {
            let guard = lock.read("a/b/c").await;
            let sender = sender.clone();
            tokio::task::spawn(async move {
                work(1).await;
                ok!(sender.send(2));
                std::mem::drop(guard);
            });
        }

        {
            let sender = sender.clone();
            tokio::task::spawn(async move {
                let _guard = lock.read("a/b/c").await;
                ok!(sender.send(1));
            });
        }

        for index in [1, 2] {
            assert_eq!(receiver.recv().await, Some(index));
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn write_dependent() {
        let lock = Lock::default();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();

        {
            let guard = lock.write("a/b/c").await;
            let sender = sender.clone();
            tokio::task::spawn(async move {
                work(1).await;
                ok!(sender.send(1));
                std::mem::drop(guard);
            });
        }

        {
            let sender = sender.clone();
            tokio::task::spawn(async move {
                let _guard = lock.write("a/b/c").await;
                ok!(sender.send(2));
            });
        }

        for index in [1, 2] {
            assert_eq!(receiver.recv().await, Some(index));
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn write_independent() {
        let lock = Lock::default();
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();

        {
            let guard = lock.write("a/b/c").await;
            let sender = sender.clone();
            tokio::task::spawn(async move {
                work(1).await;
                ok!(sender.send(2));
                std::mem::drop(guard);
            });
        }

        {
            let sender = sender.clone();
            tokio::task::spawn(async move {
                let _guard = lock.write("a/b/d").await;
                ok!(sender.send(1));
            });
        }

        for index in [1, 2] {
            assert_eq!(receiver.recv().await, Some(index));
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn write_read_independent_dependent() {
        let lock = Arc::new(Lock::default());
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();

        {
            let guard = lock.write("a/b/c").await;
            let sender = sender.clone();
            tokio::task::spawn(async move {
                work(1).await;
                ok!(sender.send(2));
                std::mem::drop(guard);
            });
        }

        {
            let lock = lock.clone();
            let sender = sender.clone();
            tokio::task::spawn(async move {
                let _guard = lock.read("a/b").await;
                ok!(sender.send(1));
            });
        }

        {
            let sender = sender.clone();
            tokio::task::spawn(async move {
                let _guard = lock.read("a/b/c/d").await;
                ok!(sender.send(3));
            });
        }

        for index in [1, 2, 3] {
            assert_eq!(receiver.recv().await, Some(index));
        }
    }

    async fn work(load: u64) {
        tokio::time::sleep(std::time::Duration::from_secs(4 * load)).await;
    }
}
