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
            if index == 0 || *character == SEPARATOR {
                Some((&value[..(count - index)]).to_vec())
            } else {
                None
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::Lock;

    macro_rules! ok(($result:expr) => ($result.unwrap()));

    #[tokio::test(flavor = "multi_thread")]
    async fn two_dependent_readers() {
        let lock = Lock::default();
        let (sender, mut receiver) = tokio::sync::mpsc::channel(2);

        {
            let guard = lock.read("a/b/c").await;
            let sender = sender.clone();
            tokio::task::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                ok!(sender.send(2).await);
                std::mem::drop(guard);
            });
        }

        {
            let sender = sender.clone();
            tokio::task::spawn(async move {
                let _guard = lock.read("a/b/c").await;
                ok!(sender.send(1).await);
            });
        }

        for index in [1, 2] {
            assert_eq!(receiver.recv().await, Some(index));
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn two_dependent_writers() {
        let lock = Lock::default();
        let (sender, mut receiver) = tokio::sync::mpsc::channel(2);

        {
            let guard = lock.write("a/b/c").await;
            let sender = sender.clone();
            tokio::task::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                ok!(sender.send(1).await);
                std::mem::drop(guard);
            });
        }

        {
            let sender = sender.clone();
            tokio::task::spawn(async move {
                let _guard = lock.write("a/b/c").await;
                ok!(sender.send(2).await);
            });
        }

        for index in [1, 2] {
            assert_eq!(receiver.recv().await, Some(index));
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn two_independent_writers() {
        let lock = Lock::default();
        let (sender, mut receiver) = tokio::sync::mpsc::channel(2);

        {
            let guard = lock.write("a/b/c").await;
            let sender = sender.clone();
            tokio::task::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                ok!(sender.send(2).await);
                std::mem::drop(guard);
            });
        }

        {
            let sender = sender.clone();
            tokio::task::spawn(async move {
                let _guard = lock.write("a/b/d").await;
                ok!(sender.send(1).await);
            });
        }

        for index in [1, 2] {
            assert_eq!(receiver.recv().await, Some(index));
        }
    }
}
