//! Hierarchical read-write lock.
//!
//! Given a hierarchical path defined as a sequence of progressively nested segments separated by
//! slashes—as in `a/b/c` where `c` is nested in `b` and `a`, and `b` is nested in `a`—the locking
//! mechanism ensures that
//!
//! * a writer is given access to the path only when the last segment is not currently being read
//!   from or written into and is not nested in any segment that is currently being written into,
//!   and that
//!
//! * a reader is given access to the path only when the last segment is not currently being written
//!   into and is not nested in any segment that is currently being written into.
//!
//! For instance, one can concurrently write into `a/b/c` and `a/b/d` and read from `a` and `a/b`.
//! However, reading from or writing into `a/b/c` or `a/b/d` would have to wait for `a/b` if the
//! last was acquired for writing, but one would be able to read from `a`.
//!
//! # Usage
//!
//! ```
//! const N: usize = 10;
//!
//! # #[tokio::main]
//! # async fn main() {
//! let lock = std::sync::Arc::new(bolt::Lock::<N>::default());
//!
//! {
//!     let lock = lock.clone();
//!     tokio::task::spawn(async move {
//!         let _guards = lock.write("a/b/c").await;
//!     });
//! }
//!
//! {
//!     let lock = lock.clone();
//!     tokio::task::spawn(async move {
//!         let _guards = lock.write("a/b/d").await;
//!     });
//! }
//! # }
//! ```

use std::sync::Arc;

use papaya::HashMap;
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

const SEPARATOR: u8 = b'/';

/// A lock.
///
/// The constant generic parameter gives an upper bound on the number of segments to consider
/// counting from the tail. This is to avoid memory allocation when locking. For instance, given
/// `a/b/c`, `Lock::<2>` would protect `a/b/c` and `a/b` but not `a`.
#[derive(Default)]
pub struct Lock<const N: usize> {
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

impl<const N: usize> Lock<N> {
    /// Acquire the lock for reading.
    pub async fn read<T: AsRef<[u8]>>(&self, path: T) -> [Option<Guard>; N] {
        let mut guards: [Option<Guard>; N] = [const { None }; N];
        for (index, path) in partition(path.as_ref()).take(N).enumerate() {
            let lock = self.lock(path);
            guards[index] = Some(Guard::Read(lock.read_owned().await));
        }
        guards
    }

    /// Acquire the lock for writing.
    pub async fn write<T: AsRef<[u8]>>(&self, path: T) -> [Option<Guard>; N] {
        let mut guards: [Option<Guard>; N] = [const { None }; N];
        for (index, path) in partition(path.as_ref()).take(N).enumerate() {
            let lock = self.lock(path);
            if index == 0 {
                guards[index] = Some(Guard::Write(lock.write_owned().await));
            } else {
                guards[index] = Some(Guard::Read(lock.read_owned().await));
            }
        }
        guards
    }

    fn lock(&self, path: &[u8]) -> Arc<RwLock<()>> {
        match self.inner.pin().get(path).cloned() {
            Some(value) => value,
            _ => {
                let path = path.to_vec();
                self.inner
                    .pin()
                    .get_or_insert_with(path, Default::default)
                    .clone()
            }
        }
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

fn partition(value: &[u8]) -> impl Iterator<Item = &[u8]> {
    let count = value.len();
    value
        .iter()
        .rev()
        .enumerate()
        .filter_map(move |(index, character)| {
            if index == 0 {
                Some(value)
            } else if *character == SEPARATOR {
                Some(&value[..(count - index - 1)])
            } else {
                None
            }
        })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::Lock;

    const N: usize = 10;

    macro_rules! ok(($result:expr) => ($result.unwrap()));

    #[tokio::test(flavor = "multi_thread")]
    async fn read_independent() {
        let lock = Lock::<N>::default();
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
        let lock = Lock::<N>::default();
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
        let lock = Lock::<N>::default();
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
        let lock = Arc::new(Lock::<N>::default());
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
