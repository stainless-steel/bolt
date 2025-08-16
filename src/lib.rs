//! Hierarchical read-write lock.

use std::sync::Arc;

use papaya::HashMap;
use tokio::sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

/// A lock.
#[derive(Default)]
pub struct Lock {
    inner: HashMap<String, Arc<RwLock<()>>>,
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
    pub async fn read<T: Into<String>>(&self, value: T) -> Guard {
        let lock: Arc<RwLock<()>> = self
            .inner
            .pin()
            .get_or_insert_with(value.into(), Default::default)
            .clone();
        Guard::Read(lock.read_owned().await)
    }

    /// Acquire the lock for writing.
    pub async fn write<T: Into<String>>(&self, value: T) -> Guard {
        let lock: Arc<RwLock<()>> = self
            .inner
            .pin()
            .get_or_insert_with(value.into(), Default::default)
            .clone();
        Guard::Write(lock.write_owned().await)
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

#[cfg(test)]
mod tests {
    use super::Lock;

    macro_rules! ok(($result:expr) => ($result.unwrap()));

    #[tokio::test(flavor = "multi_thread")]
    async fn write() {
        let lock = Lock::default();
        let (sender, mut receiver) = tokio::sync::mpsc::channel(2);

        {
            let guard = lock.write("head/body/tail").await;
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
                let _guard = lock.write("head/body/tail").await;
                ok!(sender.send(2).await);
            });
        }

        assert_eq!(receiver.recv().await, Some(1));
        assert_eq!(receiver.recv().await, Some(2));
    }
}
