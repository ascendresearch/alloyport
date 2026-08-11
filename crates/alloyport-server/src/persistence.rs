//! Bounded isolation for synchronous persistence adapters used by async services.

use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;
use tokio::sync::Semaphore;

use crate::storage::RepositoryError;

const MAX_BLOCKING_PERSISTENCE_OPERATIONS: usize = 8;

#[derive(Clone, Debug)]
pub(crate) struct ServerPersistence {
    permits: Arc<Semaphore>,
}

impl Default for ServerPersistence {
    fn default() -> Self {
        Self {
            permits: Arc::new(Semaphore::new(MAX_BLOCKING_PERSISTENCE_OPERATIONS)),
        }
    }
}

impl ServerPersistence {
    pub(crate) async fn run<T, F>(&self, operation: F) -> Result<T, PersistenceTaskError>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        let permit = Arc::clone(&self.permits)
            .acquire_owned()
            .await
            .map_err(|_| PersistenceTaskError::Closed)?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            operation()
        })
        .await
        .map_err(PersistenceTaskError::Join)
    }
}

#[derive(Debug)]
pub(crate) enum PersistenceTaskError {
    Closed,
    Join(tokio::task::JoinError),
}

impl Display for PersistenceTaskError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Closed => formatter.write_str("server persistence executor closed"),
            Self::Join(error) => write!(formatter, "server persistence task failed: {error}"),
        }
    }
}

impl Error for PersistenceTaskError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Closed => None,
            Self::Join(error) => Some(error),
        }
    }
}

impl From<PersistenceTaskError> for RepositoryError {
    fn from(error: PersistenceTaskError) -> Self {
        Self::Storage(Box::new(error))
    }
}
