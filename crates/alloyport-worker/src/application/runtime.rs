//! Long-running outbound worker lifecycle.

use crate::OutboundWorker;
use std::error::Error;
use std::time::Duration;

const INITIAL_RECONNECT_BACKOFF: Duration = Duration::from_secs(1);
const MAX_RECONNECT_BACKOFF: Duration = Duration::from_secs(30);

pub(super) async fn run(worker: OutboundWorker) -> Result<(), Box<dyn Error>> {
    let mut backoff = INITIAL_RECONNECT_BACKOFF;
    loop {
        tokio::select! {
            result = worker.run_session() => {
                if let Err(error) = result {
                    eprintln!("worker session ended: {error}; reconnecting in {backoff:?}");
                }
            }
            signal = tokio::signal::ctrl_c() => {
                signal?;
                return Ok(());
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_RECONNECT_BACKOFF);
    }
}
