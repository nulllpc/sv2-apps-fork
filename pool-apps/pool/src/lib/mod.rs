use error::PoolErrorKind;
use pool_runtime::{Init, PoolRuntime};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use stratum_apps::bitcoin_core_sv2::common::template_distribution_protocol::CancellationToken;
use tokio::sync::Notify;
use tracing::info;

use crate::config::PoolConfig;

pub mod channel_manager;
pub mod config;
pub mod downstream;
pub mod error;
mod io_task;
#[cfg(feature = "monitoring")]
mod monitoring;
mod pool_runtime;
pub mod template_receiver;
pub mod utils;

#[derive(Debug, Clone)]
pub struct PoolSv2 {
    config: PoolConfig,
    cancellation_token: CancellationToken,
    shutdown_notify: Arc<Notify>,
    is_alive: Arc<AtomicBool>,
}

#[cfg_attr(not(test), hotpath::measure_all)]
impl PoolSv2 {
    pub fn new(config: PoolConfig) -> Self {
        Self {
            config,
            cancellation_token: CancellationToken::new(),
            shutdown_notify: Arc::new(Notify::new()),
            is_alive: Arc::new(AtomicBool::new(true)),
        }
    }

    pub async fn start(&self) -> Result<(), PoolErrorKind> {
        let runtime = PoolRuntime::<Init>::new(self.clone())?;

        let runtime = match runtime.bootstrap().await {
            Ok(runtime) => runtime,
            Err(err) => {
                return Err(err);
            }
        };

        runtime.wait_for_shutdown().await;
        runtime.shutdown().await;

        Ok(())
    }

    pub async fn shutdown(&self) {
        if !self.is_alive.load(Ordering::Relaxed) {
            return;
        }
        // The Notified future is guaranteed to receive wakeups from notify_waiters()
        // as soon as it has been created, even if it has not yet been polled.
        let notified = self.shutdown_notify.notified();
        self.cancellation_token.cancel();
        notified.await;
    }
}

impl Drop for PoolSv2 {
    fn drop(&mut self) {
        info!("PoolSv2 dropped");
        self.cancellation_token.cancel();
    }
}
