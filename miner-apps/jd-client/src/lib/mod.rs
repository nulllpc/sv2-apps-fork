use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use jdc_runtime::{Init, JdcRuntime, RuntimeEvent};
use stratum_apps::bitcoin_core_sv2::CancellationToken;
use tokio::sync::Notify;
use tracing::{error, info};

use crate::config::JobDeclaratorClientConfig;

mod channel_manager;
pub mod config;
mod downstream;
pub mod error;
mod io_task;
pub mod jd_mode;
mod jdc_runtime;
mod job_declarator;
#[cfg(feature = "monitoring")]
pub mod monitoring;
mod template_receiver;
mod upstream;
pub mod utils;

/// Represent Job Declarator Client
#[derive(Clone)]
pub struct JobDeclaratorClient {
    config: JobDeclaratorClientConfig,
    cancellation_token: CancellationToken,
    shutdown_notify: Arc<Notify>,
    is_alive: Arc<AtomicBool>,
}

#[cfg_attr(not(test), hotpath::measure_all)]
impl JobDeclaratorClient {
    /// Creates a new [`JobDeclaratorClient`] instance.
    pub fn new(config: JobDeclaratorClientConfig) -> Self {
        Self {
            config,
            cancellation_token: CancellationToken::new(),
            shutdown_notify: Arc::new(Notify::new()),
            is_alive: Arc::new(AtomicBool::new(true)),
        }
    }

    /// Starts the Job Declarator Client (JDC) main loop.
    pub async fn start(&self) {
        let runtime = match JdcRuntime::<Init>::new(self.clone()) {
            Ok(runtime) => runtime,
            Err(_) => return,
        };

        let mut running = match runtime.bootstrap().await {
            Ok(running) => running,
            Err(bootstrap_err) => {
                error!(?bootstrap_err.kind, "Failed to bootstrap JDC");
                bootstrap_err.runtime.shutdown().await;
                return;
            }
        };

        loop {
            match running.wait().await {
                RuntimeEvent::Shutdown => {
                    running.shutdown().await;
                    break;
                }
                RuntimeEvent::Fallback => {
                    let tp_ready_runtime = running.cleanup_for_fallback().await;

                    running = match tp_ready_runtime.start_template_provider().await {
                        Ok(new_running) => new_running,
                        Err(bootstrap_err) => {
                            error!(?bootstrap_err.kind, "Failed to reconnect JDC");
                            bootstrap_err.runtime.shutdown().await;
                            return;
                        }
                    };
                }
            }
        }
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

impl Drop for JobDeclaratorClient {
    fn drop(&mut self) {
        info!("JobDeclaratorClient dropped");
        self.cancellation_token.cancel();
    }
}
