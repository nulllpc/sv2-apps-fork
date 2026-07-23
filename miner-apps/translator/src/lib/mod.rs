//! ## Translator Sv2
//!
//! Provides the core logic and main struct (`TranslatorSv2`) for running a
//! Stratum V1 to Stratum V2 translation proxy.
//!
//! This module orchestrates the interaction between downstream SV1 miners and upstream SV2
//! applications (proxies or pool servers).
//!
//! The central component is the `TranslatorSv2` struct, which encapsulates the state and
//! provides the `start` method as the main entry point for running the translator service.
//! It relies on several sub-modules (`config`, `downstream_sv1`, `upstream_sv2`, `proxy`, `status`,
//! etc.) for specialized functionalities.
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

pub use stratum_apps::stratum_core::sv1_api::server_to_client;

use config::TranslatorConfig;

use crate::utils::TproxyMode;

use translator_runtime::{Init, RuntimeEvent, TranslatorRuntime};

pub mod config;
pub mod error;
mod io_task;
#[cfg(feature = "monitoring")]
mod monitoring;
pub mod sv1;
pub mod sv2;
mod translator_runtime;
pub mod utils;

/// The main struct that manages the SV1/SV2 translator.
#[derive(Clone)]
pub struct TranslatorSv2 {
    config: TranslatorConfig,
    cancellation_token: CancellationToken,
    shutdown_notify: Arc<Notify>,
    is_alive: Arc<AtomicBool>,
}

#[cfg_attr(not(test), hotpath::measure_all)]
impl TranslatorSv2 {
    /// Creates a new `TranslatorSv2`.
    ///
    /// Initializes the translator with the given configuration and sets up
    /// the reconnect wait time.
    pub fn new(config: TranslatorConfig) -> Self {
        Self {
            config,
            cancellation_token: CancellationToken::new(),
            shutdown_notify: Arc::new(Notify::new()),
            is_alive: Arc::new(AtomicBool::new(true)),
        }
    }

    /// Starts the translator.
    ///
    /// This method starts the main event loop, which handles connections,
    /// protocol translation, job management, and status reporting.
    pub async fn start(self) {
        info!("Starting Translator Proxy...");

        let runtime = match TranslatorRuntime::<Init>::new(self.clone()) {
            Ok(runtime) => runtime,
            Err(_) => return,
        };

        let mut running = match runtime.bootstrap().await {
            Ok(running) => running,
            Err(bootstrap_err) => {
                error!(?bootstrap_err.kind, "Failed to bootstrap Translator Proxy");
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
                    let cm_ready_runtime = running.cleanup_for_fallback().await;

                    running = match cm_ready_runtime.try_upstream().await {
                        Ok(new_running) => new_running.start_services().await,
                        Err(bootstrap_err) => {
                            error!(?bootstrap_err.kind, "Failed to reconnect Translator Proxy");
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

impl Drop for TranslatorSv2 {
    fn drop(&mut self) {
        info!("TranslatorSv2 dropped");
        self.cancellation_token.cancel();
    }
}
