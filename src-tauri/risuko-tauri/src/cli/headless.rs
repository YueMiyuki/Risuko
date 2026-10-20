use std::path::PathBuf;

use risuko_engine::standalone::{StandaloneConfig, StandaloneEngine};

fn init_headless_tracing() {
    use tracing_subscriber::EnvFilter;
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,diag=debug,risuko_bt=debug,risuko_engine=info"));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .try_init();
}

pub async fn start_headless_engine(
    rpc_port: u16,
) -> Result<HeadlessEngine, Box<dyn std::error::Error>> {
    init_headless_tracing();
    tracing::info!("Starting headless engine on port {}", rpc_port);
    let engine = StandaloneEngine::start(StandaloneConfig {
        config_dir: get_config_dir(),
        rpc_port: Some(rpc_port),
        enable_rpc: true,
        require_download_dir: true,
    })
    .await?;
    Ok(HeadlessEngine { engine })
}

pub struct HeadlessEngine {
    engine: StandaloneEngine,
}

impl HeadlessEngine {
    pub fn rpc_secret(&self) -> Option<&str> {
        self.engine.rpc_secret.as_deref()
    }

    pub async fn shutdown_requested(&self) {
        self.engine.shutdown_signal().notified().await
    }

    pub async fn shutdown(self) {
        self.engine.stop().await;
        tracing::info!("Headless engine stopped");
    }
}

pub(super) fn get_config_dir() -> PathBuf {
    risuko_engine::standalone::gui_config_dir()
}
