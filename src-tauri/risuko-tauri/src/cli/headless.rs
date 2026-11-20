use std::path::PathBuf;

pub(super) fn init_headless_tracing() {
    init_tracing("info,diag=debug,risuko_bt=debug,risuko_engine=info");
}

pub(super) fn init_verbose_tracing() {
    init_tracing("debug,risuko_engine=trace");
}

fn init_tracing(default_filter: &str) {
    use tracing_subscriber::EnvFilter;
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_filter));
    let _ = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .try_init();
}

pub(super) fn get_config_dir() -> PathBuf {
    risuko_engine::standalone::gui_config_dir()
}
