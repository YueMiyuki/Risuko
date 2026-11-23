use std::sync::atomic::{AtomicBool, Ordering};

static FORCED_EXIT_ARMED: AtomicBool = AtomicBool::new(false);

pub async fn shutdown_signal() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut interrupt = signal(SignalKind::interrupt())?;
        let mut terminate = signal(SignalKind::terminate())?;
        tokio::select! {
            _ = interrupt.recv() => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await?;
    exit_on_next_signal();
    Ok(())
}

pub fn exit_on_next_signal() {
    if FORCED_EXIT_ARMED.swap(true, Ordering::SeqCst) {
        return;
    }
    if let Err(e) = arm_forced_exit() {
        FORCED_EXIT_ARMED.store(false, Ordering::SeqCst);
        eprintln!("Warning: a second Ctrl+C will not force an exit: {e}");
    }
}

#[cfg(unix)]
fn arm_forced_exit() -> std::io::Result<()> {
    use tokio::signal::unix::{signal, SignalKind};
    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut terminate = signal(SignalKind::terminate())?;
    tokio::spawn(async move {
        let code = tokio::select! {
            _ = interrupt.recv() => 130,
            _ = terminate.recv() => 143,
        };
        std::process::exit(code)
    });
    Ok(())
}

#[cfg(not(unix))]
fn arm_forced_exit() -> std::io::Result<()> {
    let mut interrupt = tokio::signal::windows::ctrl_c()?;
    tokio::spawn(async move {
        if interrupt.recv().await.is_some() {
            std::process::exit(130)
        }
    });
    Ok(())
}
