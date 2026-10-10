#![cfg_attr(
    all(not(debug_assertions), target_os = "windows"),
    windows_subsystem = "windows"
)]

use clap::Parser;
use risuko_lib::cli;

fn main() {
    let args: Vec<String> = std::env::args_os()
        .map(|arg| arg.to_string_lossy().into_owned())
        .collect();
    let command = if cli::is_cli_invocation(&args) {
        cli::Cli::parse().command
    } else {
        None
    };

    if let Some(command) = command {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("Failed to build tokio runtime");

        let code = rt.block_on(async {
            match cli::run(command).await {
                Ok(()) => 0,
                Err(e) => {
                    eprintln!("Error: {}", e);
                    1
                }
            }
        });

        std::process::exit(code);
    }

    risuko_lib::run();
}
