pub mod commands;
pub mod headless;

pub use risuko_cli::{progress, rpc_client};

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "risuko", about = "A full-featured download manager", version)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Command>,

    /// Internal flag set by autostart (hidden from help)
    #[arg(long = "opened-at-login", hide = true)]
    pub opened_at_login: Option<String>,
}

#[derive(Subcommand)]
#[allow(clippy::large_enum_variant)]
pub enum Command {
    /// Download a file from a URL, magnet link, or torrent file
    Download(DownloadArgs),

    /// Show status of downloads
    Status(StatusArgs),

    /// Pause a download
    Pause(PauseArgs),

    /// Resume a paused download
    Resume(ResumeArgs),

    /// Remove a download
    Remove(RemoveArgs),

    /// Start headless engine (RPC server only, no GUI)
    Serve(ServeArgs),

    /// Internal: extract browser cookies as JSON. Used by the Windows elevated helper to decrypt app-bound (Chrome v20) cookies. Hidden from `--help`
    #[command(hide = true)]
    ExtractCookies(ExtractCookiesArgs),
}

#[derive(clap::Args)]
pub struct DownloadArgs {
    /// URL(s), magnet link, or path to a .torrent file
    #[arg(required = true, num_args = 1..)]
    pub urls: Vec<String>,

    /// Number of connections per download (maps to split)
    #[arg(short = 't', long, default_value_t = 16)]
    pub threads: u32,

    /// Download directory
    #[arg(short, long)]
    pub dir: Option<String>,

    /// Output filename
    #[arg(short, long)]
    pub out: Option<String>,

    /// HTTP header (repeatable, e.g. -H "Cookie: foo=bar")
    #[arg(short = 'H', long = "header")]
    pub headers: Vec<String>,

    /// User agent string
    #[arg(long)]
    pub user_agent: Option<String>,

    /// Proxy server URL (e.g. http://proxy:8080)
    #[arg(long)]
    pub proxy: Option<String>,

    /// DNS-over-HTTPS endpoint URL (e.g. https://cloudflare-dns.com/dns-query)
    #[arg(long)]
    pub doh_url: Option<String>,

    /// Bootstrap IPs for the DoH endpoint, comma separated (optional)
    #[arg(long)]
    pub doh_bootstrap: Option<String>,

    /// HTTP referer
    #[arg(long)]
    pub referer: Option<String>,

    /// Cookie string
    #[arg(long)]
    pub cookie: Option<String>,

    /// Media format selector passed to yt-dlp (YouTube, Vimeo, etc.)
    #[arg(long, alias = "youtube-format")]
    pub media_format: Option<String>,

    /// Force the URL through the yt-dlp media engine even if its host is not in the built-in allowlist
    #[arg(long = "ytdlp")]
    pub force_ytdlp: bool,

    /// BT seed ratio (e.g. 1.0)
    #[arg(long)]
    pub seed_ratio: Option<f64>,

    /// BT seed time in minutes
    #[arg(long)]
    pub seed_time: Option<u64>,

    /// RPC port to connect to
    #[arg(long, default_value_t = 16800)]
    pub rpc_port: u16,

    /// RPC secret for authentication
    #[arg(long)]
    pub rpc_secret: Option<String>,

    /// Output as JSON
    #[arg(long)]
    pub json: bool,
}

#[derive(clap::Args)]
pub struct StatusArgs {
    /// Show a specific task by GID
    #[arg(long)]
    pub gid: Option<String>,

    /// RPC port to connect to
    #[arg(long, default_value_t = 16800)]
    pub rpc_port: u16,

    /// RPC secret for authentication
    #[arg(long)]
    pub rpc_secret: Option<String>,

    /// Output as JSON
    #[arg(long)]
    pub json: bool,
}

#[derive(clap::Args)]
pub struct PauseArgs {
    /// Task GID to pause
    pub gid: String,

    /// RPC port to connect to
    #[arg(long, default_value_t = 16800)]
    pub rpc_port: u16,

    /// RPC secret for authentication
    #[arg(long)]
    pub rpc_secret: Option<String>,
}

#[derive(clap::Args)]
pub struct ResumeArgs {
    /// Task GID to resume
    pub gid: String,

    /// RPC port to connect to
    #[arg(long, default_value_t = 16800)]
    pub rpc_port: u16,

    /// RPC secret for authentication
    #[arg(long)]
    pub rpc_secret: Option<String>,
}

#[derive(clap::Args)]
pub struct RemoveArgs {
    /// Task GID to remove
    pub gid: String,

    /// RPC port to connect to
    #[arg(long, default_value_t = 16800)]
    pub rpc_port: u16,

    /// RPC secret for authentication
    #[arg(long)]
    pub rpc_secret: Option<String>,
}

#[derive(clap::Args)]
pub struct ServeArgs {
    /// RPC port to listen on
    #[arg(long, default_value_t = 16800)]
    pub rpc_port: u16,
}

#[derive(clap::Args)]
pub struct ExtractCookiesArgs {
    /// Browser id (chrome, chromium, brave, edge, vivaldi, opera, arc, firefox, librewolf, zen, ...)
    #[arg(long)]
    pub browser: String,

    /// Target URL or bare host to scope the cookies to
    #[arg(long)]
    pub url: String,

    /// Write the resulting HostCookies JSON to this file instead of stdout. The GUI passes a temp path here when relaunching elevated
    #[arg(long)]
    pub out: Option<String>,
}

pub async fn run(command: Command) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        Command::Download(args) => commands::download(args).await,
        Command::Status(args) => commands::status(args).await,
        Command::Pause(args) => commands::pause(args).await,
        Command::Resume(args) => commands::resume(args).await,
        Command::Remove(args) => commands::remove(args).await,
        Command::Serve(args) => commands::serve(args).await,
        Command::ExtractCookies(args) => commands::extract_cookies(args).await,
    }
}

pub fn is_cli_invocation(args: &[String]) -> bool {
    matches!(
        args.get(1).map(String::as_str),
        Some(
            "download"
                | "status"
                | "pause"
                | "resume"
                | "remove"
                | "serve"
                | "extract-cookies"
                | "help"
                | "-h"
                | "--help"
                | "-V"
                | "--version"
        )
    )
}

#[cfg(test)]
mod launch_tests {
    use super::is_cli_invocation;

    fn argv(v: &[&str]) -> Vec<String> {
        v.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn subcommands_and_help_are_cli() {
        assert!(is_cli_invocation(&argv(&["risuko", "download", "x"])));
        assert!(is_cli_invocation(&argv(&["risuko", "--help"])));
        assert!(is_cli_invocation(&argv(&["risuko", "serve"])));
    }

    #[test]
    fn paths_links_and_flags_launch_gui() {
        assert!(!is_cli_invocation(&argv(&["risuko"])));
        assert!(!is_cli_invocation(&argv(&["risuko", "/tmp/a.torrent"])));
        assert!(!is_cli_invocation(&argv(&[
            "risuko",
            "magnet:?xt=urn:btih:abc"
        ])));
        assert!(!is_cli_invocation(&argv(&[
            "risuko",
            "--opened-at-login=1"
        ])));
    }
}
