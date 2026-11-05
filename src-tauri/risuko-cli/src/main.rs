#![allow(clippy::large_enum_variant)]

mod commands;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "risuko", about = "Risuko download engine CLI", version)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
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

    /// Pause all active downloads
    PauseAll(RpcArgs),

    /// Resume all paused downloads
    ResumeAll(RpcArgs),

    /// Show global download/upload stats
    GlobalStat(RpcArgs),

    /// Show files belonging to a download
    Files(GidArgs),

    /// Show peers for a torrent download
    Peers(GidArgs),

    /// Purge completed/error/removed download results
    Purge(RpcArgs),

    /// Manage configuration
    Config(ConfigCommand),

    /// Manage RSS feeds
    Rss(RssCommand),

    /// Start headless engine (RPC server only, no GUI)
    Serve(ServeArgs),

    /// Request engine shutdown via RPC
    Shutdown(RpcArgs),

    /// Create a .torrent file from a file or folder
    CreateTorrent(CreateTorrentArgs),
}

#[derive(clap::Args)]
pub struct CreateTorrentArgs {
    /// File or folder to share
    pub path: std::path::PathBuf,

    /// Where to write the .torrent file (default: <name>.torrent in the current directory)
    #[arg(short, long)]
    pub output: Option<std::path::PathBuf>,

    /// Torrent format
    #[arg(long, value_enum, default_value_t = TorrentFormat::Hybrid)]
    pub format: TorrentFormat,

    /// Piece size such as 256K or 4M (chosen from the total size when omitted)
    #[arg(long)]
    pub piece_size: Option<String>,

    /// Tracker URL; each flag starts a new tier, comma-separate trackers that share a tier
    #[arg(short = 'a', long = "tracker")]
    pub trackers: Vec<String>,

    /// Web seed URL (repeatable)
    #[arg(short = 'w', long = "web-seed")]
    pub web_seeds: Vec<String>,

    /// Comment stored in the torrent
    #[arg(short, long)]
    pub comment: Option<String>,

    /// Source tag stored in the info dictionary, as some private trackers require
    #[arg(short, long)]
    pub source: Option<String>,

    /// Mark the torrent private (no DHT, PEX or local peer discovery)
    #[arg(short, long)]
    pub private: bool,

    /// Leave out the creation date
    #[arg(long)]
    pub no_date: bool,

    /// Overwrite the output file if it already exists
    #[arg(short, long)]
    pub force: bool,

    /// Seed it from where the files are now, using the running engine or a headless one on this machine
    #[arg(long)]
    pub seed: bool,

    #[arg(long, default_value_t = 16800)]
    pub rpc_port: u16,

    /// RPC secret for authentication
    #[arg(long)]
    pub rpc_secret: Option<String>,

    /// Output as JSON
    #[arg(long)]
    pub json: bool,
}

#[derive(Clone, Copy, clap::ValueEnum)]
pub enum TorrentFormat {
    V1,
    V2,
    Hybrid,
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

    /// HTTP referer
    #[arg(long)]
    pub referer: Option<String>,

    /// Cookie string
    #[arg(long)]
    pub cookie: Option<String>,

    /// Media format selector passed to yt-dlp
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

    #[arg(long, default_value_t = 16800)]
    pub rpc_port: u16,

    /// RPC secret for authentication
    #[arg(long)]
    pub rpc_secret: Option<String>,
}

#[derive(clap::Args)]
pub struct RemoveArgs {
    /// Task GID(s) to remove
    #[arg(required = true, num_args = 1..)]
    pub gids: Vec<String>,

    #[arg(long, default_value_t = 16800)]
    pub rpc_port: u16,

    /// RPC secret for authentication
    #[arg(long)]
    pub rpc_secret: Option<String>,
}

#[derive(clap::Args)]
pub struct RpcArgs {
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
pub struct GidArgs {
    /// Task GID
    pub gid: String,

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
pub struct ServeArgs {
    #[arg(long, default_value_t = 16800)]
    pub rpc_port: u16,

    /// Enable verbose logging (maximize log output)
    #[arg(short, long)]
    pub verbose: bool,
}

#[derive(clap::Args)]
pub struct ConfigCommand {
    #[command(subcommand)]
    pub action: ConfigAction,
}

#[derive(Subcommand)]
pub enum ConfigAction {
    /// Get a config value
    Get {
        /// Config key
        key: String,
    },
    /// Set a config value
    Set {
        /// Config key
        key: String,
        /// Config value (JSON)
        value: String,
    },
    /// List all config values
    List {
        /// Output as JSON
        #[arg(long)]
        json: bool,
    },
}

#[derive(clap::Args)]
pub struct RssCommand {
    #[command(subcommand)]
    pub action: RssAction,
}

#[derive(Subcommand)]
pub enum RssAction {
    /// Add an RSS feed
    Add {
        /// Feed URL
        url: String,
        /// RPC port
        #[arg(long, default_value_t = 16800)]
        rpc_port: u16,
        /// RPC secret for authentication
        #[arg(long)]
        rpc_secret: Option<String>,
    },
    /// List all RSS feeds
    List {
        /// RPC port
        #[arg(long, default_value_t = 16800)]
        rpc_port: u16,
        /// RPC secret for authentication
        #[arg(long)]
        rpc_secret: Option<String>,
        /// Output as JSON
        #[arg(long)]
        json: bool,
    },
    /// Refresh all RSS feeds
    Refresh {
        /// RPC port
        #[arg(long, default_value_t = 16800)]
        rpc_port: u16,
        /// RPC secret for authentication
        #[arg(long)]
        rpc_secret: Option<String>,
    },
    /// Remove an RSS feed
    Remove {
        /// Feed ID
        id: String,
        /// RPC port
        #[arg(long, default_value_t = 16800)]
        rpc_port: u16,
        /// RPC secret for authentication
        #[arg(long)]
        rpc_secret: Option<String>,
    },
}

fn main() {
    let cli = Cli::parse();

    let default_level = match &cli.command {
        Command::Serve(args) if args.verbose => "debug,risuko_engine=trace",
        Command::Serve(_) => "info",
        _ => "warn",
    };

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default_level)),
        )
        .init();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to build tokio runtime");

    let code = rt.block_on(async {
        match run(cli.command).await {
            Ok(()) => 0,
            Err(e) => {
                eprintln!("Error: {}", e);
                1
            }
        }
    });

    std::process::exit(code);
}

async fn run(command: Command) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        Command::Download(args) => commands::download(args).await,
        Command::Status(args) => commands::status(args).await,
        Command::Pause(args) => commands::pause(args).await,
        Command::Resume(args) => commands::resume(args).await,
        Command::Remove(args) => commands::remove(args).await,
        Command::PauseAll(args) => commands::pause_all(args).await,
        Command::ResumeAll(args) => commands::resume_all(args).await,
        Command::GlobalStat(args) => commands::global_stat(args).await,
        Command::Files(args) => commands::files(args).await,
        Command::Peers(args) => commands::peers(args).await,
        Command::Purge(args) => commands::purge(args).await,
        Command::Config(cmd) => commands::config(cmd).await,
        Command::Rss(cmd) => commands::rss(cmd).await,
        Command::Serve(args) => commands::serve(args).await,
        Command::Shutdown(args) => commands::shutdown(args).await,
        Command::CreateTorrent(args) => commands::create_torrent(args).await,
    }
}
