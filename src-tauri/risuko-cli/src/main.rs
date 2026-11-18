#![allow(clippy::large_enum_variant)]

mod commands;

use clap::{Parser, Subcommand};
use risuko_cli::args::{DownloadArgs, PauseArgs, RemoveArgs, ResumeArgs, ServeArgs, StatusArgs};
use risuko_cli::shared;

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
        Command::Download(args) => shared::download(&commands::HOST, args).await,
        Command::Status(args) => shared::status(&commands::HOST, args).await,
        Command::Pause(args) => shared::pause(&commands::HOST, args).await,
        Command::Resume(args) => shared::resume(&commands::HOST, args).await,
        Command::Remove(args) => shared::remove(&commands::HOST, args).await,
        Command::PauseAll(args) => commands::pause_all(args).await,
        Command::ResumeAll(args) => commands::resume_all(args).await,
        Command::GlobalStat(args) => commands::global_stat(args).await,
        Command::Files(args) => commands::files(args).await,
        Command::Peers(args) => commands::peers(args).await,
        Command::Purge(args) => commands::purge(args).await,
        Command::Config(cmd) => commands::config(cmd).await,
        Command::Rss(cmd) => commands::rss(cmd).await,
        Command::Serve(args) => shared::serve(&commands::HOST, args).await,
        Command::Shutdown(args) => commands::shutdown(args).await,
        Command::CreateTorrent(args) => commands::create_torrent(args).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    fn argv<'a>(args: &'a [&'a str]) -> impl Iterator<Item = &'a str> {
        std::iter::once("risuko").chain(args.iter().copied())
    }

    fn parse(args: &[&str]) -> Command {
        Cli::try_parse_from(argv(args))
            .unwrap_or_else(|e| panic!("{args:?}: {e}"))
            .command
    }

    fn matches(args: &[&str]) -> clap::ArgMatches {
        Cli::command()
            .try_get_matches_from(argv(args))
            .unwrap_or_else(|e| panic!("{args:?}: {e}"))
    }

    #[test]
    fn download_takes_several_urls_with_defaults() {
        let Command::Download(args) = parse(&["download", "http://a/f", "http://b/f"]) else {
            panic!("not download");
        };
        assert_eq!(args.urls, ["http://a/f", "http://b/f"]);
        assert_eq!(args.threads, 16);
        assert_eq!(args.dir, None);
        assert_eq!(args.out, None);
        assert!(args.headers.is_empty());
        assert_eq!(args.user_agent, None);
        assert_eq!(args.proxy, None);
        assert_eq!(args.referer, None);
        assert_eq!(args.cookie, None);
        assert_eq!(args.media_format, None);
        assert!(!args.force_ytdlp);
        assert_eq!(args.seed_ratio, None);
        assert_eq!(args.seed_time, None);
        assert_eq!(args.rpc_port, 16800);
        assert_eq!(args.rpc_secret, None);
        assert!(!args.json);
    }

    #[test]
    fn download_takes_every_flag() {
        let Command::Download(args) = parse(&[
            "download",
            "-t",
            "4",
            "-d",
            "/tmp/d",
            "-o",
            "f.bin",
            "-H",
            "A: 1",
            "-H",
            "B: 2",
            "--user-agent",
            "UA",
            "--proxy",
            "http://p:8080",
            "--referer",
            "http://r/",
            "--cookie",
            "c=1",
            "--media-format",
            "best",
            "--ytdlp",
            "--seed-ratio",
            "1.5",
            "--seed-time",
            "30",
            "--rpc-port",
            "7000",
            "--rpc-secret",
            "s",
            "--json",
            "magnet:?xt=urn:btih:abc",
        ]) else {
            panic!("not download");
        };
        assert_eq!(args.urls, ["magnet:?xt=urn:btih:abc"]);
        assert_eq!(args.threads, 4);
        assert_eq!(args.dir.as_deref(), Some("/tmp/d"));
        assert_eq!(args.out.as_deref(), Some("f.bin"));
        assert_eq!(args.headers, ["A: 1", "B: 2"]);
        assert_eq!(args.user_agent.as_deref(), Some("UA"));
        assert_eq!(args.proxy.as_deref(), Some("http://p:8080"));
        assert_eq!(args.referer.as_deref(), Some("http://r/"));
        assert_eq!(args.cookie.as_deref(), Some("c=1"));
        assert_eq!(args.media_format.as_deref(), Some("best"));
        assert!(args.force_ytdlp);
        assert_eq!(args.seed_ratio, Some(1.5));
        assert_eq!(args.seed_time, Some(30));
        assert_eq!(args.rpc_port, 7000);
        assert_eq!(args.rpc_secret.as_deref(), Some("s"));
        assert!(args.json);
    }

    #[test]
    fn download_takes_long_flags_and_the_format_alias() {
        let Command::Download(args) = parse(&[
            "download",
            "--threads",
            "2",
            "--dir",
            "d",
            "--out",
            "o",
            "--header",
            "X: y",
            "--youtube-format",
            "worst",
            "u",
        ]) else {
            panic!("not download");
        };
        assert_eq!(args.threads, 2);
        assert_eq!(args.dir.as_deref(), Some("d"));
        assert_eq!(args.out.as_deref(), Some("o"));
        assert_eq!(args.headers, ["X: y"]);
        assert_eq!(args.media_format.as_deref(), Some("worst"));
    }

    #[test]
    fn download_needs_a_url() {
        assert!(Cli::try_parse_from(argv(&["download"])).is_err());
    }

    #[test]
    fn download_takes_the_doh_flags() {
        let m = matches(&[
            "download",
            "--doh-url",
            "https://dns.example/dns-query",
            "--doh-bootstrap",
            "1.1.1.1,1.0.0.1",
            "u",
        ]);
        let download = m.subcommand_matches("download").unwrap();
        assert_eq!(
            download.get_one::<String>("doh_url").map(String::as_str),
            Some("https://dns.example/dns-query")
        );
        assert_eq!(
            download
                .get_one::<String>("doh_bootstrap")
                .map(String::as_str),
            Some("1.1.1.1,1.0.0.1")
        );
    }

    #[test]
    fn status_takes_an_optional_gid() {
        let Command::Status(args) = parse(&["status"]) else {
            panic!("not status");
        };
        assert_eq!(args.gid, None);
        assert_eq!(args.rpc_port, 16800);
        assert_eq!(args.rpc_secret, None);
        assert!(!args.json);

        let Command::Status(args) = parse(&[
            "status",
            "--gid",
            "abc",
            "--rpc-port",
            "7000",
            "--rpc-secret",
            "s",
            "--json",
        ]) else {
            panic!("not status");
        };
        assert_eq!(args.gid.as_deref(), Some("abc"));
        assert_eq!(args.rpc_port, 7000);
        assert_eq!(args.rpc_secret.as_deref(), Some("s"));
        assert!(args.json);
    }

    #[test]
    fn pause_and_resume_take_one_gid() {
        let Command::Pause(args) = parse(&["pause", "abc"]) else {
            panic!("not pause");
        };
        assert_eq!(args.gid, "abc");
        assert_eq!(args.rpc_port, 16800);
        assert_eq!(args.rpc_secret, None);

        let Command::Resume(args) =
            parse(&["resume", "abc", "--rpc-port", "7000", "--rpc-secret", "s"])
        else {
            panic!("not resume");
        };
        assert_eq!(args.gid, "abc");
        assert_eq!(args.rpc_port, 7000);
        assert_eq!(args.rpc_secret.as_deref(), Some("s"));

        assert!(Cli::try_parse_from(argv(&["pause"])).is_err());
        assert!(Cli::try_parse_from(argv(&["resume", "a", "b"])).is_err());
    }

    #[test]
    fn remove_takes_one_gid() {
        let Command::Remove(args) =
            parse(&["remove", "abc", "--rpc-port", "7000", "--rpc-secret", "s"])
        else {
            panic!("not remove");
        };
        assert_eq!(args.gids, ["abc"]);
        assert_eq!(args.rpc_port, 7000);
        assert_eq!(args.rpc_secret.as_deref(), Some("s"));
        assert!(Cli::try_parse_from(argv(&["remove"])).is_err());
    }

    #[test]
    fn remove_takes_several_gids() {
        let Command::Remove(args) = parse(&["remove", "a", "b", "c"]) else {
            panic!("not remove");
        };
        assert_eq!(args.gids, ["a", "b", "c"]);
        assert_eq!(args.rpc_port, 16800);
    }

    #[test]
    fn serve_takes_a_port_and_verbose() {
        let Command::Serve(args) = parse(&["serve"]) else {
            panic!("not serve");
        };
        assert_eq!(args.rpc_port, 16800);
        assert!(!args.verbose);

        let Command::Serve(args) = parse(&["serve", "--rpc-port", "7000", "--verbose"]) else {
            panic!("not serve");
        };
        assert_eq!(args.rpc_port, 7000);
        assert!(args.verbose);

        let Command::Serve(args) = parse(&["serve", "-v"]) else {
            panic!("not serve");
        };
        assert!(args.verbose);
    }
}
