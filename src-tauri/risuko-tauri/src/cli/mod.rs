pub mod headless;

pub use risuko_cli::{progress, rpc_client};

use clap::{Parser, Subcommand};
use risuko_cli::args::{DownloadArgs, PauseArgs, RemoveArgs, ResumeArgs, ServeArgs, StatusArgs};
use risuko_cli::shared::{self, EngineHost};

const HOST: EngineHost = EngineHost {
    config_dir: headless::get_config_dir,
    require_download_dir: true,
    before_start: headless::init_headless_tracing,
};

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
}

pub async fn run(command: Command) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        Command::Download(args) => shared::download(&HOST, args).await,
        Command::Status(args) => shared::status(&HOST, args).await,
        Command::Pause(args) => shared::pause(&HOST, args).await,
        Command::Resume(args) => shared::resume(&HOST, args).await,
        Command::Remove(args) => shared::remove(&HOST, args).await,
        Command::Serve(args) => {
            if args.verbose {
                headless::init_verbose_tracing();
            }
            shared::serve(&HOST, args).await
        }
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

#[cfg(test)]
mod parse_tests {
    use super::*;
    use clap::CommandFactory;

    fn argv<'a>(args: &'a [&'a str]) -> impl Iterator<Item = &'a str> {
        std::iter::once("risuko").chain(args.iter().copied())
    }

    fn parse(args: &[&str]) -> Command {
        Cli::try_parse_from(argv(args))
            .unwrap_or_else(|e| panic!("{args:?}: {e}"))
            .command
            .unwrap_or_else(|| panic!("{args:?}: no subcommand"))
    }

    fn matches(args: &[&str]) -> clap::ArgMatches {
        Cli::command()
            .try_get_matches_from(argv(args))
            .unwrap_or_else(|e| panic!("{args:?}: {e}"))
    }

    #[test]
    fn no_subcommand_and_the_login_flag() {
        let cli = Cli::try_parse_from(argv(&[])).unwrap();
        assert!(cli.command.is_none());
        assert_eq!(cli.opened_at_login, None);

        let cli = Cli::try_parse_from(argv(&["--opened-at-login=1"])).unwrap();
        assert!(cli.command.is_none());
        assert_eq!(cli.opened_at_login.as_deref(), Some("1"));
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
        assert_eq!(args.doh_url, None);
        assert_eq!(args.doh_bootstrap, None);
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
            "--doh-url",
            "https://dns.example/dns-query",
            "--doh-bootstrap",
            "1.1.1.1,1.0.0.1",
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
        assert_eq!(
            args.doh_url.as_deref(),
            Some("https://dns.example/dns-query")
        );
        assert_eq!(args.doh_bootstrap.as_deref(), Some("1.1.1.1,1.0.0.1"));
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
        let m = matches(&["remove", "a", "b", "c"]);
        let remove = m.subcommand_matches("remove").unwrap();
        let gids: Vec<&str> = remove
            .get_many::<String>("gids")
            .unwrap()
            .map(String::as_str)
            .collect();
        assert_eq!(gids, ["a", "b", "c"]);
    }

    #[test]
    fn serve_takes_a_port() {
        let Command::Serve(args) = parse(&["serve"]) else {
            panic!("not serve");
        };
        assert_eq!(args.rpc_port, 16800);

        let Command::Serve(args) = parse(&["serve", "--rpc-port", "7000"]) else {
            panic!("not serve");
        };
        assert_eq!(args.rpc_port, 7000);
    }

    #[test]
    fn serve_takes_verbose() {
        for flag in ["--verbose", "-v"] {
            let m = matches(&["serve", flag]);
            assert!(m.subcommand_matches("serve").unwrap().get_flag("verbose"));
        }
    }
}
