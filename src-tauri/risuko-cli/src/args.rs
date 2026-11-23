#[derive(clap::Args)]
pub struct DownloadArgs {
    /// URL(s), magnet link, or path to a .torrent file
    #[arg(required = true, num_args = 1..)]
    pub urls: Vec<String>,

    /// Number of connections per download (maps to split); the configured value is used when not given
    #[arg(short = 't', long)]
    pub threads: Option<u32>,

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
    /// Task GID(s) to remove
    #[arg(required = true, num_args = 1..)]
    pub gids: Vec<String>,

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

    /// Enable verbose logging (maximize log output)
    #[arg(short, long)]
    pub verbose: bool,
}
