export const TEMP_DOWNLOAD_SUFFIX = ".part";

export const APP_THEME = {
	AUTO: "auto",
	LIGHT: "light",
	DARK: "dark",
};

export const APP_RUN_MODE = {
	STANDARD: 1,
	TRAY: 2,
	HIDE_TRAY: 3,
};

export const ADD_TASK_TYPE = {
	URI: "uri",
	TORRENT: "torrent",
};

export const TASK_STATUS = {
	ACTIVE: "active",
	WAITING: "waiting",
	PAUSED: "paused",
	SCHEDULED: "scheduled",
	ERROR: "error",
	COMPLETE: "complete",
	REMOVED: "removed",
	SEEDING: "seeding",
};

export const LOG_LEVELS = [
	"error",
	"warn",
	"info",
	"verbose",
	"debug",
	"silly",
];

export const MAX_NUM_OF_DIRECTORIES = 5;

export const MAX_NUM_OF_SAVED_CREDENTIALS = 50;

export const ENGINE_RPC_HOST = "127.0.0.1";
export const ENGINE_RPC_PORT = 16800;
export const ENGINE_PBH_RPC_PORT = 16801;
export const ENGINE_MAX_CONCURRENT_DOWNLOADS = 10;

export const ONE_SECOND = 1000;
const ONE_MINUTE = ONE_SECOND * 60;
const ONE_HOUR = ONE_MINUTE * 60;

export const AUTO_SYNC_TRACKER_INTERVAL = ONE_HOUR * 12;

export const MAX_BT_TRACKER_LENGTH = 6144;

export const DEFAULT_ED2K_SERVERS =
	"176.123.5.89:4725,45.82.80.155:5687,85.239.33.123:4232,91.208.162.87:4232,145.239.2.134:4661";

export const TRACKER_SOURCE_OPTIONS = [
	{
		label: "ngosang/trackerslist",
		options: [
			{
				value:
					"https://raw.githubusercontent.com/ngosang/trackerslist/master/trackers_best.txt",
				label: "trackers_best.txt",
				cdn: false,
			},
			{
				value:
					"https://raw.githubusercontent.com/ngosang/trackerslist/master/trackers_best_ip.txt",
				label: "trackers_best_ip.txt",
				cdn: false,
			},
			{
				value:
					"https://raw.githubusercontent.com/ngosang/trackerslist/master/trackers_all.txt",
				label: "trackers_all.txt",
				cdn: false,
			},
			{
				value:
					"https://raw.githubusercontent.com/ngosang/trackerslist/master/trackers_all_ip.txt",
				label: "trackers_all_ip.txt",
				cdn: false,
			},
			{
				value:
					"https://cdn.jsdelivr.net/gh/ngosang/trackerslist/trackers_best.txt",
				label: "trackers_best.txt",
				cdn: true,
			},
			{
				value:
					"https://cdn.jsdelivr.net/gh/ngosang/trackerslist/trackers_best_ip.txt",
				label: "trackers_best_ip.txt",
				cdn: true,
			},
			{
				value:
					"https://cdn.jsdelivr.net/gh/ngosang/trackerslist/trackers_all.txt",
				label: "trackers_all.txt",
				cdn: true,
			},
			{
				value:
					"https://cdn.jsdelivr.net/gh/ngosang/trackerslist/trackers_all_ip.txt",
				label: "trackers_all_ip.txt",
				cdn: true,
			},
		],
	},
	{
		label: "XIU2/TrackersListCollection",
		options: [
			{
				value:
					"https://raw.githubusercontent.com/XIU2/TrackersListCollection/master/best.txt",
				label: "best.txt",
				cdn: false,
			},
			{
				value:
					"https://raw.githubusercontent.com/XIU2/TrackersListCollection/master/all.txt",
				label: "all.txt",
				cdn: false,
			},
			{
				value:
					"https://raw.githubusercontent.com/XIU2/TrackersListCollection/master/http.txt",
				label: "http.txt",
				cdn: false,
			},
			{
				value:
					"https://cdn.jsdelivr.net/gh/XIU2/TrackersListCollection/best.txt",
				label: "best.txt",
				cdn: true,
			},
			{
				value:
					"https://cdn.jsdelivr.net/gh/XIU2/TrackersListCollection/all.txt",
				label: "all.txt",
				cdn: true,
			},
			{
				value:
					"https://cdn.jsdelivr.net/gh/XIU2/TrackersListCollection/http.txt",
				label: "http.txt",
				cdn: true,
			},
		],
	},
];

const PROXY_SCOPES = {
	DOWNLOAD: "download",
	UPDATE_APP: "update-app",
	UPDATE_TRACKERS: "update-trackers",
};

export const PROXY_SCOPE_OPTIONS = [
	PROXY_SCOPES.DOWNLOAD,
	PROXY_SCOPES.UPDATE_APP,
	PROXY_SCOPES.UPDATE_TRACKERS,
];

export const DOH_PROVIDERS = {
	cloudflare: {
		url: "https://cloudflare-dns.com/dns-query",
		bootstrap: "1.1.1.1,1.0.0.1,2606:4700:4700::1111,2606:4700:4700::1001",
	},
	google: {
		url: "https://dns.google/dns-query",
		bootstrap: "8.8.8.8,8.8.4.4,2001:4860:4860::8888,2001:4860:4860::8844",
	},
	quad9: {
		url: "https://dns.quad9.net/dns-query",
		bootstrap: "9.9.9.9,149.112.112.112,2620:fe::fe,2620:fe::9",
	},
	custom: {
		url: "",
		bootstrap: "",
	},
} as const;

export type DohProvider = keyof typeof DOH_PROVIDERS;

export const DOH_PROVIDER_OPTIONS: DohProvider[] = [
	"cloudflare",
	"google",
	"quad9",
	"custom",
];

export const NONE_SELECTED_FILES = "none";
export const SELECTED_ALL_FILES = "all";

export const TRAY_CANVAS_CONFIG = {
	WIDTH: 56,
	HEIGHT: 16,
	ICON_WIDTH: 16,
	ICON_HEIGHT: 16,
	TEXT_WIDTH: 42,
	TEXT_FONT_SIZE: 8,
};

export const SUPPORT_RTL_LOCALES = [
	"ar",
	"fa",
	"he",
	"ku",
	"pa",
	"ps",
	"sd",
	"ur",
	"yi",
];

export const IMAGE_SUFFIXES = [
	".ai",
	".bmp",
	".eps",
	".fig",
	".gif",
	".heic",
	".icn",
	".ico",
	".jpeg",
	".jpg",
	".png",
	".psd",
	".raw",
	".sketch",
	".svg",
	".tif",
	".webp",
	".xd",
];

export const AUDIO_SUFFIXES = [
	".aac",
	".ape",
	".flac",
	".flav",
	".m4a",
	".mp3",
	".ogg",
	".wav",
	".wma",
];

export const VIDEO_SUFFIXES = [
	".avi",
	".m3u8",
	".m4v",
	".mkv",
	".mov",
	".mp4",
	".mpg",
	".rmvb",
	".ts",
	".vob",
	".wmv",
];

export const SUB_SUFFIXES = [
	".ass",
	".idx",
	".smi",
	".srt",
	".ssa",
	".sst",
	".sub",
];

export const DOCUMENT_SUFFIXES = [
	".azw3",
	".csv",
	".doc",
	".docx",
	".epub",
	".key",
	".mobi",
	".numbers",
	".pages",
	".pdf",
	".ppt",
	".pptx",
	".txt",
	".xsl",
	".xslx",
];

export const FILE_CATEGORIES = {
	MUSIC: "music",
	VIDEO: "video",
	IMAGE: "image",
	DOCUMENT: "document",
	COMPRESSED: "compressed",
	PROGRAM: "program",
	RSS: "rss",
} as const;

export const DEFAULT_CLIPBOARD_WATCH_EXTENSIONS = [
	"zip",
	"7z",
	"rar",
	"tar",
	"gz",
	"tgz",
	"bz2",
	"xz",
	"zst",
	"iso",
	"img",
	"dmg",
	"pkg",
	"exe",
	"msi",
	"apk",
	"apks",
	"xapk",
	"deb",
	"rpm",
	"appimage",
	"bin",
	"mp4",
	"mkv",
	"avi",
	"mov",
	"webm",
	"flv",
	"wmv",
	"m4v",
	"mp3",
	"flac",
	"wav",
	"m4a",
	"aac",
	"ogg",
	"opus",
	"pdf",
	"epub",
	"mobi",
	"azw3",
	"docx",
	"xlsx",
	"pptx",
	"odt",
	"jar",
	"torrent",
	"meta4",
	"metalink",
];
