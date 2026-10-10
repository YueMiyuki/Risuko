import { userKeys } from "./configKeys";

export interface SyncCategory {
	id: string;
	keys: string[];
}

const MISC_CATEGORY = "misc";

const DEVICE_LOCAL_KEYS = new Set<string>([
	"cloud-sync-enabled",
	"cloud-sync-auto",
	"cloud-sync-categories",
	"cloud-sync-token",
	"cloud-sync-last-at",
	"cloud-sync-server-url",
	"cloud-sync-category-timestamps",
	"legal-accepted",
	"clipboard-watch-notice-seen",
]);

const namedCategories: SyncCategory[] = [
	{
		id: "appearance",
		keys: [
			"theme",
			"font-family",
			"font-size",
			"hide-app-menu",
			"tray-speedometer",
			"show-progress-bar",
			"task-list-style",
			"sidebar-collapsed",
		],
	},
	{
		id: "language",
		keys: ["locale"],
	},
	{
		id: "network",
		keys: [
			"proxy",
			"all-proxy",
			"no-proxy",
			"p2p-proxy",
			"p2p-no-proxy",
			"p2p-udp-proxy",
			"p2p-udp-no-proxy",
			"cookie",
		],
	},
	{
		id: "tracker",
		keys: [
			"auto-sync-tracker",
			"tracker-source",
			"last-sync-tracker-time",
			"bt-tracker",
		],
	},
	{
		id: "directories",
		keys: ["favorite-directories", "history-directories", "file-category-dirs"],
	},
	{
		id: "download",
		keys: [
			"run-mode",
			"keep-seeding",
			"new-task-show-downloading",
			"auto-retry",
			"auto-retry-interval",
			"auto-retry-strategy",
			"no-confirm-before-delete-task",
			"resume-all-when-app-launched",
			"use-remote-file-time",
			"keep-window-state",
			"auto-hide-window",
			"dir",
			"auto-file-renaming",
			"connect-timeout",
			"file-allocation",
			"max-concurrent-downloads",
			"max-download-limit",
			"max-overall-download-limit",
			"max-overall-upload-limit",
			"engine-mode",
			"max-worker-retries",
			"netrc-path",
			"no-netrc",
			"out",
			"referer",
			"remote-time",
			"seed-ratio",
			"seed-time",
			"split",
			"uri-selector",
			"user-agent",
			"header",
			"load-cookies",
			"bt-create-subfolder",
		],
	},
	{
		id: "media",
		keys: ["media-format", "youtube-format", "m3u8-output-format"],
	},
	{
		id: "stats",
		keys: [],
	},
	{
		id: "task-routing",
		keys: ["task-routing-rules"],
	},
	{
		id: "notifications",
		keys: [
			"task-notification",
			"completion-script-enabled",
			"completion-script-command",
			"completion-script-args",
			"completion-script-timeout-ms",
		],
	},
	{
		id: "low-speed",
		keys: [
			"auto-detect-low-speed-tasks",
			"low-speed-threshold",
			"lowest-speed-limit",
			"lowest-speed-limit-timeout",
		],
	},
	{
		id: "system",
		keys: [
			"open-at-login",
			"prevent-sleep-while-downloading",
			"purge-record-on-start",
			"shutdown-when-complete",
			"auto-check-update",
			"last-check-update-time",
		],
	},
	{
		id: "engine",
		keys: [
			"external-engine-enabled",
			"external-engine-host",
			"external-engine-port",
			"external-engine-secret",
			"engine-overrides",
			"rpc-listen-port",
			"rpc-secret",
			"pbh-enable",
			"pbh-listen-port",
			"pbh-rpc-secret",
		],
	},
	{
		id: "dns",
		keys: [
			"doh-enable",
			"doh-url",
			"doh-bootstrap",
			"doh-fallback",
			"doh-provider",
		],
	},
	{
		id: "protocols",
		keys: ["protocols"],
	},
	{
		id: "logs",
		keys: ["log-dir-override", "log-level"],
	},
	{
		id: "credentials",
		keys: ["saved-credentials"],
	},
	{
		id: "bittorrent",
		keys: [
			"bt-force-encryption",
			"bt-load-saved-metadata",
			"bt-save-metadata",
			"bt-max-peers-per-torrent",
			"bt-max-outstanding-per-peer",
			"bt-max-connections",
			"bt-ban-corrupt-peers",
			"bt-ban-corrupt-strikes",
			"bt-enable-upnp",
			"bt-upnp-lease",
			"bt-enable-lsd",
			"bt-encryption-policy",
			"bt-listen-v6",
			"dht-listen-port",
		],
	},
	{
		id: "ports",
		keys: ["listen-port", "ed2k-port", "ed2k-enable-kad", "ed2k-kad-port"],
	},
	{
		id: "ftp",
		keys: [
			"ftp-passwd",
			"ftp-user",
			"sftp-passwd",
			"sftp-private-key",
			"sftp-private-key-passphrase",
			"sftp-user",
		],
	},
	{
		id: "usenet",
		keys: [
			"usenet-profiles",
			"usenet-archive-limits",
			"usenet-cleanup-mode",
			"usenet-limits-adjusted",
			"nzb-body-timeout",
		],
	},
	{
		id: "g2-gnutella",
		keys: ["gift-enabled", "gift-host", "gift-port", "ed2k-server"],
	},
];

const categorizedKeys = new Set(namedCategories.flatMap((c) => c.keys));
const miscKeys = userKeys.filter(
	(k) => !categorizedKeys.has(k) && !DEVICE_LOCAL_KEYS.has(k),
);

export const syncCategories: SyncCategory[] = miscKeys.length
	? [...namedCategories, { id: MISC_CATEGORY, keys: miscKeys }]
	: namedCategories;

export const syncCategoryIds = syncCategories.map((c) => c.id);

export function getCategoriesForKey(key: string): string[] {
	return syncCategories.filter((c) => c.keys.includes(key)).map((c) => c.id);
}
