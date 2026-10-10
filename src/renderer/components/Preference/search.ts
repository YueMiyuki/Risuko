type PreferenceSearchRoute =
	| "basic"
	| "appearance"
	| "advanced"
	| "usenet"
	| "cloud-sinks"
	| "sync";

export interface PreferenceSearchEntry {
	key: string;
	label: string;
	route: PreferenceSearchRoute;
	target: string;
	searchText: string;
}

interface PreferenceSearchTargetMarker<T> {
	target: T;
	keys: string[];
}

interface PreferenceSearchTargetLabel<T> {
	target: T;
	text: string;
}

interface PreferenceSearchDefinition {
	key: string;
	route: PreferenceSearchRoute;
	target?: string;
	availability?: "android" | "desktop" | "macos";
}

interface PreferenceSearchEnvironment {
	android?: boolean;
	macOS?: boolean;
	renderer?: boolean;
}

type TranslationBundle = Record<string, unknown>;
type Translate = (key: string) => unknown;

const APPEARANCE_PREFIXES = [
	"appearance",
	"font-",
	"theme-",
	"task-list-style",
	"sidebar-collapsed",
	"hide-app-menu",
	"auto-hide-window",
	"tray-speedometer",
];

const ADVANCED_PREFIXES = [
	"completion-script",
	"proxy",
	"enable-proxy",
	"enable-p2p-proxy",
	"doh",
	"bt-tracker",
	"sync-tracker",
	"auto-sync-tracker",
	"bt-max-",
	"bt-ban-",
	"bt-enable-",
	"bt-upnp",
	"bt-listen-",
	"bt-encryption-",
	"bt-create-subfolder",
	"http-reliability",
	"connect-timeout",
	"nzb-body-timeout",
	"lowest-speed-",
	"uri-selector",
	"storage",
	"file-allocation",
	"ed2k-server",
	"ed2k-kad",
	"ed2k-enable-kad",
	"gift-",
	"ftp-",
	"sftp-",
	"saved-credentials",
	"no-saved-credentials",
	"credential-",
	"vault-",
	"auto-update",
	"auto-check-update",
	"last-check-update-time",
	"m3u8-",
	"media-",
	"rpc",
	"pbh",
	"external-engine-",
	"port",
	"bt-port",
	"dht-port",
	"ed2k-port",
	"download-protocol",
	"protocols-",
	"user-agent",
	"mock-user-agent",
	"cookies",
	"load-cookies-",
	"saved-cookies",
	"netrc",
	"no-netrc",
	"developer",
	"app-log-path",
	"log-dir-override",
	"engine-overrides",
	"session-reset",
	"factory-reset",
	"randomize-port",
	"generate-rpc-secret",
	"generate-pbh-rpc-secret",
	"copy-rpc-url",
];

const TARGETS: Record<string, string> = {
	"preferences.run-mode-standard": "preferences.run-mode",
	"preferences.run-mode-tray": "preferences.run-mode",
	"preferences.history-directories": "preferences.default-dir",
	"preferences.favorite-directory": "preferences.default-dir",
	"preferences.unfavorite-directory": "preferences.default-dir",
	"preferences.remove-history-directory": "preferences.default-dir",
	"preferences.file-category-music": "preferences.file-category-dirs",
	"preferences.file-category-video": "preferences.file-category-dirs",
	"preferences.file-category-image": "preferences.file-category-dirs",
	"preferences.file-category-document": "preferences.file-category-dirs",
	"preferences.file-category-compressed": "preferences.file-category-dirs",
	"preferences.file-category-program": "preferences.file-category-dirs",
	"preferences.file-category-rss": "preferences.file-category-dirs",
	"preferences.task-routing-rule-add": "preferences.task-routing-rules",
	"preferences.seed-ratio": "preferences.keep-seeding",
	"preferences.seed-time": "preferences.keep-seeding",
	"preferences.auto-retry-strategy": "preferences.auto-retry",
	"preferences.auto-retry-strategy-exponential":
		"preferences.auto-retry-strategy",
	"preferences.auto-retry-strategy-static": "preferences.auto-retry-strategy",
	"preferences.auto-retry-interval": "preferences.auto-retry",
	"preferences.worker-max-retries": "preferences.auto-retry",
	"preferences.low-speed-threshold": "preferences.auto-detect-low-speed-tasks",
	"preferences.clipboard-watch-ext": "preferences.clipboard-watch",
	"preferences.theme-auto": "preferences.appearance-theme",
	"preferences.theme-light": "preferences.appearance-theme",
	"preferences.theme-dark": "preferences.appearance-theme",
	"preferences.font-family-system": "preferences.font-family",
	"preferences.font-family-rounded": "preferences.font-family",
	"preferences.font-family-serif": "preferences.font-family",
	"preferences.font-family-mono": "preferences.font-family",
	"preferences.font-size-small": "preferences.font-size",
	"preferences.font-size-default": "preferences.font-size",
	"preferences.font-size-large": "preferences.font-size",
	"preferences.font-size-extra-large": "preferences.font-size",
	"preferences.task-list-style-compact": "preferences.task-list-style",
	"preferences.task-list-style-card": "preferences.task-list-style",
	"preferences.completion-script-command":
		"preferences.completion-script-enabled",
	"preferences.completion-script-args": "preferences.completion-script-enabled",
	"preferences.completion-script-timeout":
		"preferences.completion-script-enabled",
	"preferences.proxy-scope-label": "preferences.enable-proxy",
	"preferences.proxy-scope-download": "preferences.enable-proxy",
	"preferences.proxy-scope-update-app": "preferences.enable-proxy",
	"preferences.proxy-scope-update-trackers": "preferences.enable-proxy",
	"preferences.proxy-http-profile": "preferences.enable-proxy",
	"preferences.proxy-p2p-profile": "preferences.enable-p2p-proxy",
	"preferences.proxy-p2p-tcp-server": "preferences.enable-p2p-proxy",
	"preferences.proxy-p2p-tcp-bypass": "preferences.enable-p2p-proxy",
	"preferences.proxy-p2p-udp-server": "preferences.enable-p2p-proxy",
	"preferences.proxy-p2p-udp-bypass": "preferences.enable-p2p-proxy",
	"preferences.proxy-p2p-udp-tips": "preferences.enable-p2p-proxy",
	"preferences.doh-provider": "preferences.doh-enable",
	"preferences.doh-provider-cloudflare": "preferences.doh-provider",
	"preferences.doh-provider-google": "preferences.doh-provider",
	"preferences.doh-provider-quad9": "preferences.doh-provider",
	"preferences.doh-provider-custom": "preferences.doh-provider",
	"preferences.doh-url": "preferences.doh-enable",
	"preferences.doh-bootstrap": "preferences.doh-enable",
	"preferences.doh-fallback": "preferences.doh-enable",
	"preferences.bt-encryption-plaintext": "preferences.bt-encryption-policy",
	"preferences.bt-encryption-prefer": "preferences.bt-encryption-policy",
	"preferences.bt-encryption-require": "preferences.bt-encryption-policy",
	"preferences.uri-selector-feedback": "preferences.uri-selector",
	"preferences.uri-selector-inorder": "preferences.uri-selector",
	"preferences.uri-selector-adaptive": "preferences.uri-selector",
	"preferences.file-allocation-falloc": "preferences.file-allocation",
	"preferences.file-allocation-trunc": "preferences.file-allocation",
	"preferences.file-allocation-none": "preferences.file-allocation",
	"preferences.external-engine-ip": "preferences.external-engine-enable",
	"preferences.external-engine-port": "preferences.external-engine-enable",
	"preferences.external-engine-secret": "preferences.external-engine-enable",
	"preferences.protocols-default-client": "preferences.download-protocol",
	"preferences.protocols-magnet": "preferences.download-protocol",
	"preferences.protocols-thunder": "preferences.download-protocol",
	"preferences.protocols-ed2k": "preferences.download-protocol",
	"preferences.protocols-adc": "preferences.download-protocol",
	"preferences.protocols-gnutella": "preferences.download-protocol",
	"preferences.protocols-g2": "preferences.download-protocol",
	"preferences.gift-integration": "preferences.gift-enabled",
	"preferences.gift-host": "preferences.gift-enabled",
	"preferences.gift-port": "preferences.gift-enabled",
	"preferences.mock-user-agent": "preferences.user-agent",
	"preferences.saved-cookies": "preferences.cookies",
	"preferences.no-netrc": "preferences.netrc",
	"preferences.engine-overrides": "preferences.developer",
	"preferences.session-reset": "preferences.developer",
	"preferences.factory-reset": "preferences.developer",
	"preferences.usenet-cleanup-action": "preferences.usenet-cleanup",
	"preferences.usenet-cleanup-keep": "preferences.usenet-cleanup-action",
	"preferences.usenet-cleanup-par2": "preferences.usenet-cleanup-action",
	"preferences.usenet-cleanup-all": "preferences.usenet-cleanup-action",
	"preferences.usenet-desktop-limits": "preferences.usenet-archive-safety",
	"preferences.usenet-android-limits": "preferences.usenet-archive-safety",
	"preferences.usenet-max-entries": "preferences.usenet-archive-safety",
	"preferences.usenet-max-expanded": "preferences.usenet-archive-safety",
	"preferences.usenet-max-entry": "preferences.usenet-archive-safety",
	"preferences.usenet-max-depth": "preferences.usenet-archive-safety",
	"preferences.usenet-max-ratio": "preferences.usenet-archive-safety",
	"preferences.usenet-free-space-reserve": "preferences.usenet-archive-safety",
	"preferences.usenet-max-active-time": "preferences.usenet-archive-safety",
	"cloudSinks.label": "cloudSinks.sinks",
	"cloudSinks.protocol": "cloudSinks.sinks",
	"cloudSinks.endpoint": "cloudSinks.sinks",
	"cloudSinks.basePath": "cloudSinks.sinks",
	"cloudSinks.username": "cloudSinks.sinks",
	"cloudSinks.password": "cloudSinks.sinks",
	"cloudSinks.credentials": "cloudSinks.sinks",
	"cloudSinks.allowInsecure": "cloudSinks.sinks",
	"cloudSinks.s3Endpoint": "cloudSinks.sinks",
	"cloudSinks.s3Region": "cloudSinks.sinks",
	"cloudSinks.s3Bucket": "cloudSinks.sinks",
	"cloudSinks.s3AccessKey": "cloudSinks.sinks",
	"cloudSinks.s3SecretKey": "cloudSinks.sinks",
	"cloudSinks.s3Prefix": "cloudSinks.sinks",
	"cloudSinks.s3PathStyle": "cloudSinks.sinks",
	"cloudSinks.host": "cloudSinks.sinks",
	"cloudSinks.port": "cloudSinks.sinks",
	"cloudSinks.privateKey": "cloudSinks.sinks",
	"cloudSinks.sftpBasePath": "cloudSinks.sinks",
	"cloudSinks.ftpSecure": "cloudSinks.sinks",
	"cloudSinks.postAction": "cloudSinks.sinks",
	"cloudSinks.moveTarget": "cloudSinks.sinks",
	"cloudSinks.ruleTarget": "cloudSinks.rules",
	"cloudSinks.ruleCategories": "cloudSinks.rules",
	"cloudSinks.ruleTaskKinds": "cloudSinks.rules",
	"cloudSinks.ruleExtensions": "cloudSinks.rules",
	"cloudSinks.ruleSize": "cloudSinks.rules",
	"cloudSinks.ruleEnabled": "cloudSinks.rules",
};

const BASIC_KEYS = [
	"preferences.language",
	"preferences.run-mode",
	"preferences.run-mode-standard",
	"preferences.run-mode-tray",
	"preferences.open-at-login",
	"preferences.keep-window-state",
	"preferences.auto-resume-all",
	"preferences.purge-record-on-start",
	"preferences.default-dir",
	"preferences.history-directories",
	"preferences.favorite-directory",
	"preferences.unfavorite-directory",
	"preferences.remove-history-directory",
	"preferences.file-category-dirs",
	"preferences.file-category-music",
	"preferences.file-category-video",
	"preferences.file-category-image",
	"preferences.file-category-document",
	"preferences.file-category-compressed",
	"preferences.file-category-program",
	"preferences.file-category-rss",
	"preferences.task-routing-rules",
	"preferences.task-routing-rule-add",
	"preferences.speed-limit-enabled",
	"preferences.transfer-speed-upload",
	"preferences.transfer-speed-download",
	"preferences.bt-save-metadata",
	"preferences.bt-force-encryption",
	"preferences.keep-seeding",
	"preferences.seed-ratio",
	"preferences.seed-time",
	"preferences.max-concurrent-downloads",
	"preferences.connections-per-task",
	"preferences.auto-retry",
	"preferences.auto-retry-strategy",
	"preferences.auto-retry-strategy-exponential",
	"preferences.auto-retry-strategy-static",
	"preferences.auto-retry-interval",
	"preferences.worker-max-retries",
	"preferences.auto-detect-low-speed-tasks",
	"preferences.low-speed-threshold",
	"preferences.new-task-show-downloading",
	"preferences.task-completed-notify",
	"preferences.clipboard-watch",
	"preferences.clipboard-watch-ext",
	"preferences.prevent-sleep-while-downloading",
	"preferences.shutdown-when-complete",
	"preferences.no-confirm-before-delete-task",
	"preferences.use-remote-file-time",
	"preferences.auto-file-renaming",
];

const APPEARANCE_KEYS = [
	"preferences.appearance-theme",
	"preferences.theme-auto",
	"preferences.theme-light",
	"preferences.theme-dark",
	"preferences.font-family",
	"preferences.font-family-system",
	"preferences.font-family-rounded",
	"preferences.font-family-serif",
	"preferences.font-family-mono",
	"preferences.font-size",
	"preferences.font-size-small",
	"preferences.font-size-default",
	"preferences.font-size-large",
	"preferences.font-size-extra-large",
	"preferences.task-list-style",
	"preferences.task-list-style-compact",
	"preferences.task-list-style-card",
	"preferences.appearance-window",
	"preferences.sidebar-collapsed",
	"preferences.hide-app-menu",
	"preferences.auto-hide-window",
	"preferences.tray-speedometer",
];

const ADVANCED_KEYS = [
	"preferences.completion-script",
	"preferences.completion-script-enabled",
	"preferences.completion-script-command",
	"preferences.completion-script-args",
	"preferences.completion-script-timeout",
	"preferences.proxy",
	"preferences.enable-proxy",
	"preferences.enable-p2p-proxy",
	"preferences.proxy-http-profile",
	"preferences.proxy-p2p-profile",
	"preferences.proxy-p2p-tcp-server",
	"preferences.proxy-p2p-tcp-bypass",
	"preferences.proxy-p2p-udp-server",
	"preferences.proxy-p2p-udp-bypass",
	"preferences.proxy-p2p-udp-tips",
	"preferences.proxy-scope-label",
	"preferences.proxy-scope-download",
	"preferences.proxy-scope-update-app",
	"preferences.proxy-scope-update-trackers",
	"preferences.doh",
	"preferences.doh-enable",
	"preferences.doh-provider",
	"preferences.doh-provider-cloudflare",
	"preferences.doh-provider-google",
	"preferences.doh-provider-quad9",
	"preferences.doh-provider-custom",
	"preferences.doh-url",
	"preferences.doh-bootstrap",
	"preferences.doh-fallback",
	"preferences.bt-tracker",
	"preferences.auto-sync-tracker",
	"preferences.bt-max-peers-per-torrent",
	"preferences.bt-max-outstanding-per-peer",
	"preferences.bt-max-connections",
	"preferences.bt-ban-corrupt-peers",
	"preferences.bt-ban-corrupt-strikes",
	"preferences.bt-enable-upnp",
	"preferences.bt-upnp-lease",
	"preferences.bt-enable-lsd",
	"preferences.bt-create-subfolder",
	"preferences.bt-encryption-policy",
	"preferences.bt-encryption-plaintext",
	"preferences.bt-encryption-prefer",
	"preferences.bt-encryption-require",
	"preferences.bt-listen-v6",
	"preferences.http-reliability",
	"preferences.connect-timeout",
	"preferences.nzb-body-timeout",
	"preferences.lowest-speed-limit",
	"preferences.lowest-speed-limit-timeout",
	"preferences.uri-selector",
	"preferences.uri-selector-feedback",
	"preferences.uri-selector-inorder",
	"preferences.uri-selector-adaptive",
	"preferences.storage",
	"preferences.file-allocation",
	"preferences.file-allocation-falloc",
	"preferences.file-allocation-trunc",
	"preferences.file-allocation-none",
	"preferences.ed2k-server",
	"preferences.ed2k-kad",
	"preferences.ed2k-enable-kad",
	"preferences.ed2k-kad-port",
	"preferences.gift-integration",
	"preferences.gift-enabled",
	"preferences.gift-host",
	"preferences.gift-port",
	"preferences.ftp-sftp-settings",
	"preferences.ftp-username",
	"preferences.ftp-password",
	"preferences.sftp-private-key",
	"preferences.sftp-key-passphrase",
	"preferences.m3u8-output-format",
	"preferences.m3u8-output-format-label",
	"preferences.media-settings",
	"preferences.media-format",
	"preferences.external-engine-enable",
	"preferences.external-engine-ip",
	"preferences.external-engine-port",
	"preferences.external-engine-secret",
	"preferences.rpc",
	"preferences.rpc-host",
	"preferences.rpc-listen-port",
	"preferences.rpc-secret",
	"preferences.pbh",
	"preferences.pbh-enable",
	"preferences.pbh-listen-port",
	"preferences.pbh-rpc-secret",
	"preferences.port",
	"preferences.bt-port",
	"preferences.dht-port",
	"preferences.ed2k-port",
	"preferences.download-protocol",
	"preferences.protocols-default-client",
	"preferences.protocols-magnet",
	"preferences.protocols-thunder",
	"preferences.protocols-ed2k",
	"preferences.protocols-adc",
	"preferences.protocols-gnutella",
	"preferences.protocols-g2",
	"preferences.user-agent",
	"preferences.mock-user-agent",
	"preferences.cookies",
	"preferences.saved-cookies",
	"preferences.saved-credentials",
	"preferences.netrc",
	"preferences.no-netrc",
	"preferences.developer",
	"preferences.app-log-path",
	"preferences.engine-overrides",
	"preferences.session-reset",
	"preferences.factory-reset",
	"preferences.auto-update",
	"preferences.auto-check-update",
];

const USENET_KEYS = [
	"preferences.usenet-profiles",
	"preferences.usenet-cleanup",
	"preferences.usenet-cleanup-action",
	"preferences.usenet-cleanup-keep",
	"preferences.usenet-cleanup-par2",
	"preferences.usenet-cleanup-all",
	"preferences.usenet-archive-safety",
	"preferences.usenet-desktop-limits",
	"preferences.usenet-android-limits",
	"preferences.usenet-max-entries",
	"preferences.usenet-max-expanded",
	"preferences.usenet-max-entry",
	"preferences.usenet-max-depth",
	"preferences.usenet-max-ratio",
	"preferences.usenet-free-space-reserve",
	"preferences.usenet-max-active-time",
];

const CLOUD_SINK_KEYS = [
	"cloudSinks.sinks",
	"cloudSinks.label",
	"cloudSinks.protocol",
	"cloudSinks.endpoint",
	"cloudSinks.basePath",
	"cloudSinks.username",
	"cloudSinks.password",
	"cloudSinks.credentials",
	"cloudSinks.allowInsecure",
	"cloudSinks.s3Endpoint",
	"cloudSinks.s3Region",
	"cloudSinks.s3Bucket",
	"cloudSinks.s3AccessKey",
	"cloudSinks.s3SecretKey",
	"cloudSinks.s3Prefix",
	"cloudSinks.s3PathStyle",
	"cloudSinks.host",
	"cloudSinks.port",
	"cloudSinks.privateKey",
	"cloudSinks.sftpBasePath",
	"cloudSinks.ftpSecure",
	"cloudSinks.postAction",
	"cloudSinks.moveTarget",
	"cloudSinks.rules",
	"cloudSinks.ruleTarget",
	"cloudSinks.ruleEnabled",
	"cloudSinks.ruleCategories",
	"cloudSinks.ruleTaskKinds",
	"cloudSinks.ruleExtensions",
	"cloudSinks.ruleSize",
	"cloudSinks.recentJobs",
];

const SYNC_KEYS = [
	"sync.cloud-sync",
	"sync.cloud-sync-server-url",
	"sync.cloud-sync-categories",
	"sync.cloud-sync-auto",
	"sync.category-appearance",
	"sync.category-language",
	"sync.category-network",
	"sync.category-tracker",
	"sync.category-directories",
	"sync.category-download",
	"sync.category-media",
	"sync.category-stats",
	"sync.category-task-routing",
	"sync.category-notifications",
	"sync.category-low-speed",
	"sync.category-system",
	"sync.category-engine",
	"sync.category-dns",
	"sync.category-protocols",
	"sync.category-logs",
	"sync.category-credentials",
	"sync.category-bittorrent",
	"sync.category-ports",
	"sync.category-ftp",
	"sync.category-g2-gnutella",
	"sync.category-usenet",
	"sync.category-misc",
];

function makeDefinitions(
	route: PreferenceSearchRoute,
	keys: string[],
): PreferenceSearchDefinition[] {
	return keys.map((key) => ({ key, route, target: TARGETS[key] }));
}

const SEARCH_DEFINITIONS = [
	...makeDefinitions("basic", BASIC_KEYS),
	...makeDefinitions("appearance", APPEARANCE_KEYS),
	...makeDefinitions("advanced", ADVANCED_KEYS),
	...makeDefinitions("usenet", USENET_KEYS),
	...makeDefinitions("cloud-sinks", CLOUD_SINK_KEYS),
	...makeDefinitions("sync", SYNC_KEYS),
].map((definition) => {
	if (definition.key === "preferences.run-mode") {
		return { ...definition, availability: "macos" as const };
	}
	if (
		definition.key === "preferences.run-mode-standard" ||
		definition.key === "preferences.run-mode-tray"
	) {
		return { ...definition, availability: "macos" as const };
	}
	if (
		definition.key === "preferences.clipboard-watch" ||
		definition.key === "preferences.clipboard-watch-ext" ||
		definition.key === "preferences.shutdown-when-complete" ||
		definition.key.startsWith("preferences.font-family") ||
		definition.key === "preferences.appearance-window" ||
		definition.key === "preferences.sidebar-collapsed" ||
		definition.key === "preferences.hide-app-menu" ||
		definition.key === "preferences.auto-hide-window" ||
		definition.key === "preferences.auto-update" ||
		definition.key === "preferences.auto-check-update" ||
		definition.key === "preferences.usenet-desktop-limits"
	) {
		return { ...definition, availability: "desktop" as const };
	}
	if (definition.key === "preferences.tray-speedometer") {
		return { ...definition, availability: "macos" as const };
	}
	if (definition.key === "preferences.usenet-android-limits") {
		return { ...definition, availability: "android" as const };
	}
	return definition;
});

function startsWithAny(value: string, prefixes: string[]): boolean {
	return prefixes.some(
		(prefix) => value === prefix || value.startsWith(prefix),
	);
}

function getRouteForKey(key: string): PreferenceSearchRoute | null {
	if (key.startsWith("cloudSinks.")) {
		return "cloud-sinks";
	}
	if (key.startsWith("sync.")) {
		return "sync";
	}

	if (!key.startsWith("preferences.")) {
		return null;
	}

	const localKey = key.slice("preferences.".length);
	if (startsWithAny(localKey, APPEARANCE_PREFIXES)) {
		return "appearance";
	}
	if (localKey.startsWith("usenet-")) {
		return "usenet";
	}
	if (startsWithAny(localKey, ADVANCED_PREFIXES)) {
		return "advanced";
	}

	return "basic";
}

function getTranslationValue(
	bundle: TranslationBundle | undefined,
	key: string,
): string | undefined {
	let value: unknown = bundle;
	for (const part of key.split(".")) {
		if (!value || typeof value !== "object") {
			return undefined;
		}
		value = (value as TranslationBundle)[part];
	}
	return typeof value === "string" ? value : undefined;
}

function normalizeSearchText(value: string): string {
	return value
		.normalize("NFKD")
		.toLocaleLowerCase("en-US")
		.replace(/\p{Mark}+/gu, "")
		.replace(/ı/g, "i")
		.replace(/ß/g, "ss")
		.replace(/[\u2010-\u2015]/g, "-")
		.replace(/[^\p{Letter}\p{Number}]+/gu, " ")
		.trim();
}

function isAvailable(
	definition: PreferenceSearchDefinition,
	environment: PreferenceSearchEnvironment,
): boolean {
	switch (definition.availability) {
		case "android":
			return !!environment.android;
		case "desktop":
			return !environment.android && environment.renderer !== false;
		case "macos":
			return !!environment.macOS;
		default:
			return true;
	}
}

export function buildPreferenceSearchEntries(
	primaryBundle: TranslationBundle | undefined,
	fallbackBundle: TranslationBundle | undefined,
	translate: Translate,
	environment: PreferenceSearchEnvironment = {},
): PreferenceSearchEntry[] {
	return SEARCH_DEFINITIONS.flatMap((definition) => {
		if (!isAvailable(definition, environment)) {
			return [];
		}

		const primaryLabel = getTranslationValue(primaryBundle, definition.key);
		const fallbackLabel = getTranslationValue(fallbackBundle, definition.key);
		const sourceLabel = primaryLabel || fallbackLabel;
		const translatedLabel = String(translate(definition.key) || "").trim();
		const label =
			translatedLabel && translatedLabel !== definition.key
				? translatedLabel
				: (sourceLabel || "").trim();
		if (!label || label.includes("{{")) {
			return [];
		}

		const keySearchText = definition.key
			.replace(/^[^.]+\./, "")
			.replace(/[-_.]/g, " ");
		const aliases = [label, primaryLabel, fallbackLabel]
			.filter((value): value is string => !!value && !value.includes("{{"))
			.join(" ");
		return [
			{
				key: definition.key,
				label,
				route: definition.route,
				target: definition.target || definition.key,
				searchText: normalizeSearchText(`${aliases} ${keySearchText}`),
			},
		];
	});
}

export function filterPreferenceSearchEntries(
	entries: PreferenceSearchEntry[],
	query: string,
	limit = Number.POSITIVE_INFINITY,
): PreferenceSearchEntry[] {
	const normalizedQuery = normalizeSearchText(query);
	if (!normalizedQuery) {
		return [];
	}
	const terms = normalizedQuery.split(" ").filter(Boolean);

	return entries
		.map((entry) => {
			const matches = terms.every((term) => entry.searchText.includes(term));
			if (!matches) {
				return null;
			}

			const label = normalizeSearchText(entry.label);
			const score =
				label === normalizedQuery
					? 0
					: label.startsWith(normalizedQuery)
						? 1
						: label.includes(normalizedQuery)
							? 2
							: 3;
			return { entry, score };
		})
		.filter(
			(result): result is { entry: PreferenceSearchEntry; score: number } =>
				!!result,
		)
		.sort((left, right) => {
			if (left.score !== right.score) {
				return left.score - right.score;
			}
			return left.entry.label.localeCompare(right.entry.label);
		})
		.slice(0, limit)
		.map(({ entry }) => entry);
}

export function resolvePreferenceSearchTarget<T>(
	settingKey: string,
	label: string,
	markers: PreferenceSearchTargetMarker<T>[],
	labels: PreferenceSearchTargetLabel<T>[],
): T | null {
	const exactMarker = markers.find(
		(marker) => marker.keys.length === 1 && marker.keys[0] === settingKey,
	);
	if (exactMarker) {
		return exactMarker.target;
	}

	const needle = normalizeSearchText(label);
	if (needle) {
		const candidates = labels.map((candidate) => ({
			...candidate,
			text: normalizeSearchText(candidate.text),
		}));
		const exactLabel = candidates.find(
			(candidate) => candidate.text === needle,
		);
		if (exactLabel) {
			return exactLabel.target;
		}

		const partialLabel = candidates
			.filter((candidate) => candidate.text.includes(needle))
			.sort((left, right) => left.text.length - right.text.length)[0];
		if (partialLabel) {
			return partialLabel.target;
		}
	}

	return (
		markers.find((marker) => marker.keys.includes(settingKey))?.target || null
	);
}

export { getRouteForKey, normalizeSearchText };
