import type { SavedCredential } from "./credential";
import type {
	UsenetArchiveLimits,
	UsenetCleanupMode,
	UsenetProviderProfile,
} from "./usenet";

export const FONT_FAMILY_OPTIONS = [
	"system",
	"rounded",
	"serif",
	"mono",
] as const;
export type FontFamily = (typeof FONT_FAMILY_OPTIONS)[number];
export const DEFAULT_FONT_FAMILY: FontFamily = "system";

export const FONT_SIZE_OPTIONS = [
	"small",
	"default",
	"large",
	"extra-large",
] as const;
export type FontSize = (typeof FONT_SIZE_OPTIONS)[number];
export const DEFAULT_FONT_SIZE: FontSize = "default";

export const normalizeConfigOption = <T extends readonly string[]>(
	value: unknown,
	options: T,
	fallback: T[number],
): T[number] => {
	return typeof value === "string" && options.includes(value)
		? (value as T[number])
		: fallback;
};

interface TaskRoutingRule {
	id: string;
	label: string;
	pattern: string;
	dir: string;
	enabled: boolean;
}

export interface AppConfig {
	locale: string;
	theme?: string;
	fontFamily?: FontFamily;
	fontSize?: FontSize;
	dir?: string;
	split?: number;
	allProxy?: string;
	cookie?: string;
	referer?: string;
	userAgent?: string;
	followTorrent?: boolean;
	newTaskShowDownloading?: boolean;
	fileCategoryDirs?: Record<string, string>;
	"ftp-user"?: string;
	"ftp-passwd"?: string;
	"sftp-private-key"?: string;
	"sftp-private-key-passphrase"?: string;
	traySpeedometer?: boolean | string;
	engineMode?: "MAX" | "LIMIT";
	taskListStyle?: "compact" | "card";
	sidebarCollapsed?: boolean | string;
	runMode?: number | string;
	rpcSecret?: string;
	externalEngineEnabled?: boolean | string;
	externalEngineHost?: string;
	externalEnginePort?: number | string;
	externalEngineSecret?: string;
	engineOverrides?: Record<string, string | number | boolean | null>;
	autoCheckUpdate?: boolean;
	lastCheckUpdateTime?: number;
	autoSyncTracker?: boolean;
	trackerSource?: string[];
	btTracker?: string;
	btCreateSubfolder?: boolean;
	lastSyncTrackerTime?: number;
	maxOverallDownloadLimit?: number;
	maxOverallUploadLimit?: number;
	historyDirectories?: string[];
	favoriteDirectories?: string[];
	savedCredentials?: SavedCredential[];
	usenetProfiles?: UsenetProviderProfile[];
	usenetArchiveLimits?: UsenetArchiveLimits;
	usenetCleanupMode?: UsenetCleanupMode;
	usenetLimitsAdjusted?: boolean;
	protocols?: {
		magnet?: boolean | string;
		thunder?: boolean | string;
		ed2k?: boolean | string;
		adc?: boolean | string;
		gnutella?: boolean | string;
		g2?: boolean | string;
	};
	proxy?: ProxyConfig;
	openAtLogin?: boolean;
	preventSleepWhileDownloading?: boolean;
	purgeRecordOnStart?: boolean;
	autoDetectLowSpeedTasks?: boolean;
	lowSpeedThreshold?: number;
	lowSpeedStrikeThreshold?: number;
	lowSpeedCooldownMs?: number;
	appLogPath?: string;
	logDirOverride?: string;
	taskRoutingRules?: TaskRoutingRule[];
	completionScriptEnabled?: boolean;
	completionScriptCommand?: string;
	completionScriptArgs?: string;
	completionScriptTimeoutMs?: number;
	shutdownWhenComplete?: boolean;
	clipboardWatch?: boolean;
	clipboardWatchExtensions?: string[];
	clipboardWatchNoticeSeen?: boolean;
	legalAccepted?: boolean;
	dohEnable?: boolean;
	dohUrl?: string;
	dohBootstrap?: string;
	dohFallback?: boolean;
	dohProvider?: string;
	ed2KEnableKad?: boolean | string;
	ed2KKadPort?: number | string;
	cloudSyncEnabled?: boolean;
	cloudSyncAuto?: boolean;
	cloudSyncCategories?: string[];
	cloudSyncToken?: string;
	cloudSyncLastAt?: number;
	cloudSyncServerUrl?: string;
	cloudSyncCategoryTimestamps?: Record<string, number>;
	[key: string]: unknown;
}

interface ProxyProfile {
	enable?: boolean;
	server?: string;
	bypass?: string;
	scope?: string[];
}

interface P2pProxyProfile {
	enable?: boolean;
	server?: string;
	bypass?: string;
	udp?: P2pUdpProxyProfile;
}

interface P2pUdpProxyProfile {
	server?: string;
	bypass?: string;
}

export interface ProxyConfig {
	http?: ProxyProfile;
	p2p?: P2pProxyProfile;
	enable?: boolean;
	server?: string;
	bypass?: string;
	scope?: string[];
}

const PROXY_SCOPE_OPTIONS = [
	"download",
	"update-app",
	"update-trackers",
] as const;

type ProxyScope = (typeof PROXY_SCOPE_OPTIONS)[number];

interface NormalizedProxyProfile {
	enable: boolean;
	server: string;
	bypass: string;
	scope: ProxyScope[];
}

interface NormalizedP2pProxyProfile {
	enable: boolean;
	server: string;
	bypass: string;
	udp: NormalizedP2pUdpProxyProfile;
}

interface NormalizedP2pUdpProxyProfile {
	server: string;
	bypass: string;
}

export interface NormalizedProxyConfig {
	http: NormalizedProxyProfile;
	p2p: NormalizedP2pProxyProfile;
}

const proxyBoolean = (value: unknown): boolean => {
	if (typeof value === "boolean") {
		return value;
	}
	if (typeof value === "number") {
		return Number.isFinite(value) && value !== 0;
	}
	if (typeof value === "string") {
		return ["true", "1", "yes", "on"].includes(value.trim().toLowerCase());
	}
	return false;
};

const parseProxyPort = (value: string): number | null => {
	if (!/^\d+$/.test(value)) {
		return null;
	}
	const port = Number(value);
	return Number.isSafeInteger(port) && port > 0 && port <= 65535 ? port : null;
};

const parseIpv4 = (value: string): number[] | null => {
	const parts = value.split(".");
	if (
		parts.length !== 4 ||
		parts.some((part) => !/^(?:0|[1-9]\d*)$/.test(part) || Number(part) > 255)
	) {
		return null;
	}
	return parts.map(Number);
};

const parseIpv6 = (value: string): number[] | null => {
	const host = value.toLowerCase();
	if (!host || host.includes("%")) {
		return null;
	}
	const halves = host.split("::");
	if (halves.length > 2) {
		return null;
	}

	const leftParts = halves[0] ? halves[0].split(":") : [];
	const rightParts =
		halves.length === 2 && halves[1] ? halves[1].split(":") : [];
	const allParts = [...leftParts, ...rightParts];
	const parseParts = (parts: string[], offset: number): number[] | null => {
		const parsed: number[] = [];
		for (let index = 0; index < parts.length; index += 1) {
			const part = parts[index];
			if (!part) {
				return null;
			}
			if (part.includes(".")) {
				if (offset + index !== allParts.length - 1) {
					return null;
				}
				const ipv4 = parseIpv4(part);
				if (!ipv4) {
					return null;
				}
				parsed.push((ipv4[0] << 8) | ipv4[1], (ipv4[2] << 8) | ipv4[3]);
				continue;
			}
			if (!/^[0-9a-f]{1,4}$/i.test(part)) {
				return null;
			}
			parsed.push(Number.parseInt(part, 16));
		}
		return parsed;
	};
	const left = parseParts(leftParts, 0);
	const right = parseParts(rightParts, leftParts.length);
	if (!left || !right) {
		return null;
	}
	const groups = left.length + right.length;
	if (halves.length === 1) {
		return groups === 8 ? left : null;
	}
	if (groups >= 8) {
		return null;
	}
	return [...left, ...Array<number>(8 - groups).fill(0), ...right];
};

type ParsedIp =
	| { family: "ipv4"; segments: number[] }
	| { family: "ipv6"; segments: number[] };

const parseIp = (value: string): ParsedIp | null => {
	const ipv4 = parseIpv4(value);
	if (ipv4) {
		return { family: "ipv4", segments: ipv4 };
	}
	const ipv6 = parseIpv6(value);
	return ipv6 ? { family: "ipv6", segments: ipv6 } : null;
};

const formatIpv6 = (segments: number[]): string => {
	let longestStart = -1;
	let longestLength = 0;
	for (let index = 0; index < segments.length; ) {
		if (segments[index] !== 0) {
			index += 1;
			continue;
		}
		const start = index;
		while (index < segments.length && segments[index] === 0) {
			index += 1;
		}
		if (index - start > longestLength) {
			longestStart = start;
			longestLength = index - start;
		}
	}
	if (longestLength < 2) {
		return segments.map((segment) => segment.toString(16)).join(":");
	}
	const before = segments
		.slice(0, longestStart)
		.map((segment) => segment.toString(16))
		.join(":");
	const after = segments
		.slice(longestStart + longestLength)
		.map((segment) => segment.toString(16))
		.join(":");
	if (!before && !after) {
		return "::";
	}
	if (!before) {
		return `::${after}`;
	}
	if (!after) {
		return `${before}::`;
	}
	return `${before}::${after}`;
};

const maskIp = (ip: ParsedIp, prefix: number): ParsedIp => {
	const width = ip.family === "ipv4" ? 8 : 16;
	const masked = ip.segments.map((segment, index) => {
		const remaining = prefix - index * width;
		if (remaining >= width) {
			return segment;
		}
		if (remaining <= 0) {
			return 0;
		}
		const mask =
			(((1 << width) - 1) << (width - remaining)) & ((1 << width) - 1);
		return segment & mask;
	});
	return { ...ip, segments: masked };
};

const formatIp = (ip: ParsedIp): string =>
	ip.family === "ipv4" ? ip.segments.join(".") : formatIpv6(ip.segments);

const parseCidrSuffix = (
	value: string,
): { cidr: number; port: number | null } | null => {
	const separator = value.lastIndexOf(":");
	const prefix = separator >= 0 ? value.slice(0, separator) : value;
	if (!/^\d+$/.test(prefix)) {
		return null;
	}
	let port: number | null = null;
	if (separator >= 0) {
		port = parseProxyPort(value.slice(separator + 1));
		if (port === null) {
			return null;
		}
	}
	return { cidr: Number(prefix), port };
};

const normalizeBypassEntry = (raw: string): string | null => {
	const entry = raw.trim();
	if (!entry) {
		return null;
	}
	if (entry === "*") {
		return entry;
	}

	let host = entry;
	let port: number | null = null;
	let cidr: number | null = null;
	let bracketed = false;
	if (entry.startsWith("[")) {
		const close = entry.indexOf("]");
		if (close <= 1) {
			return null;
		}
		bracketed = true;
		host = entry.slice(1, close);
		const suffix = entry.slice(close + 1);
		if (suffix.startsWith("/")) {
			const parsed = parseCidrSuffix(suffix.slice(1));
			if (!parsed) {
				return null;
			}
			cidr = parsed.cidr;
			port = parsed.port;
		} else if (suffix) {
			if (!suffix.startsWith(":")) {
				return null;
			}
			port = parseProxyPort(suffix.slice(1));
			if (port === null) {
				return null;
			}
		}
	} else {
		const slash = entry.indexOf("/");
		if (slash >= 0) {
			host = entry.slice(0, slash);
			const parsed = parseCidrSuffix(entry.slice(slash + 1));
			if (!parsed) {
				return null;
			}
			cidr = parsed.cidr;
			port = parsed.port;
		} else if (entry.split(":").length === 2) {
			const separator = entry.lastIndexOf(":");
			host = entry.slice(0, separator);
			port = parseProxyPort(entry.slice(separator + 1));
			if (port === null) {
				return null;
			}
		}
	}

	const ip = parseIp(host);
	if (bracketed && !ip) {
		return null;
	}
	if (ip) {
		const maxPrefix = ip.family === "ipv6" ? 128 : 32;
		if (
			cidr !== null &&
			(!Number.isInteger(cidr) || cidr < 0 || cidr > maxPrefix)
		) {
			return null;
		}
		const normalized = cidr === null ? ip : maskIp(ip, cidr);
		const normalizedHost = formatIp(normalized);
		if (ip.family === "ipv6") {
			const body = cidr === null ? normalizedHost : `${normalizedHost}/${cidr}`;
			return port === null
				? body
				: cidr === null
					? `[${body}]:${port}`
					: `[${normalizedHost}]/${cidr}:${port}`;
		}
		return `${normalizedHost}${cidr === null ? "" : `/${cidr}`}${port === null ? "" : `:${port}`}`;
	}

	if (cidr !== null || host.includes(":") || host.includes("/")) {
		return null;
	}
	if (
		host.includes(".") &&
		host.split(".").every((label) => /^\d+$/.test(label))
	) {
		return null;
	}
	host = host.replace(/^\.+|\.+$/g, "").toLowerCase();
	if (
		!host ||
		host.length > 253 ||
		host
			.split(".")
			.some(
				(label) =>
					!label ||
					label.length > 63 ||
					!/^[a-z0-9_-]+$/i.test(label) ||
					label.startsWith("-") ||
					label.endsWith("-"),
			)
	) {
		return null;
	}
	return `${host}${port === null ? "" : `:${port}`}`;
};

export const normalizeProxyBypass = (value: unknown): string => {
	const raw = Array.isArray(value)
		? value
				.filter((entry): entry is string => typeof entry === "string")
				.join(",")
		: typeof value === "string"
			? value
			: "";
	const entries: string[] = [];
	for (const token of raw.split(/[,\r\n]/)) {
		const entry = normalizeBypassEntry(token);
		if (entry && !entries.includes(entry)) {
			entries.push(entry);
		}
	}
	return entries.join(",");
};

const normalizeProxyScopes = (value: unknown): ProxyScope[] => {
	if (value === undefined || value === null) {
		return [...PROXY_SCOPE_OPTIONS];
	}
	const values = Array.isArray(value)
		? value.flatMap((entry) =>
				typeof entry === "string" ? entry.split(/[,\r\n]/) : [],
			)
		: typeof value === "string"
			? value.split(/[,\r\n]/)
			: [];
	const scopes: ProxyScope[] = [];
	for (const candidate of values) {
		if (typeof candidate === "string") {
			const normalized = candidate.trim().toLowerCase();
			if (!(PROXY_SCOPE_OPTIONS as readonly string[]).includes(normalized)) {
				continue;
			}
			const scope = normalized as ProxyScope;
			if (!scopes.includes(scope)) {
				scopes.push(scope);
			}
		}
	}
	return scopes;
};

export const normalizeProxyConfig = (value: unknown): NormalizedProxyConfig => {
	const root =
		value && typeof value === "object" && !Array.isArray(value)
			? (value as Record<string, unknown>)
			: {};
	const nestedHttp =
		root.http && typeof root.http === "object" && !Array.isArray(root.http)
			? (root.http as Record<string, unknown>)
			: undefined;
	const hasLegacyFields = ["enable", "server", "bypass", "scope"].some((key) =>
		Object.hasOwn(root, key),
	);
	const legacyHttp = hasLegacyFields ? root : {};
	const http = nestedHttp ? { ...legacyHttp, ...nestedHttp } : legacyHttp;
	const p2p =
		root.p2p && typeof root.p2p === "object" && !Array.isArray(root.p2p)
			? (root.p2p as Record<string, unknown>)
			: {};
	const nestedUdp =
		p2p.udp && typeof p2p.udp === "object" && !Array.isArray(p2p.udp)
			? (p2p.udp as Record<string, unknown>)
			: {};
	return {
		http: {
			enable: proxyBoolean(http.enable),
			server: typeof http.server === "string" ? http.server.trim() : "",
			bypass: normalizeProxyBypass(http.bypass),
			scope: normalizeProxyScopes(http.scope),
		},
		p2p: {
			enable: proxyBoolean(p2p.enable),
			server: typeof p2p.server === "string" ? p2p.server.trim() : "",
			bypass: normalizeProxyBypass(p2p.bypass),
			udp: {
				server:
					typeof nestedUdp.server === "string" ? nestedUdp.server.trim() : "",
				bypass: normalizeProxyBypass(nestedUdp.bypass),
			},
		},
	};
};

export const normalizeNetworkProxyConfig = (
	data: Record<string, unknown>,
): Record<string, unknown> => {
	const setting = (key: string): string =>
		typeof data[key] === "string" ? (data[key] as string) : "";
	const hasLegacyP2pTcp = ["p2p-proxy", "p2p-no-proxy"].some((key) =>
		Object.hasOwn(data, key),
	);
	const hasLegacyP2pUdp = ["p2p-udp-proxy", "p2p-udp-no-proxy"].some((key) =>
		Object.hasOwn(data, key),
	);
	const hasLegacyP2p = hasLegacyP2pTcp || hasLegacyP2pUdp;
	const rawProxy =
		data.proxy && typeof data.proxy === "object" && !Array.isArray(data.proxy)
			? (data.proxy as Record<string, unknown>)
			: undefined;
	const rawP2p =
		rawProxy?.p2p &&
		typeof rawProxy.p2p === "object" &&
		!Array.isArray(rawProxy.p2p)
			? (rawProxy.p2p as Record<string, unknown>)
			: {};
	const rawUdp =
		rawP2p.udp && typeof rawP2p.udp === "object" && !Array.isArray(rawP2p.udp)
			? (rawP2p.udp as Record<string, unknown>)
			: {};
	const nonEmpty = (value: unknown): boolean =>
		typeof value === "string" && value.trim().length > 0;
	const hasNestedP2p =
		Object.hasOwn(rawProxy ?? {}, "p2p") &&
		(proxyBoolean(rawP2p.enable) ||
			nonEmpty(rawP2p.server) ||
			nonEmpty(rawP2p.bypass) ||
			nonEmpty(rawUdp.server) ||
			nonEmpty(rawUdp.bypass) ||
			rawProxy?.["p2p-profile-explicit"] === true);
	const legacyTcpServer = setting("p2p-proxy").trim();
	const legacyTcpBypass = normalizeProxyBypass(setting("p2p-no-proxy"));
	const legacyUdpServer = setting("p2p-udp-proxy").trim();
	const legacyUdpBypass = normalizeProxyBypass(setting("p2p-udp-no-proxy"));
	const flattenedUdpInheritsTcp =
		legacyUdpServer.length > 0 &&
		legacyUdpServer === legacyTcpServer &&
		legacyUdpBypass === legacyTcpBypass;
	const hasNestedUdp =
		Object.hasOwn(rawP2p, "udp") &&
		(nonEmpty(rawUdp.server) ||
			nonEmpty(rawUdp.bypass) ||
			!hasLegacyP2pUdp ||
			flattenedUdpInheritsTcp);

	if ("proxy" in data) {
		const proxy = normalizeProxyConfig(data.proxy);
		if (hasLegacyP2p && !hasNestedP2p) {
			proxy.p2p = {
				enable: Boolean(legacyTcpServer || legacyUdpServer),
				server: legacyTcpServer,
				bypass: legacyTcpBypass,
				udp: { server: legacyUdpServer, bypass: legacyUdpBypass },
			};
		} else if (hasLegacyP2pUdp && !hasNestedUdp) {
			proxy.p2p.udp = {
				server: legacyUdpServer,
				bypass: legacyUdpBypass,
			};
		}
		return { ...data, proxy };
	}

	const hasLegacyHttp = ["all-proxy", "no-proxy"].some((key) =>
		Object.hasOwn(data, key),
	);
	if (!hasLegacyHttp && !hasLegacyP2p) {
		return data;
	}

	return {
		...data,
		proxy: normalizeProxyConfig({
			http: {
				enable: Boolean(setting("all-proxy").trim()),
				server: setting("all-proxy"),
				bypass: setting("no-proxy"),
				scope: ["download"],
			},
			p2p: {
				enable: Boolean(
					setting("p2p-proxy").trim() || setting("p2p-udp-proxy").trim(),
				),
				server: setting("p2p-proxy"),
				bypass: setting("p2p-no-proxy"),
				udp: {
					server: setting("p2p-udp-proxy"),
					bypass: setting("p2p-udp-no-proxy"),
				},
			},
		}),
	};
};

export const redactProxyUrl = (value: string): string => {
	const trimmed = value.trim();
	if (!trimmed) {
		return "";
	}
	try {
		const url = new URL(trimmed);
		if (!url.hostname) {
			return "<invalid proxy>";
		}
		url.username = "";
		url.password = "";
		return url.toString();
	} catch {
		const marker = trimmed.indexOf("://");
		if (marker < 0) {
			return "<invalid proxy>";
		}
		const scheme = trimmed.slice(0, marker);
		const remainder = trimmed.slice(marker + 3);
		const end = remainder.search(/[/?#]/);
		const authority = end < 0 ? remainder : remainder.slice(0, end);
		const host = authority.includes("@")
			? authority.slice(authority.lastIndexOf("@") + 1)
			: authority;
		return host ? `${scheme.toLowerCase()}://${host}` : "<invalid proxy>";
	}
};

export const redactProxyConfig = (value: unknown): NormalizedProxyConfig => {
	const normalized = normalizeProxyConfig(value);
	return {
		http: {
			...normalized.http,
			server: redactProxyUrl(normalized.http.server),
		},
		p2p: {
			...normalized.p2p,
			server: redactProxyUrl(normalized.p2p.server),
			udp: {
				...normalized.p2p.udp,
				server: redactProxyUrl(normalized.p2p.udp.server),
			},
		},
	};
};

export const redactProxySettings = (
	value: Record<string, unknown>,
): Record<string, unknown> => {
	const result = { ...value };
	if (Object.hasOwn(result, "proxy")) {
		result.proxy = redactProxyConfig(result.proxy);
	}
	for (const key of [
		"all-proxy",
		"p2p-proxy",
		"p2p-udp-proxy",
		"allProxy",
		"p2pProxy",
		"p2pUdpProxy",
	]) {
		if (typeof result[key] === "string") {
			result[key] = redactProxyUrl(result[key] as string);
		}
	}
	for (const key of [
		"pbhRpcSecret",
		"pbh-rpc-secret",
		"rpcSecret",
		"rpc-secret",
		"externalEngineSecret",
	]) {
		if (key in result && result[key] != null && result[key] !== "") {
			result[key] = "[REDACTED]";
		}
	}
	return result;
};
