import { sameSyncData } from "./syncConflict.ts";

export const SECRET_SYNC_KEYS: readonly string[] = [
	"rpc-secret",
	"pbh-rpc-secret",
	"external-engine-secret",
	"saved-credentials",
	"ftp-passwd",
	"sftp-passwd",
	"sftp-private-key",
	"sftp-private-key-passphrase",
	"cookie",
	"header",
];

export const RETIRED_SYNC_CATEGORIES: readonly string[] = ["credentials"];

const SECRET_OPTION_PARTS = [
	"passwd",
	"password",
	"passphrase",
	"secret",
	"token",
	"cookie",
	"private-key",
	"authorization",
];

const URL_SCHEME = /^[a-z][a-z0-9+.-]*:\/\//i;

const ENGINE_OVERRIDES = "engine-overrides";

const TRACKER_LIST = "bt-tracker";

const PASSKEY_SEGMENT = /^[A-Za-z0-9_-]{20,}$/;

const PROXY_SERVER_PATHS: readonly (readonly string[])[] = [
	["all-proxy"],
	["p2p-proxy"],
	["p2p-udp-proxy"],
	["proxy", "server"],
	["proxy", "http", "server"],
	["proxy", "p2p", "server"],
	["proxy", "p2p", "udp", "server"],
];

type SyncData = Record<string, unknown>;

type Strip = (value: string) => string;

type Rewrite = (value: string, local: unknown, strip: Strip) => string;

const isRecord = (value: unknown): value is SyncData =>
	!!value && typeof value === "object" && !Array.isArray(value);

export function stripProxyCredentials(server: string): string {
	const trimmed = server.trim();
	const scheme = URL_SCHEME.exec(trimmed)?.[0] ?? "";
	const rest = trimmed.slice(scheme.length);
	const at = rest.lastIndexOf("@");
	return at === -1 ? server : scheme + rest.slice(at + 1);
}

export function stripUrlCredentials(url: string): string {
	const scheme = URL_SCHEME.exec(url)?.[0] ?? "";
	const rest = url.slice(scheme.length);
	const end = rest.search(/[/?#]/);
	const authority = end === -1 ? rest : rest.slice(0, end);
	const at = authority.lastIndexOf("@");
	return at === -1 ? url : scheme + rest.slice(at + 1);
}

export function isSecretOption(key: string): boolean {
	const lower = key.toLowerCase();
	return (
		lower === "header" ||
		SECRET_OPTION_PARTS.some((part) => lower.includes(part))
	);
}

export function isPrivateTracker(url: string): boolean {
	const trimmed = url.trim();
	const scheme = URL_SCHEME.exec(trimmed)?.[0];
	if (!scheme) {
		return true;
	}
	const rest = trimmed.slice(scheme.length);
	const end = rest.search(/[/?#]/);
	const authority = end === -1 ? rest : rest.slice(0, end);
	if (authority === "" || authority.includes("@")) {
		return true;
	}
	const tail = end === -1 ? "" : rest.slice(end);
	if (tail.includes("?")) {
		return true;
	}
	const path = tail.split("#", 1)[0];
	return path.split("/").some((segment) => PASSKEY_SEGMENT.test(segment));
}

function trackerEntries(value: unknown): string[] {
	return typeof value === "string"
		? value
				.split(/[,\r\n]+/)
				.map((entry) => entry.trim())
				.filter((entry) => entry !== "")
		: [];
}

const stripOnly: Rewrite = (value, _local, strip) => strip(value);

const preferLocal: Rewrite = (value, local, strip) =>
	typeof local === "string" && strip(local) === value ? local : value;

function overrideStrip(key: string, value: string): Strip | undefined {
	if (key.toLowerCase().includes("proxy")) {
		return stripProxyCredentials;
	}
	return URL_SCHEME.test(value) ? stripUrlCredentials : undefined;
}

function rewriteAt(
	value: unknown,
	local: unknown,
	path: readonly string[],
	rewrite: Rewrite,
): unknown {
	if (path.length === 0) {
		return typeof value === "string"
			? rewrite(value, local, stripProxyCredentials)
			: value;
	}
	const [key, ...rest] = path;
	if (!isRecord(value) || !Object.hasOwn(value, key)) {
		return value;
	}
	return {
		...value,
		[key]: rewriteAt(
			value[key],
			isRecord(local) && Object.hasOwn(local, key) ? local[key] : undefined,
			rest,
			rewrite,
		),
	};
}

function rewriteProxyServers(
	data: SyncData,
	local: SyncData,
	rewrite: Rewrite,
): SyncData {
	return PROXY_SERVER_PATHS.reduce<SyncData>(
		(result, path) => rewriteAt(result, local, path, rewrite) as SyncData,
		data,
	);
}

function rewriteOverrides(
	overrides: SyncData,
	local: SyncData,
	rewrite: Rewrite,
): SyncData {
	return Object.fromEntries(
		Object.entries(overrides)
			.filter(([key]) => !isSecretOption(key))
			.map(([key, value]) => {
				if (typeof value !== "string") {
					return [key, value];
				}
				const strip = overrideStrip(key, value);
				const localValue = Object.hasOwn(local, key) ? local[key] : undefined;
				return [key, strip ? rewrite(value, localValue, strip) : value];
			}),
	);
}

export function withoutSyncSecrets(
	categoryId: string,
	data: SyncData,
): SyncData {
	if (categoryId === "network") {
		return rewriteProxyServers(data, {}, stripOnly);
	}
	if (categoryId === "engine" && isRecord(data[ENGINE_OVERRIDES])) {
		return {
			...data,
			[ENGINE_OVERRIDES]: rewriteOverrides(
				data[ENGINE_OVERRIDES],
				{},
				stripOnly,
			),
		};
	}
	if (categoryId === "tracker" && typeof data[TRACKER_LIST] === "string") {
		const entries = trackerEntries(data[TRACKER_LIST]);
		const publicEntries = entries.filter((entry) => !isPrivateTracker(entry));
		if (publicEntries.length !== entries.length) {
			return { ...data, [TRACKER_LIST]: publicEntries.join(",") };
		}
	}
	return data;
}

export function keepLocalSyncSecrets(
	categoryId: string,
	local: SyncData,
	incoming: SyncData,
): SyncData {
	const cleaned = withoutSyncSecrets(categoryId, incoming);
	if (categoryId === "tracker" && typeof cleaned[TRACKER_LIST] === "string") {
		const kept = trackerEntries(cleaned[TRACKER_LIST]);
		const localPrivate = trackerEntries(local[TRACKER_LIST]).filter(
			isPrivateTracker,
		);
		const added = [...new Set(localPrivate)].filter(
			(entry) => !kept.includes(entry),
		);
		return added.length === 0
			? cleaned
			: { ...cleaned, [TRACKER_LIST]: [...kept, ...added].join(",") };
	}
	if (categoryId === "network") {
		return rewriteProxyServers(cleaned, local, preferLocal);
	}
	const overrides = cleaned[ENGINE_OVERRIDES];
	if (categoryId === "engine" && isRecord(overrides)) {
		const localOverrides = isRecord(local[ENGINE_OVERRIDES])
			? local[ENGINE_OVERRIDES]
			: {};
		const localSecrets = Object.entries(localOverrides).filter(([key]) =>
			isSecretOption(key),
		);
		return {
			...cleaned,
			[ENGINE_OVERRIDES]: {
				...rewriteOverrides(overrides, localOverrides, preferLocal),
				...Object.fromEntries(localSecrets),
			},
		};
	}
	return cleaned;
}

export function remoteNeedsScrub(
	categoryId: string,
	declared: SyncData,
	raw: SyncData,
): boolean {
	return (
		SECRET_SYNC_KEYS.some((key) => Object.hasOwn(raw, key)) ||
		!sameSyncData(withoutSyncSecrets(categoryId, declared), declared)
	);
}
