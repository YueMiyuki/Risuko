export function mergeSyncConflict(
	local: Record<string, unknown>,
	remote: Record<string, unknown>,
	dirtyKeys: readonly string[] | null,
): Record<string, unknown> {
	const merged = { ...remote };
	for (const key of dirtyKeys ?? Object.keys(local)) {
		if (key in local) {
			merged[key] = local[key];
		}
	}
	return merged;
}

export function trackDirtyKey(
	dirty: Record<string, string[]>,
	categoryId: string,
	key: string,
): boolean {
	const known = dirty[categoryId];
	if (!known || known.includes(key)) {
		return false;
	}
	dirty[categoryId] = [...known, key];
	return true;
}

export interface SyncSide {
	hasData: boolean;
	timestamp?: number;
	rev?: number;
}

export type SyncStep = "skip" | "pull" | "push" | "merge";

export function planCategorySync(
	local: SyncSide,
	remote: SyncSide,
	dirtyKeys: readonly string[] | null,
): SyncStep {
	const hasRemote = remote.timestamp !== undefined;
	if (!local.hasData) {
		return hasRemote && remote.hasData ? "pull" : "skip";
	}
	if (!hasRemote) {
		return "push";
	}
	if (local.timestamp === undefined) {
		return remote.hasData ? "pull" : "skip";
	}
	const remoteTimestamp = remote.timestamp ?? 0;
	const remoteChanged =
		remote.rev === undefined
			? remoteTimestamp > local.timestamp
			: remote.rev !== (local.rev ?? 0);
	const localChanged = dirtyKeys
		? dirtyKeys.length > 0
		: local.timestamp > remoteTimestamp;
	if (localChanged) {
		return remoteChanged && remote.rev !== undefined ? "merge" : "push";
	}
	return remoteChanged && remote.hasData ? "pull" : "skip";
}

function stableJson(value: unknown): string {
	if (Array.isArray(value)) {
		return `[${value.map(stableJson).join(",")}]`;
	}
	if (value && typeof value === "object") {
		const entries = Object.entries(value as Record<string, unknown>)
			.filter(([, entry]) => entry !== undefined)
			.sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))
			.map(([key, entry]) => `${JSON.stringify(key)}:${stableJson(entry)}`);
		return `{${entries.join(",")}}`;
	}
	return JSON.stringify(value) ?? "null";
}

export function sameSyncData(a: unknown, b: unknown): boolean {
	return stableJson(a) === stableJson(b);
}

export const USENET_PROFILES_KEY = "usenet-profiles";
const PROFILE_KEY_PREFIX = `${USENET_PROFILES_KEY}#`;
export const PUSH_DELAY_MS = 500;
const PUSH_RETRY_BASE_MS = 30_000;
const PUSH_RETRY_MAX_MS = 10 * 60_000;

export function profileDirtyKey(id: string): string {
	return `${PROFILE_KEY_PREFIX}${id}`;
}

function profilesById(value: unknown): Map<string, Record<string, unknown>> {
	const byId = new Map<string, Record<string, unknown>>();
	if (!Array.isArray(value)) {
		return byId;
	}
	for (const entry of value) {
		if (!entry || typeof entry !== "object" || Array.isArray(entry)) {
			continue;
		}
		const profile = entry as Record<string, unknown>;
		if (typeof profile.id !== "string" || !profile.id || byId.has(profile.id)) {
			continue;
		}
		byId.set(profile.id, profile);
	}
	return byId;
}

function profileUpdatedAt(profile: Record<string, unknown>): number {
	return typeof profile.updatedAt === "number" &&
		Number.isFinite(profile.updatedAt)
		? profile.updatedAt
		: 0;
}

export function changedProfileIds(previous: unknown, next: unknown): string[] {
	const before = profilesById(previous);
	return [...profilesById(next)]
		.filter(([id, profile]) => !sameSyncData(before.get(id), profile))
		.map(([id]) => id);
}

export function dirtyProfileIds(
	dirtyKeys: readonly string[] | null,
): Set<string> | null {
	if (!dirtyKeys) {
		return null;
	}
	const ids = dirtyKeys
		.filter((key) => key.startsWith(PROFILE_KEY_PREFIX))
		.map((key) => key.slice(PROFILE_KEY_PREFIX.length));
	if (ids.length === 0 && dirtyKeys.includes(USENET_PROFILES_KEY)) {
		return null;
	}
	return new Set(ids);
}

export function mergeProfiles(
	local: unknown,
	remote: unknown,
	dirtyIds: ReadonlySet<string> | null,
): unknown[] {
	const merged = profilesById(local);
	for (const [id, remoteProfile] of profilesById(remote)) {
		const localProfile = merged.get(id);
		if (
			localProfile &&
			(dirtyIds
				? dirtyIds.has(id)
				: profileUpdatedAt(localProfile) >= profileUpdatedAt(remoteProfile))
		) {
			continue;
		}
		merged.set(id, remoteProfile);
	}
	return [...merged.values()].map((profile) => ({ ...profile }));
}

export function conflictKeys(
	categoryId: string,
	local: Record<string, unknown>,
	dirtyKeys: readonly string[] | null,
): string[] {
	const keys = [...(dirtyKeys ?? Object.keys(local))];
	if (categoryId !== "usenet") {
		return keys;
	}
	return keys.filter(
		(key) => key !== USENET_PROFILES_KEY && !key.startsWith(PROFILE_KEY_PREFIX),
	);
}

export function remainingDirtyKeys(
	current: readonly string[],
	mark: {
		pushedKeys?: readonly string[] | null;
		pulledKeys?: readonly string[] | null;
	},
): string[] {
	const { pushedKeys, pulledKeys } = mark;
	if (pushedKeys) {
		return current.filter((key) => !pushedKeys.includes(key));
	}
	if (!pulledKeys) {
		return [];
	}
	const keepProfiles = current.some((key) =>
		key.startsWith(PROFILE_KEY_PREFIX),
	);
	return current.filter(
		(key) =>
			(keepProfiles &&
				(key === USENET_PROFILES_KEY || key.startsWith(PROFILE_KEY_PREFIX))) ||
			!pulledKeys.includes(key),
	);
}

export function pushRetryDelayMs(attempt: number): number {
	const exponent = Math.max(1, attempt) - 1;
	return Math.min(PUSH_RETRY_BASE_MS * 2 ** exponent, PUSH_RETRY_MAX_MS);
}

export function isAuthFailure(status: unknown): boolean {
	return status === 401 || status === 403;
}

export function nextPushDelayMs(
	failed: readonly string[],
	queued: readonly string[],
	attempt: number,
): number {
	return queued.some((categoryId) => !failed.includes(categoryId))
		? PUSH_DELAY_MS
		: pushRetryDelayMs(attempt);
}
