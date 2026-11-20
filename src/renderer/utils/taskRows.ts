export const TASK_LIST_KEYS = [
	"gid",
	"status",
	"kind",
	"totalLength",
	"completedLength",
	"downloadSpeed",
	"uploadSpeed",
	"uploadLength",
	"connections",
	"dir",
	"tag",
	"files",
	"errorCode",
	"errorMessage",
	"createdAt",
	"startAt",
	"scheduleMissed",
	"infoHash",
	"infoHashV2",
	"metaVersion",
	"bittorrent",
	"seeder",
	"numSeeders",
	"numPeers",
	"pieceLength",
	"numPieces",
	"ed2kLink",
	"m3u8Link",
	"usenetStage",
	"usenetWarning",
	"usenetRepairFailure",
];

export const TASK_LIST_LIGHT_KEYS = TASK_LIST_KEYS.filter(
	(key) => key !== "files",
);
export const LIGHT_REFRESH_FULL_EVERY = 20;
export const LIGHT_REFRESH_MAX_MISSING = 20;

const LIGHT_REFRESH_LISTS = new Set([
	"waiting",
	"scheduled",
	"stopped",
	"completed",
]);

export function canRefreshLight({
	type,
	lastType,
	lightRefreshes,
}: {
	type: string;
	lastType: string | null;
	lightRefreshes: number;
}): boolean {
	return (
		LIGHT_REFRESH_LISTS.has(type) &&
		type === lastType &&
		lightRefreshes < LIGHT_REFRESH_FULL_EVERY
	);
}

export function mergeRowFiles<
	T extends { gid: string; status: string; files?: unknown },
>(
	previous: readonly T[],
	light: readonly T[],
): { rows: T[]; missing: string[] } {
	const held = new Map<string, T>();
	for (const row of previous) {
		if (Array.isArray(row.files)) {
			held.set(row.gid, row);
		}
	}
	const missing: string[] = [];
	const rows = light.map((row) => {
		const old = held.get(row.gid);
		if (old && old.status === row.status) {
			return { ...row, files: old.files };
		}
		missing.push(row.gid);
		return row;
	});
	return { rows, missing };
}

export function fillRowFiles<T extends { gid: string; files?: unknown }>(
	rows: readonly T[],
	items: readonly ({ gid: string; files?: unknown } | null | undefined)[],
): T[] | null {
	const fetched = new Map<string, unknown>();
	for (const item of items) {
		if (item && Array.isArray(item.files)) {
			fetched.set(item.gid, item.files);
		}
	}
	const filled: T[] = [];
	for (const row of rows) {
		const files = Array.isArray(row.files) ? row.files : fetched.get(row.gid);
		if (!Array.isArray(files)) {
			return null;
		}
		filled.push(files === row.files ? row : { ...row, files });
	}
	return filled;
}

export function deepEqual(a: unknown, b: unknown): boolean {
	if (a === b) {
		return true;
	}
	if (
		typeof a !== "object" ||
		typeof b !== "object" ||
		a === null ||
		b === null
	) {
		return false;
	}
	if (Array.isArray(a) !== Array.isArray(b)) {
		return false;
	}
	const left = a as Record<string, unknown>;
	const right = b as Record<string, unknown>;
	const leftKeys = Object.keys(left);
	if (leftKeys.length !== Object.keys(right).length) {
		return false;
	}
	for (const key of leftKeys) {
		if (!Object.hasOwn(right, key) || !deepEqual(left[key], right[key])) {
			return false;
		}
	}
	return true;
}

export function reuseUnchangedRows<T extends { gid: string }>(
	previous: readonly T[],
	next: readonly T[],
): T[] {
	if (previous.length === 0) {
		return next as T[];
	}
	const byGid = new Map<string, T>();
	for (const row of previous) {
		byGid.set(row.gid, row);
	}
	return next.map((row) => {
		const old = byGid.get(row.gid);
		return old && deepEqual(old, row) ? old : row;
	});
}
