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
