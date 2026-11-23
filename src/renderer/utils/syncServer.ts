export const DEFAULT_SYNC_SERVER = "https://api.risuko.app";

export function normalizeSyncServer(url: string | undefined | null): string {
	const trimmed = `${url || ""}`.trim().replace(/\/+$/, "");
	return trimmed || DEFAULT_SYNC_SERVER;
}

export function sameSyncServer(
	a: string | undefined | null,
	b: string | undefined | null,
): boolean {
	return (
		normalizeSyncServer(a).toLowerCase() ===
		normalizeSyncServer(b).toLowerCase()
	);
}
