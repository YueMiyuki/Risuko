export const DEFAULT_SYNC_SERVER = "https://api.risuko.app";

export function normalizeSyncServer(url: string | undefined | null): string {
	const trimmed = `${url || ""}`.trim().replace(/\/+$/, "");
	return trimmed || DEFAULT_SYNC_SERVER;
}

function parseSyncServer(url: string | undefined | null): URL | null {
	try {
		return new URL(normalizeSyncServer(url));
	} catch {
		return null;
	}
}

function serverIdentity(url: string | undefined | null): string {
	const parsed = parseSyncServer(url);
	if (!parsed) {
		return normalizeSyncServer(url);
	}
	const path = parsed.pathname.replace(/\/+$/, "");
	return `${parsed.protocol}//${parsed.host}${path}${parsed.search}`;
}

export function sameSyncServer(
	a: string | undefined | null,
	b: string | undefined | null,
): boolean {
	return serverIdentity(a) === serverIdentity(b);
}

export function sendsTokenUnencrypted(url: string | undefined | null): boolean {
	const parsed = parseSyncServer(url);
	if (parsed?.protocol !== "http:") {
		return false;
	}
	const host = parsed.hostname.replace(/^\[|\]$/g, "");
	const loopback =
		host === "localhost" ||
		host.endsWith(".localhost") ||
		host === "::1" ||
		/^127(\.\d{1,3}){3}$/.test(host);
	return !loopback;
}
