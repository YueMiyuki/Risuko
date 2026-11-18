import { MAX_BT_TRACKER_LENGTH, ONE_SECOND } from "@shared/constants";
import { normalizeProxyConfig, type ProxyConfig } from "@shared/types/config";

const TRACKER_SOURCE_CACHE_TTL = 10 * 60 * ONE_SECOND;
const trackerSourceCache = new Map<
	string,
	{ expiresAt: number; value: string[] }
>();
const trackerSourceInFlight = new Map<string, Promise<string[]>>();

const normalizeSource = (source: string[] = []) => {
	return [
		...new Set(source.map((item) => String(item).trim()).filter(Boolean)),
	].sort();
};

const buildTrackerSourceCacheKey = (source = [], proxyServer = "") => {
	return JSON.stringify({
		proxyServer,
		source: normalizeSource(source),
	});
};

const getCachedTrackerSource = (key: string, now: number) => {
	const cached = trackerSourceCache.get(key);
	if (!cached) {
		return;
	}

	if (cached.expiresAt > now) {
		return cached.value;
	}

	trackerSourceCache.delete(key);
};

export const fetchBtTrackerFromSource = async (
	source: string[],
	proxyConfig: ProxyConfig,
	fetchSource: (urls: string[]) => Promise<string[]>,
) => {
	if (!source?.length) {
		return [];
	}

	const now = Date.now();
	const {
		enable,
		server,
		bypass,
		scope = [],
	} = normalizeProxyConfig(proxyConfig).http;
	const cacheKey = buildTrackerSourceCacheKey(
		source,
		`${enable ? "1" : "0"}|${server || ""}|${bypass || ""}|${scope.join(",")}`,
	);
	const cached = getCachedTrackerSource(cacheKey, now);
	if (cached) {
		return cached;
	}

	const inFlight = trackerSourceInFlight.get(cacheKey);
	if (inFlight) {
		return inFlight;
	}

	const requestPromise = (async () => {
		const values = await fetchSource(
			source.map((url) => appendCacheBust(url, now)),
		);
		const result = [...new Set(values)];
		if (result.length > 0) {
			trackerSourceCache.set(cacheKey, {
				value: result,
				expiresAt: Date.now() + TRACKER_SOURCE_CACHE_TTL,
			});
		}
		return result;
	})().finally(() => {
		trackerSourceInFlight.delete(cacheKey);
	});

	trackerSourceInFlight.set(cacheKey, requestPromise);
	return requestPromise;
};

function appendCacheBust(source: string, now: number): string {
	try {
		const url = new URL(source);
		url.searchParams.set("t", `${now}`);
		return url.toString();
	} catch {
		const separator = source.includes("?") ? "&" : "?";
		return `${source}${separator}t=${now}`;
	}
}

export const convertTrackerDataToLine = (arr = []) => {
	return arr
		.join("\r\n")
		.replace(/^\s*[\r\n]/gm, "")
		.trim();
};

export const reduceTrackerString = (str = "") => {
	if (str.length <= MAX_BT_TRACKER_LENGTH) {
		return str;
	}

	const subStr = str.substring(0, MAX_BT_TRACKER_LENGTH);
	const index = subStr.lastIndexOf(",");
	if (index === -1) {
		return subStr;
	}

	const result = subStr.substring(0, index);
	return result;
};
