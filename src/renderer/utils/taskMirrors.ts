import type { TaskMirror } from "@shared/types/task";

export interface MirrorRow {
	key: string;
	uri: string;
	label: string;
	completed: number;
	speed: number;
	connections: number;
	status: TaskMirror["status"];
}

const parseUrl = (uri: string): URL | null => {
	try {
		return new URL(uri);
	} catch {
		return null;
	}
};

const countValues = (values: string[]): Map<string, number> => {
	const counts = new Map<string, number>();
	for (const value of values) {
		counts.set(value, (counts.get(value) ?? 0) + 1);
	}
	return counts;
};

export const buildMirrorRows = (
	mirrors: TaskMirror[] | undefined,
): MirrorRow[] => {
	const list = Array.isArray(mirrors) ? mirrors : [];
	if (list.length < 2) {
		return [];
	}
	const urls = list.map((m) => parseUrl(m.uri));
	const hosts = urls.map((url, i) => url?.host || list[i].uri);
	const paths = urls.map((url, i) =>
		url ? `${url.host}${url.pathname}` : list[i].uri,
	);
	const hostCounts = countValues(hosts);
	const pathCounts = countValues(paths);
	return list.map((mirror, i) => {
		const url = urls[i];
		let label = hosts[i];
		if (url && (hostCounts.get(hosts[i]) ?? 0) > 1) {
			label =
				(pathCounts.get(paths[i]) ?? 0) > 1
					? `${paths[i]}${url.search}`
					: paths[i];
		}
		return {
			key: `${i}:${mirror.uri}`,
			uri: mirror.uri,
			label,
			completed: Number(mirror.completedLength) || 0,
			speed: Number(mirror.downloadSpeed) || 0,
			connections: Number(mirror.connections) || 0,
			status: mirror.status,
		};
	});
};
