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

const isShared = (values: string[], i: number): boolean =>
	values.filter((value) => value === values[i]).length > 1;

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
	return list.map((mirror, i) => {
		const url = urls[i];
		let label = hosts[i];
		if (url && isShared(hosts, i)) {
			label = isShared(paths, i) ? `${paths[i]}${url.search}` : paths[i];
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
