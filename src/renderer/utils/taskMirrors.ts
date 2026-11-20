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

export const buildMirrorRows = (
	mirrors: TaskMirror[] | undefined,
): MirrorRow[] => {
	const list = Array.isArray(mirrors) ? mirrors : [];
	if (list.length < 2) {
		return [];
	}
	const urls = list.map((m) => parseUrl(m.uri));
	const hosts = urls.map((url, i) => url?.host || list[i].uri);
	return list.map((mirror, i) => {
		const shared = hosts.filter((host) => host === hosts[i]).length > 1;
		const url = urls[i];
		return {
			key: `${i}:${mirror.uri}`,
			uri: mirror.uri,
			label: shared && url ? `${url.host}${url.pathname}` : hosts[i],
			completed: Number(mirror.completedLength) || 0,
			speed: Number(mirror.downloadSpeed) || 0,
			connections: Number(mirror.connections) || 0,
			status: mirror.status,
		};
	});
};
