export interface TorrentTrashTargets {
	folder: string | null;
	files: string[];
}

const normalize = (path: string): string =>
	path.replace(/[/\\]+/g, "/").replace(/\/+$/, "");

export function torrentTrashTargets(
	dir: string,
	folderName: string,
	filePaths: string[],
): TorrentTrashTargets {
	const base = normalize(dir);
	const files = [
		...new Set(
			filePaths
				.map((path) => `${path || ""}`.trim())
				.filter((path) => path && normalize(path) !== base),
		),
	];
	const name = folderName.trim();
	const folder = name ? `${base}/${name}` : "";
	const prefix = `${normalize(folder)}/`;
	const allInside =
		!!folder &&
		files.length > 0 &&
		files.every((path) => normalize(path).startsWith(prefix));
	return { folder: allInside ? folder : null, files };
}
