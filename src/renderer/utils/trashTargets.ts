export interface TorrentTrashTargets {
	folder: string | null;
	files: string[];
}

const normalize = (path: string): string =>
	path.replace(/[/\\]+/g, "/").replace(/\/+$/, "");

const isChildName = (name: string): boolean =>
	!!name.trim() && name !== "." && name !== ".." && !/[/\\]/.test(name);

const climbs = (path: string): boolean =>
	normalize(path)
		.split("/")
		.some((segment) => segment === "..");

export function torrentTrashTargets(
	dir: string,
	folderName: string,
	filePaths: string[],
): TorrentTrashTargets {
	const base = normalize(dir);
	const files = [
		...new Set(
			filePaths
				.map((path) => `${path || ""}`)
				.filter((path) => path.trim() && normalize(path) !== base),
		),
	];
	const folder = isChildName(folderName) ? `${base}/${folderName}` : "";
	const prefix = `${folder}/`;
	const allInside =
		!!folder &&
		files.length > 0 &&
		files.every((path) => normalize(path).startsWith(prefix) && !climbs(path));
	return { folder: allInside ? folder : null, files };
}
