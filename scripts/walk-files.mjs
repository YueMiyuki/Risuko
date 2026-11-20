import { readdirSync } from "node:fs";
import { join } from "node:path";

export function* iterFiles(root, { skipDirs = [] } = {}) {
	const entries = readdirSync(root, { withFileTypes: true }).sort((a, b) =>
		a.name.localeCompare(b.name),
	);
	for (const entry of entries) {
		const path = join(root, entry.name);
		if (entry.isDirectory()) {
			if (!skipDirs.includes(entry.name)) {
				yield* iterFiles(path, { skipDirs });
			}
		} else if (entry.isFile()) {
			yield path;
		}
	}
}

export function walkFiles(root, options) {
	return [...iterFiles(root, options)];
}
