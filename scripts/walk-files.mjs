import { readdirSync } from "node:fs";
import { join } from "node:path";

export function walkFiles(root, { skipDirs = [] } = {}) {
	const files = [];
	const entries = readdirSync(root, { withFileTypes: true }).sort((a, b) =>
		a.name.localeCompare(b.name),
	);
	for (const entry of entries) {
		const path = join(root, entry.name);
		if (entry.isDirectory()) {
			if (!skipDirs.includes(entry.name)) {
				files.push(...walkFiles(path, { skipDirs }));
			}
		} else if (entry.isFile()) {
			files.push(path);
		}
	}
	return files;
}
