import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, relative } from "node:path";
import { test } from "node:test";
import { walkFiles } from "./walk-files.mjs";

test("files are listed depth-first in name order and skipped folders are left out", (t) => {
	const root = mkdtempSync(join(tmpdir(), "walk-files-"));
	t.after(() => rmSync(root, { recursive: true, force: true }));
	for (const dir of ["b", "a/deep", "node_modules/pkg"]) {
		mkdirSync(join(root, dir), { recursive: true });
	}
	for (const file of [
		"z.txt",
		"b/one.apk",
		"a/deep/two.apk",
		"a/first.txt",
		"node_modules/pkg/package.json",
	]) {
		writeFileSync(join(root, file), "");
	}

	const all = walkFiles(root).map((path) => relative(root, path));
	assert.deepEqual(all, [
		join("a", "deep", "two.apk"),
		join("a", "first.txt"),
		join("b", "one.apk"),
		join("node_modules", "pkg", "package.json"),
		"z.txt",
	]);
	const skipped = walkFiles(root, { skipDirs: ["node_modules"] });
	assert.equal(skipped.length, 4);
});
