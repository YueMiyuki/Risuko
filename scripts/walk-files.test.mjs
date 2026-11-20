import assert from "node:assert/strict";
import {
	mkdirSync,
	mkdtempSync,
	rmSync,
	symlinkSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join, relative } from "node:path";
import { test } from "node:test";
import { iterFiles, walkFiles } from "./walk-files.mjs";

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

test("iterFiles yields the files lazily in the same order as walkFiles", (t) => {
	const root = mkdtempSync(join(tmpdir(), "walk-files-"));
	t.after(() => rmSync(root, { recursive: true, force: true }));
	mkdirSync(join(root, "a", "deep"), { recursive: true });
	for (const file of ["z.txt", "a/deep/two.apk", "a/first.txt"]) {
		writeFileSync(join(root, file), "");
	}

	const files = iterFiles(root);
	const first = files.next();
	assert.equal(first.done, false);
	assert.equal(relative(root, first.value), join("a", "deep", "two.apk"));
	assert.deepEqual([first.value, ...files], walkFiles(root));
});

test("symlinked files and folders are not followed", (t) => {
	const root = mkdtempSync(join(tmpdir(), "walk-files-"));
	t.after(() => rmSync(root, { recursive: true, force: true }));
	mkdirSync(join(root, "real"));
	writeFileSync(join(root, "real", "package.json"), "");
	writeFileSync(join(root, "target.txt"), "");
	try {
		symlinkSync(join(root, "real"), join(root, "linked-dir"), "dir");
		symlinkSync(join(root, "target.txt"), join(root, "linked.txt"));
		symlinkSync(root, join(root, "real", "loop"), "dir");
	} catch {
		t.skip("symlinks cannot be created here");
		return;
	}

	assert.deepEqual(
		walkFiles(root).map((path) => relative(root, path)),
		[join("real", "package.json"), "target.txt"],
	);
});
