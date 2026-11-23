import assert from "node:assert/strict";
import { test } from "node:test";
import { torrentTrashTargets } from "./trashTargets.ts";

test("files inside the torrent folder, nested ones too, trash the folder", () => {
	const targets = torrentTrashTargets("/dl", "Show", [
		"/dl/Show/a.mkv",
		"/dl/Show/Extras/b.mkv",
	]);
	assert.equal(targets.folder, "/dl/Show");
	assert.deepEqual(targets.files, ["/dl/Show/a.mkv", "/dl/Show/Extras/b.mkv"]);
});

test("files saved directly in the download folder are trashed one by one", () => {
	const targets = torrentTrashTargets("/dl", "Show", [
		"/dl/a.mkv",
		"/dl/Extras/b.mkv",
	]);
	assert.equal(targets.folder, null);
	assert.deepEqual(targets.files, ["/dl/a.mkv", "/dl/Extras/b.mkv"]);
});

test("files inside a folder the engine renamed do not trash the named folder", () => {
	const targets = torrentTrashTargets("/dl", "Show:Part", [
		"/dl/Show_Part/a.mkv",
		"/dl/Show_Part/b.mkv",
	]);
	assert.equal(targets.folder, null);
	assert.deepEqual(targets.files, [
		"/dl/Show_Part/a.mkv",
		"/dl/Show_Part/b.mkv",
	]);
});

test("a sibling folder that only shares the name as a prefix does not count", () => {
	const targets = torrentTrashTargets("/dl", "Show", ["/dl/Show 2/a.mkv"]);
	assert.equal(targets.folder, null);
	assert.deepEqual(targets.files, ["/dl/Show 2/a.mkv"]);
});

test("one file outside the torrent folder means no folder is trashed", () => {
	const targets = torrentTrashTargets("/dl", "Show", [
		"/dl/Show/a.mkv",
		"/dl/b.mkv",
	]);
	assert.equal(targets.folder, null);
	assert.deepEqual(targets.files, ["/dl/Show/a.mkv", "/dl/b.mkv"]);
});

test("an empty folder name never gives a folder to trash", () => {
	assert.equal(torrentTrashTargets("/dl", "", ["/dl/a.mkv"]).folder, null);
	assert.equal(torrentTrashTargets("/dl", "  ", ["/dl/a.mkv"]).folder, null);
});

test("empty paths and the download folder itself are dropped and duplicates listed once", () => {
	const targets = torrentTrashTargets("/dl/", "Show", [
		"",
		"  ",
		"/dl",
		"/dl/",
		"/dl/Show/a.mkv",
		"/dl/Show/a.mkv",
	]);
	assert.deepEqual(targets.files, ["/dl/Show/a.mkv"]);
	assert.equal(targets.folder, "/dl/Show");
});

test("windows separators are understood", () => {
	const targets = torrentTrashTargets("C:\\dl", "Name", [
		"C:\\dl\\Name\\a.txt",
		"C:\\dl\\Name\\sub\\b.txt",
	]);
	assert.equal(targets.folder, "C:/dl/Name");
	assert.deepEqual(targets.files, [
		"C:\\dl\\Name\\a.txt",
		"C:\\dl\\Name\\sub\\b.txt",
	]);
});

test("no files at all give no folder to trash", () => {
	assert.deepEqual(torrentTrashTargets("/dl", "Show", []), {
		folder: null,
		files: [],
	});
});
