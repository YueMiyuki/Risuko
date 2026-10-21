import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import test from "node:test";

import { addRelease, METAINFO, releaseNotes } from "./add-metainfo-release.mjs";

const xml = `<component>
  <releases>
    <release version="1.0.1" date="2026-10-20" />
  </releases>
</component>
`;

test("releaseNotes takes the PR titles from GitHub's generated notes", () => {
	const body = [
		'<img alt="banner" src="https://example.com/a.png" />',
		"",
		"## What's Changed",
		"* fix: notification crash on linux by @YueMiyuki in https://github.com/YueMiyuki/Risuko/pull/161",
		"* build: split crates by @YueMiyuki in https://github.com/YueMiyuki/Risuko/pull/163",
		"",
		"## New Contributors",
		"* @meme-code made their first contribution in https://github.com/YueMiyuki/Risuko/pull/166",
		"",
		"**Full Changelog**: https://github.com/YueMiyuki/Risuko/compare/v1.0.0...v1.0.1",
	].join("\n");
	assert.deepEqual(releaseNotes(body), ["fix: notification crash on linux", "build: split crates"]);
});

test("releaseNotes uses plain text as a single summary", () => {
	assert.deepEqual(releaseNotes("Fixes a crash when showing notifications\n"), [
		"Fixes a crash when showing notifications",
	]);
	assert.deepEqual(releaseNotes("**Full Changelog**: https://github.com/a/b/compare/v1...v2\n"), []);
	assert.deepEqual(releaseNotes(""), []);
});

test("addRelease puts the newest release first", () => {
	const next = addRelease(xml, "v1.0.2", "2026-11-01", ["Fixes a crash"]);
	assert.match(
		next,
		/<releases>\n {4}<release version="1\.0\.2" date="2026-11-01">\n {6}<description>\n {8}<p>Fixes a crash<\/p>\n {6}<\/description>\n {4}<\/release>\n {4}<release version="1\.0\.1"/,
	);
});

test("addRelease lists several notes and escapes XML", () => {
	const next = addRelease(xml, "1.0.2", "2026-11-01", ["a < b", "c & d"]);
	assert.match(next, /<ul>\n {10}<li>a &lt; b<\/li>\n {10}<li>c &amp; d<\/li>\n {8}<\/ul>/);
});

test("addRelease writes a bare entry without notes and is idempotent", () => {
	const next = addRelease(xml, "1.0.2", "2026-11-01");
	assert.match(next, /<releases>\n {4}<release version="1\.0\.2" date="2026-11-01" \/>\n/);
	assert.equal(addRelease(next, "1.0.2", "2026-12-01", ["later"]), next);
});

test("addRelease rejects bad input", () => {
	assert.throws(() => addRelease(xml, "next", "2026-11-01"), /Invalid version/);
	assert.throws(() => addRelease(xml, "1.0.2", "Nov 1"), /Invalid date/);
	assert.throws(() => addRelease("<component />", "1.0.2", "2026-11-01"), /No <releases>/);
});

test("the real metainfo accepts a new release", () => {
	const real = readFileSync(resolve(import.meta.dirname, "..", METAINFO), "utf8");
	const next = addRelease(real, "99.0.0", "2099-01-01", ["Test"]);
	assert.ok(next.indexOf('version="99.0.0"') < next.indexOf('version="1.0.1"'));
});
