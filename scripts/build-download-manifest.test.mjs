import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { buildDownloadManifest } from "./build-download-manifest.mjs";

const dir = mkdtempSync(join(tmpdir(), "dl-manifest-"));

try {
	writeFileSync(join(dir, "Risuko_1.2.3_win32_x64.setup.exe"), "setup");
	writeFileSync(join(dir, "Risuko_1.2.3_win32_x64.setup.exe.sha256"), "x");
	writeFileSync(join(dir, "Risuko_1.2.3_win32_x64.setup.exe.sig"), "x");
	writeFileSync(join(dir, "Risuko_1.2.3_darwin_arm64.dmg"), "dmg");
	writeFileSync(join(dir, "latest.json"), "{}");
	writeFileSync(join(dir, "manifest.json"), "{}");
	mkdirSync(join(dir, "nested"));

	const m = buildDownloadManifest(dir, {
		tag: "v1.2.3",
		name: "Risuko v1.2.3",
		publishedAt: "2026-10-10T02:15:20Z",
	});

	assert.equal(m.version, "1.2.3");
	assert.equal(m.tag, "v1.2.3");
	assert.equal(m.name, "Risuko v1.2.3");
	assert.equal(m.pub_date, "2026-10-10T02:15:20Z");
	assert.deepEqual(
		m.assets.map((a) => a.name),
		["Risuko_1.2.3_darwin_arm64.dmg", "Risuko_1.2.3_win32_x64.setup.exe"],
	);
	assert.equal(m.assets[1].size, 5);
	assert.equal(m.assets[1].sha256, createHash("sha256").update("setup").digest("hex"));

	const fallback = buildDownloadManifest(dir, { tag: "v1.2.3-beta.1" });
	assert.equal(fallback.version, "1.2.3-beta.1");
	assert.equal(fallback.name, "v1.2.3-beta.1");
	assert.match(fallback.pub_date, /^\d{4}-\d{2}-\d{2}T/);

	assert.throws(() => buildDownloadManifest(dir, {}), /tag must look like/);
	assert.throws(() => buildDownloadManifest(dir, { tag: "1.2.3" }), /tag must look like/);

	const emptyDir = mkdtempSync(join(tmpdir(), "dl-manifest-empty-"));
	try {
		writeFileSync(join(emptyDir, "latest.json"), "{}");
		assert.throws(() => buildDownloadManifest(emptyDir, { tag: "v1.0.0" }), /no assets/);
	} finally {
		rmSync(emptyDir, { recursive: true, force: true });
	}

	console.log("ok");
} finally {
	rmSync(dir, { recursive: true, force: true });
}
