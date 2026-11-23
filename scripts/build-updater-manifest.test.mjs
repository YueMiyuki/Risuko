import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { buildManifest } from "./build-updater-manifest.mjs";

const previousArgv1 = process.argv[1];
try {
	process.argv[1] = undefined;
	await import(`./build-updater-manifest.mjs?argv-missing=${Date.now()}`);
} finally {
	process.argv[1] = previousArgv1;
}

const dir = mkdtempSync(join(tmpdir(), "frag-"));

try {
	writeFileSync(
		join(dir, "darwin-aarch64.json"),
		JSON.stringify({
			key: "darwin-aarch64",
			asset: "Risuko_1.2.3_darwin_arm64.app.tar.gz",
			signature: "SIG_A",
		}),
	);
	writeFileSync(
		join(dir, "windows-x86_64.json"),
		JSON.stringify({
			key: "windows-x86_64",
			asset: "Risuko_1.2.3_win32_x64.setup.exe",
			signature: "SIG_W",
		}),
	);

	const m = buildManifest(dir, {
		version: "v1.2.3",
		repo: "YueMiyuki/Risuko",
	});

	assert.equal(m.version, "1.2.3");
	assert.match(m.pub_date, /^\d{4}-\d{2}-\d{2}T/);
	assert.equal(m.platforms["darwin-aarch64"].signature, "SIG_A");
	assert.equal(m.platforms["windows-x86_64"].signature, "SIG_W");
	assert.equal(
		m.platforms["darwin-aarch64"].url,
		"https://github.com/YueMiyuki/Risuko/releases/download/v1.2.3/Risuko_1.2.3_darwin_arm64.app.tar.gz",
	);
	assert.equal(
		m.platforms["windows-x86_64"].url,
		"https://github.com/YueMiyuki/Risuko/releases/download/v1.2.3/Risuko_1.2.3_win32_x64.setup.exe",
	);
	assert.throws(() => buildManifest(dir, { repo: "x/y" }), /version required/);
	assert.throws(() => buildManifest(dir, { version: "1.0.0" }), /repo .* required/);

	const emptyDir = mkdtempSync(join(tmpdir(), "frag-empty-"));
	try {
		assert.throws(() => buildManifest(emptyDir, { version: "1.0.0", repo: "x/y" }), /no fragments/);
	} finally {
		rmSync(emptyDir, { recursive: true, force: true });
	}

	const badDir = mkdtempSync(join(tmpdir(), "frag-bad-"));
	try {
		writeFileSync(join(badDir, "bad.json"), JSON.stringify({ key: "linux-x86_64", asset: "a" }));
		assert.throws(
			() => buildManifest(badDir, { version: "1.0.0", repo: "x/y" }),
			/incomplete fragment/,
		);
	} finally {
		rmSync(badDir, { recursive: true, force: true });
	}

	const dupDir = mkdtempSync(join(tmpdir(), "frag-dup-"));
	try {
		const frag = JSON.stringify({
			key: "linux-x86_64",
			asset: "a.AppImage.tar.gz",
			signature: "SIG",
		});
		writeFileSync(join(dupDir, "a.json"), frag);
		writeFileSync(join(dupDir, "b.json"), frag);
		assert.throws(
			() => buildManifest(dupDir, { version: "1.0.0", repo: "x/y" }),
			/duplicate fragment key/,
		);
		} finally {
			rmSync(dupDir, { recursive: true, force: true });
		}

		const weirdDir = mkdtempSync(join(tmpdir(), "frag-weird-"));
		try {
			writeFileSync(
				join(weirdDir, "b.json"),
				JSON.stringify({ key: "b", asset: "b.tar.gz", signature: "SIG_B" }),
			);
			writeFileSync(
				join(weirdDir, "a.json"),
				JSON.stringify({ key: "a", asset: "a.tar.gz", signature: "SIG_A" }),
			);
			writeFileSync(
				join(weirdDir, "proto.json"),
				JSON.stringify({
					key: "__proto__",
					asset: "proto.tar.gz",
					signature: "SIG_P",
				}),
			);
			const weird = buildManifest(weirdDir, {
				version: "1.0.0",
				repo: "x/y",
			});
			assert.equal(Object.getPrototypeOf(weird.platforms), null);
			assert.deepEqual(Object.keys(weird.platforms), ["a", "b", "__proto__"]);
			assert.equal(weird.platforms.__proto__.signature, "SIG_P");
		} finally {
			rmSync(weirdDir, { recursive: true, force: true });
		}

		console.log("ok");
	} finally {
	rmSync(dir, { recursive: true, force: true });
}
