import assert from "node:assert/strict";
import { cpSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import test from "node:test";

import { normalizeVersion, setVersion } from "./set-version.mjs";

const repoRoot = resolve(import.meta.dirname, "..");

function write(root, path, content) {
	const full = join(root, path);
	mkdirSync(dirname(full), { recursive: true });
	writeFileSync(full, content);
}

function fixture() {
	const root = mkdtempSync(join(tmpdir(), "risuko-set-version-"));
	write(root, "package.json", '{\n\t"name": "risuko",\n\t"version": "0.1.0"\n}\n');
	write(
		root,
		"packages/risuko-cli/package.json",
		JSON.stringify(
			{
				name: "@risuko/cli",
				version: "0.1.0",
				optionalDependencies: { "@risuko/cli-darwin-arm64": "0.1.0", other: "^1.0.0" },
			},
			null,
			2,
		),
	);
	write(
		root,
		"packages/risuko-cli/npm/darwin-arm64/package.json",
		'{\n  "name": "@risuko/cli-darwin-arm64",\n  "version": "0.1.0"\n}\n',
	);
	write(root, "src-tauri/tauri.conf.json", '{\n\t"productName": "Risuko",\n\t"version": "0.1.0"\n}\n');
	write(
		root,
		"src-tauri/Cargo.toml",
		'[workspace]\nmembers = ["risuko-bt"]\n\n[workspace.package]\nauthors = ["A"]\nversion = "0.1.0"\n\n[workspace.dependencies]\nserde = { version = "1" }\n\n[package]\nname = "risuko"\nversion.workspace = true\n',
	);
	write(root, "src-tauri/risuko-bt/Cargo.toml", '[package]\nname = "risuko-bt"\nversion.workspace = true\n');
	write(
		root,
		"src-tauri/Cargo.lock",
		'[[package]]\nname = "risuko"\nversion = "0.1.0"\n\n[[package]]\nname = "risuko-bt"\nversion = "0.1.0"\n\n[[package]]\nname = "unrelated"\nversion = "0.1.0"\n',
	);
	return root;
}

test("normalizeVersion strips a leading v and rejects junk", () => {
	assert.equal(normalizeVersion("v1.2.3"), "1.2.3");
	assert.equal(normalizeVersion("1.2.3-beta.1"), "1.2.3-beta.1");
	assert.throws(() => normalizeVersion("1.2"));
	assert.throws(() => normalizeVersion("vnext"));
	assert.throws(() => normalizeVersion(undefined));
});

test("setVersion rewrites every manifest and only workspace lock entries", () => {
	const root = fixture();
	try {
		const { version, changes } = setVersion(root, "v0.2.0-rc.1");
		assert.equal(version, "0.2.0-rc.1");
		assert.equal(changes.length, 6);
		assert.equal(JSON.parse(readFileSync(join(root, "package.json"), "utf8")).version, version);
		const cli = JSON.parse(readFileSync(join(root, "packages/risuko-cli/package.json"), "utf8"));
		assert.equal(cli.version, version);
		assert.equal(cli.optionalDependencies["@risuko/cli-darwin-arm64"], version);
		assert.equal(cli.optionalDependencies.other, "^1.0.0");
		const toml = readFileSync(join(root, "src-tauri/Cargo.toml"), "utf8");
		assert.match(toml, /\[workspace\.package\]\nauthors = \["A"\]\nversion = "0\.2\.0-rc\.1"/);
		assert.match(toml, /serde = \{ version = "1" \}/);
		const lock = readFileSync(join(root, "src-tauri/Cargo.lock"), "utf8");
		assert.match(lock, /name = "risuko"\nversion = "0\.2\.0-rc\.1"/);
		assert.match(lock, /name = "risuko-bt"\nversion = "0\.2\.0-rc\.1"/);
		assert.match(lock, /name = "unrelated"\nversion = "0\.1\.0"/);
		assert.match(readFileSync(join(root, "package.json"), "utf8"), /^\t"version"/m);
		assert.deepEqual(setVersion(root, "0.2.0-rc.1").changes, []);
	} finally {
		rmSync(root, { recursive: true, force: true });
	}
});

test("setVersion covers the real repository layout", () => {
	const root = mkdtempSync(join(tmpdir(), "risuko-set-version-repo-"));
	try {
		cpSync(join(repoRoot, "package.json"), join(root, "package.json"));
		cpSync(join(repoRoot, "packages"), join(root, "packages"), {
			recursive: true,
			filter: (src) => !src.includes("node_modules"),
		});
		mkdirSync(join(root, "src-tauri"));
		for (const file of ["Cargo.toml", "Cargo.lock", "tauri.conf.json"]) {
			cpSync(join(repoRoot, "src-tauri", file), join(root, "src-tauri", file));
		}
		const members = readFileSync(join(repoRoot, "src-tauri/Cargo.toml"), "utf8")
			.match(/^members\s*=\s*\[([^\]]*)\]/m)[1]
			.match(/"([^"]+)"/g)
			.map((m) => m.slice(1, -1));
		for (const member of members) {
			cpSync(join(repoRoot, "src-tauri", member, "Cargo.toml"), join(root, "src-tauri", member, "Cargo.toml"));
		}
		setVersion(root, "99.0.0");
		const lock = readFileSync(join(root, "src-tauri/Cargo.lock"), "utf8");
		for (const name of ["risuko", "risuko-bt", "risuko-engine", "risuko-cli", "risuko-napi"]) {
			assert.match(lock, new RegExp(`name = "${name}"\\nversion = "99\\.0\\.0"`), name);
		}
		assert.match(readFileSync(join(root, "src-tauri/tauri.conf.json"), "utf8"), /"version": "99\.0\.0"/);
	} finally {
		rmSync(root, { recursive: true, force: true });
	}
});
