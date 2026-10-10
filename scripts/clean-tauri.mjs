#!/usr/bin/env node
import { spawnSync } from "node:child_process";
import { existsSync, readdirSync, rmSync, statSync } from "node:fs";
import { join, resolve } from "node:path";

const ROOT = resolve(import.meta.dirname, "..");
const TAURI = join(ROOT, "src-tauri");
const TARGET = join(TAURI, "target");
const GEN_ANDROID = join(TAURI, "gen", "android");

const args = new Set(process.argv.slice(2));
const dryRun = args.has("--dry-run");
const full = args.has("--full");
const withAndroidRust = args.has("--android") || args.has("--all");
const withIncremental = args.has("--incremental") || args.has("--all");
const skipGradle = args.has("--keep-gradle");

const removals = [];

function dirSizeBytesRecursive(path) {
	let total = 0;
	for (const entry of readdirSync(path, { withFileTypes: true })) {
		const child = join(path, entry.name);
		if (entry.isDirectory()) {
			total += dirSizeBytesRecursive(child);
		} else if (entry.isFile()) {
			total += statSync(child).size;
		}
	}
	return total;
}

function dirSizeBytes(path) {
	if (!existsSync(path)) return 0;
	try {
		const stat = statSync(path);
		if (!stat.isDirectory()) return stat.size;
		if (process.platform !== "win32") {
			const out = spawnSync("du", ["-sk", path], { encoding: "utf8" });
			if (out.status === 0 && out.stdout) {
				const kb = Number.parseInt(out.stdout.trim().split(/\s+/)[0] ?? "0", 10);
				if (Number.isFinite(kb)) return kb * 1024;
			}
		}
		return dirSizeBytesRecursive(path);
	} catch {
		return 0;
	}
}

function formatBytes(bytes) {
	if (bytes >= 1024 ** 3) return `${(bytes / 1024 ** 3).toFixed(1)} GB`;
	if (bytes >= 1024 ** 2) return `${(bytes / 1024 ** 2).toFixed(1)} MB`;
	if (bytes >= 1024) return `${(bytes / 1024).toFixed(1)} KB`;
	return `${bytes} B`;
}

function queue(path, label = path) {
	if (existsSync(path)) {
		removals.push({ path, label, size: dirSizeBytes(path) });
	}
}

function queueGradleArtifacts() {
	queue(join(GEN_ANDROID, "app", "build"), "gen/android/app/build");
	queue(join(GEN_ANDROID, "build"), "gen/android/build");
	queue(join(GEN_ANDROID, ".gradle"), "gen/android/.gradle");
}

function queueCrossTargets() {
	const cross = [
		"aarch64-linux-android",
		"armv7-linux-androideabi",
		"i686-linux-android",
		"x86_64-linux-android",
		"x86_64-pc-windows-msvc",
		"x86_64-unknown-linux-gnu",
	];
	for (const triple of cross) {
		queue(join(TARGET, triple), `target/${triple}`);
	}
}

function queueReleaseAndIncremental() {
	queue(join(TARGET, "release"), "target/release");
	if (withIncremental) {
		queue(join(TARGET, "debug", "incremental"), "target/debug/incremental");
	}
}

function runCargoClean() {
	const cargoArgs = ["clean", "--manifest-path", join(TAURI, "Cargo.toml")];
	if (!full) cargoArgs.push("--release");
	console.log(`\n→ cargo ${cargoArgs.join(" ")}`);
	if (dryRun) return;
	const result = spawnSync("cargo", cargoArgs, { stdio: "inherit", cwd: ROOT });
	if (result.status !== 0) process.exit(result.status ?? 1);
}

function removeQueued() {
	let total = 0;
	for (const { path, label, size } of removals) {
		total += size;
		console.log(`${dryRun ? "[dry-run] " : ""}remove ${label} (${formatBytes(size)})`);
		if (!dryRun) rmSync(path, { recursive: true, force: true });
	}
	return total;
}

console.log("Risuko src-tauri cleanup");
console.log(`  mode: ${full ? "full (cargo clean)" : "desktop-friendly"}`);
if (dryRun) console.log("  dry-run: no files deleted\n");

if (full) {
	runCargoClean();
	if (!skipGradle) queueGradleArtifacts();
} else {
	if (!skipGradle) queueGradleArtifacts();
	if (withAndroidRust) queueCrossTargets();
	else {
		queue(join(TARGET, "x86_64-pc-windows-msvc"), "target/x86_64-pc-windows-msvc");
		queue(join(TARGET, "x86_64-unknown-linux-gnu"), "target/x86_64-unknown-linux-gnu");
	}
	queueReleaseAndIncremental();
	runCargoClean();
}

const freed = removeQueued();
console.log(`\n${dryRun ? "Would free" : "Freed"} ~${formatBytes(freed)}.`);

if (!full && !withAndroidRust) {
	console.log(
		"\nKept target/debug for faster desktop rebuilds. Pass --android to also remove Android Rust targets (~40+ GB).",
	);
}
if (!full && !withIncremental) {
	console.log("Pass --incremental to drop target/debug/incremental (~20+ GB on large workspaces).");
}
if (!full) {
	console.log("Pass --full for cargo clean (slowest next build, maximum space recovery).");
}
