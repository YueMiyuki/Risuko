#!/usr/bin/env node
import { execFileSync, spawnSync } from "node:child_process";
import { resolve } from "node:path";
import { pathToFileURL } from "node:url";

const KEY_CHECK_SCRIPT = resolve(import.meta.dirname, "check-config-keys.mjs");

const KEY_CHECK_INPUTS = new Set([
	"scripts/check-config-keys.mjs",
	"scripts/config-key-exceptions.json",
	"src/shared/configKeys.ts",
	"src/shared/configKeys.json",
	"src/shared/syncCategories.ts",
	"src-tauri/risuko-engine/src/config/defaults.rs",
]);

export function needsKeyCheck(staged) {
	return staged.some((file) => KEY_CHECK_INPUTS.has(file));
}

export function splitStaged(staged, unstaged) {
	const partly = new Set(unstaged);
	return {
		restage: staged.filter((file) => !partly.has(file)),
		skipped: staged.filter((file) => partly.has(file)),
	};
}

function list(...args) {
	return execFileSync("git", [...args, "-z"], { encoding: "utf8" })
		.split("\0")
		.filter(Boolean);
}

function main() {
	const staged = list("diff", "--cached", "--name-only", "--diff-filter=ACMR");
	if (staged.length === 0) {
		return;
	}
	const { restage, skipped } = splitStaged(
		staged,
		list("diff", "--name-only"),
	);

	execFileSync(
		"pnpx",
		[
			"@biomejs/biome",
			"check",
			"--write",
			"--staged",
			"--no-errors-on-unmatched",
			"--max-diagnostics",
			"none",
		],
		{ stdio: "inherit", shell: process.platform === "win32" },
	);

	if (restage.length > 0) {
		execFileSync("git", ["add", "--", ...restage], { stdio: "inherit" });
	}
	if (skipped.length > 0) {
		console.warn(
			`Formatted but not re-staged because only part of the file is staged: ${skipped.join(", ")}`,
		);
	}

	if (needsKeyCheck(staged)) {
		const result = spawnSync(process.execPath, [KEY_CHECK_SCRIPT], {
			stdio: "inherit",
		});
		if (result.status !== 0) {
			process.exitCode = result.status ?? 1;
		}
	}
}

if (import.meta.url === pathToFileURL(resolve(process.argv[1] ?? "")).href) {
	main();
}
