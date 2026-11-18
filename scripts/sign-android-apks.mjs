#!/usr/bin/env node
import { spawnSync } from "node:child_process";
import { existsSync, mkdtempSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { walkFiles } from "./walk-files.mjs";

const projectRoot = resolve(fileURLToPath(new URL("..", import.meta.url)));

const envFile = resolve(projectRoot, ".env");
if (existsSync(envFile)) {
	process.loadEnvFile(envFile);
}

const apkRoot = resolve(
	projectRoot,
	"src-tauri/gen/android/app/build/outputs/apk",
);

function fail(message) {
	throw new Error(message);
}

function run(command, args, options = {}) {
	const result = spawnSync(command, args, {
		stdio: "inherit",
		env: process.env,
		...options,
	});
	if (result.status !== 0) {
		fail(`${command} failed with exit code ${result.status}`);
	}
}

function versionParts(version) {
	return version.split(".").map((part) => Number(part));
}

function compareVersions(a, b) {
	const left = versionParts(a);
	const right = versionParts(b);
	const length = Math.max(left.length, right.length);
	for (let index = 0; index < length; index += 1) {
		const diff = (left[index] || 0) - (right[index] || 0);
		if (diff !== 0) {
			return diff;
		}
	}
	return 0;
}

function findBuildTool(name) {
	const explicit = process.env[`${name.toUpperCase()}_PATH`];
	if (explicit) {
		return explicit;
	}
	const sdkRoot = process.env.ANDROID_HOME || process.env.ANDROID_SDK_ROOT;
	if (!sdkRoot) {
		fail("ANDROID_HOME or ANDROID_SDK_ROOT is required to locate Android build tools");
	}
	const buildToolsRoot = join(sdkRoot, "build-tools");
	if (!existsSync(buildToolsRoot)) {
		fail(`Android build-tools directory not found: ${buildToolsRoot}`);
	}
	const candidates =
		process.platform === "win32"
			? [name, `${name}.bat`, `${name}.cmd`, `${name}.exe`]
			: [name];
	const versions = readdirSync(buildToolsRoot)
		.filter((entry) =>
			candidates.some((candidate) =>
				existsSync(join(buildToolsRoot, entry, candidate)),
			),
		)
		.sort(compareVersions)
		.reverse();
	if (versions.length === 0) {
		fail(`${name} not found under ${buildToolsRoot}`);
	}
	const toolName = candidates.find((candidate) =>
		existsSync(join(buildToolsRoot, versions[0], candidate)),
	);
	return join(buildToolsRoot, versions[0], toolName);
}

function findApks(root, matches) {
	if (!existsSync(root)) {
		fail(`APK output directory not found: ${root}`);
	}
	return walkFiles(root)
		.filter((path) => matches(basename(path)))
		.sort();
}

function findUnsignedApks(root) {
	return findApks(root, (name) => name.endsWith("-unsigned.apk"));
}

function findDebugApks(root) {
	return findApks(
		root,
		(name) => /-debug\.apk$/.test(name) && !name.includes("-aligned"),
	);
}

function resolveKeystore() {
	const keystorePath = process.env.ANDROID_SIGNING_KEYSTORE_PATH;
	if (keystorePath) {
		return { path: keystorePath };
	}
	const encoded = process.env.ANDROID_SIGNING_KEYSTORE_BASE64;
	if (!encoded) {
		fail(
			"ANDROID_SIGNING_KEYSTORE_PATH or ANDROID_SIGNING_KEYSTORE_BASE64 is required",
		);
	}
	const dir = mkdtempSync(join(tmpdir(), "risuko-android-signing-"));
	const path = join(dir, "release.keystore");
	writeFileSync(path, Buffer.from(encoded, "base64"));
	return { path, cleanup: () => rmSync(dir, { recursive: true, force: true }) };
}

const storePassword = process.env.ANDROID_SIGNING_KEYSTORE_PASSWORD;
const alias = process.env.ANDROID_SIGNING_KEY_ALIAS;
if (!storePassword) {
	fail("ANDROID_SIGNING_KEYSTORE_PASSWORD is required");
}
if (!alias) {
	fail("ANDROID_SIGNING_KEY_ALIAS is required");
}
if (!process.env.ANDROID_SIGNING_KEY_PASSWORD) {
	process.env.ANDROID_SIGNING_KEY_PASSWORD = storePassword;
}

const debugMode = process.argv.includes("--debug");
const apksigner = findBuildTool("apksigner");
const zipalign = findBuildTool("zipalign");
const apksToSign = debugMode ? findDebugApks(apkRoot) : findUnsignedApks(apkRoot);

if (apksToSign.length === 0) {
	fail(
		debugMode
			? `No debug APKs found under ${apkRoot}`
			: `No unsigned release APKs found under ${apkRoot}`,
	);
}

const keystore = resolveKeystore();

try {
	for (const inputApk of apksToSign) {
		const outputApk = debugMode
			? inputApk
			: inputApk.replace(/-unsigned\.apk$/, ".apk");
		const alignedApk = join(
			dirname(inputApk),
			`${basename(inputApk, ".apk")}-aligned.apk`,
		);
		run(zipalign, ["-f", "-p", "4", inputApk, alignedApk]);
		run(apksigner, [
			"sign",
			"--ks",
			keystore.path,
			"--ks-key-alias",
			alias,
			"--ks-pass",
			"env:ANDROID_SIGNING_KEYSTORE_PASSWORD",
			"--key-pass",
			"env:ANDROID_SIGNING_KEY_PASSWORD",
			"--out",
			outputApk,
			alignedApk,
		]);
		run(apksigner, ["verify", "--verbose", "--print-certs", outputApk]);
		rmSync(alignedApk, { force: true });
		console.log(`signed: ${outputApk}`);
	}
} finally {
	keystore.cleanup?.();
}