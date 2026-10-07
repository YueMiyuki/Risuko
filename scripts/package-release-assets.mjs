#!/usr/bin/env node

import {
	appendFileSync,
	copyFileSync,
	existsSync,
	mkdirSync,
	readFileSync,
	readdirSync,
	statSync,
} from "node:fs";
import { dirname, join, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath, pathToFileURL } from "node:url";

const SCRIPT_DIR = dirname(fileURLToPath(import.meta.url));
const ROOT = resolve(SCRIPT_DIR, "..");

export function resolveCargoTargetDir(root = ROOT) {
	const fromEnv = process.env.CARGO_TARGET_DIR?.trim();
	if (fromEnv) return resolve(fromEnv);
	return join(root, "src-tauri", "target");
}

const PLATFORM_INFO = {
	"darwin/arm64": { os: "darwin", arch: "arm64" },
	"darwin/x64": { os: "darwin", arch: "x64" },
	"linux/arm64": { os: "linux", arch: "arm64" },
	"linux/x64": { os: "linux", arch: "x64" },
	"win32/ia32": { os: "win32", arch: "ia32" },
	"win32/x64": { os: "win32", arch: "x64" },
};

export function canonicalAssetName(version, os, arch, extension) {
	return `Risuko_${version}_${os}_${arch}.${extension}`;
}

export function resolvePlatform(platform) {
	const info = PLATFORM_INFO[platform];
	if (!info) {
		throw new Error(`Unsupported release platform: ${platform}`);
	}
	return info;
}

function readVersion(root) {
	const cargoToml = readFileSync(join(root, "src-tauri", "Cargo.toml"), "utf8");
	const match = cargoToml.match(/^version\s*=\s*"([^"]+)"/m);
	if (!match) {
		throw new Error("Could not read the app version from src-tauri/Cargo.toml");
	}
	return match[1];
}

export function resolveVersion({ version, githubRef, githubRefName, root = ROOT } = {}) {
	if (version) return String(version).replace(/^v/i, "");
	if (githubRef?.startsWith("refs/tags/")) {
		if (!githubRefName) {
			throw new Error("GITHUB_REF_NAME is required for tagged release packaging");
		}
		return githubRefName.replace(/^v/i, "");
	}
	return readVersion(root);
}

function listFiles(dir) {
	if (!existsSync(dir)) {
		throw new Error(`Missing bundle directory: ${dir}`);
	}

	const files = [];
	for (const entry of readdirSync(dir, { withFileTypes: true }).sort((a, b) =>
		a.name.localeCompare(b.name),
	)) {
		const path = join(dir, entry.name);
		if (entry.isDirectory()) {
			files.push(...listFiles(path));
		} else if (entry.isFile()) {
			files.push(path);
		}
	}
	return files;
}

function findOne(dir, predicate, description) {
	const matches = listFiles(dir).filter(predicate);
	if (matches.length !== 1) {
		const found = matches.length === 0 ? "none" : matches.join(", ");
		throw new Error(`Expected exactly one ${description}; found ${found}`);
	}
	return matches[0];
}

function assertFile(path, description) {
	if (!existsSync(path) || !statSync(path).isFile()) {
		throw new Error(`Missing ${description}: ${path}`);
	}
	return path;
}

function assertDirectory(path, description) {
	if (!existsSync(path) || !statSync(path).isDirectory()) {
		throw new Error(`Missing ${description}: ${path}`);
	}
	return path;
}

function copyAsset(source, outputDir, name) {
	mkdirSync(outputDir, { recursive: true });
	copyFileSync(source, join(outputDir, name));
	return name;
}

function copySignature(source, outputDir, name, { required = false } = {}) {
	const signature = `${source}.sig`;
	if (!existsSync(signature)) {
		if (required) {
			throw new Error(`Missing required signature: ${signature}`);
		}
		return null;
	}
	return copyAsset(signature, outputDir, `${name}.sig`);
}

function sourceFromSignature(signature, description) {
	if (!signature.endsWith(".sig")) {
		throw new Error(`Invalid ${description} signature path: ${signature}`);
	}
	return assertFile(signature.slice(0, -4), description);
}

function createMacTarball(appBundle, outputPath) {
	const result = spawnSync(
		"tar",
		["-czf", outputPath, "-C", dirname(appBundle), "Risuko.app"],
		{ stdio: "inherit" },
	);
	if (result.error) throw result.error;
	if (result.status !== 0) {
		throw new Error(`tar failed while packaging ${appBundle}`);
	}
}

function copyLinuxPackage(bundleDir, outputDir, version, arch, extension, extraAssets) {
	const source = findOne(
		join(bundleDir, extension),
		(path) => path.endsWith(`.${extension}`),
		`Linux ${extension.toUpperCase()} package`,
	);
	const name = canonicalAssetName(version, "linux", arch, extension);
	copyAsset(source, outputDir, name);
	extraAssets.push(name);

	const signature = copySignature(source, outputDir, name);
	if (signature) extraAssets.push(signature);
}

/**
 * Copy intentional desktop bundles into canonical release filenames
 *
 * Tagged builds preserve the Tauri-produced updater payload and its detached
 * signature. Manual builds produce the same Actions artifacts as before but
 * do not need updater signatures or package-manager bundles
 */
export function packageReleaseAssets(options = {}) {
	const root = resolve(options.root ?? ROOT);
	const platform = options.platform ?? process.env.MATRIX_PLATFORM;
	const target = options.target ?? process.env.MATRIX_TARGET;
	const githubRef = options.githubRef ?? process.env.GITHUB_REF ?? "";
	const githubRefName = options.githubRefName ?? process.env.GITHUB_REF_NAME;
	const tagged =
		options.tagged ??
		(process.env.RELEASE_TAGGED === undefined
			? githubRef.startsWith("refs/tags/")
			: process.env.RELEASE_TAGGED === "true");
	const { os, arch } = resolvePlatform(platform);
	const version = resolveVersion({
		version: options.version,
		githubRef,
		githubRefName,
		root,
	});
	if (!target) throw new Error("MATRIX_TARGET is required for release packaging");

	const cargoTargetDir = resolveCargoTargetDir(root);
	const bundleDir = resolve(
		options.bundleDir ?? join(cargoTargetDir, target, "release", "bundle"),
	);
	const outputDir = resolve(options.outputDir ?? root);
	mkdirSync(outputDir, { recursive: true });
	const extraAssets = [];
	const result = {
		version,
		os,
		arch,
		appAsset: "",
		updaterSignature: "",
		plainAppImageAsset: "",
		portableAsset: "",
		extraAssets,
	};

	if (os === "darwin") {
		const appAsset = canonicalAssetName(version, os, arch, "app.tar.gz");
		if (tagged) {
			const signature = findOne(
				bundleDir,
				(path) => path.endsWith(".app.tar.gz.sig"),
				"macOS updater signature",
			);
			copyAsset(sourceFromSignature(signature, "macOS updater"), outputDir, appAsset);
			result.updaterSignature = copySignature(
				signature.slice(0, -4),
				outputDir,
				appAsset,
				{ required: true },
			);

			const dmg = findOne(
				join(bundleDir, "dmg"),
				(path) => path.endsWith(".dmg"),
				"macOS DMG",
			);
			const dmgName = canonicalAssetName(version, os, arch, "dmg");
			copyAsset(dmg, outputDir, dmgName);
			extraAssets.push(dmgName);
		} else {
			const appBundle = assertDirectory(
				join(bundleDir, "macos", "Risuko.app"),
				"macOS app bundle",
			);
			createMacTarball(appBundle, join(outputDir, appAsset));
		}
		result.appAsset = appAsset;
	} else if (os === "linux") {
		const appImage = findOne(
			join(bundleDir, "appimage"),
			(path) => path.endsWith(".AppImage"),
			"Linux AppImage",
		);

		if (tagged) {
			const bundleFiles = listFiles(bundleDir);
			const updaterSignatures = bundleFiles.filter((path) =>
				path.endsWith(".AppImage.tar.gz.sig"),
			);
			const fallbackSignatures = bundleFiles.filter((path) =>
				path.endsWith(".AppImage.sig"),
			);
			const signatures = updaterSignatures.length > 0 ? updaterSignatures : fallbackSignatures;
			if (signatures.length !== 1) {
				const found = signatures.length === 0 ? "none" : signatures.join(", ");
				throw new Error(`Expected exactly one Linux updater signature; found ${found}`);
			}
			const updaterSignature = signatures[0];
			const updaterSource = sourceFromSignature(updaterSignature, "Linux updater");
			const extension = updaterSource.endsWith(".AppImage.tar.gz")
				? "AppImage.tar.gz"
				: "AppImage";
			const appAsset = canonicalAssetName(version, os, arch, extension);
			copyAsset(updaterSource, outputDir, appAsset);
			result.updaterSignature = copySignature(updaterSource, outputDir, appAsset, {
				required: true,
			});
			result.appAsset = appAsset;

			const plainAppImage = canonicalAssetName(version, os, arch, "AppImage");
			if (plainAppImage !== appAsset) {
				copyAsset(appImage, outputDir, plainAppImage);
				result.plainAppImageAsset = plainAppImage;
				extraAssets.push(plainAppImage, `${plainAppImage}.sha256`);
			}

			copyLinuxPackage(bundleDir, outputDir, version, arch, "deb", extraAssets);
			copyLinuxPackage(bundleDir, outputDir, version, arch, "rpm", extraAssets);
		} else {
			result.appAsset = canonicalAssetName(version, os, arch, "AppImage");
			copyAsset(appImage, outputDir, result.appAsset);
		}
	} else {
		const nsisDir = join(bundleDir, "nsis");
		const appAsset = canonicalAssetName(version, os, arch, "setup.exe");
		if (tagged) {
			const signature = findOne(
				nsisDir,
				(path) => path.endsWith("-setup.exe.sig"),
				"Windows updater signature",
			);
			copyAsset(sourceFromSignature(signature, "Windows updater"), outputDir, appAsset);
			result.updaterSignature = copySignature(
				signature.slice(0, -4),
				outputDir,
				appAsset,
				{ required: true },
			);
		} else {
			const setup = findOne(
				nsisDir,
				(path) => path.endsWith("-setup.exe"),
				"Windows setup executable",
			);
			copyAsset(setup, outputDir, appAsset);
		}
		result.appAsset = appAsset;

		const portable = assertFile(
			join(cargoTargetDir, target, "release", "Risuko.exe"),
			"Windows portable executable",
		);
		result.portableAsset = canonicalAssetName(version, os, arch, "portable.exe");
		copyAsset(portable, outputDir, result.portableAsset);
	}

	return result;
}

export function writeGithubEnvironment(result, envFile = process.env.GITHUB_ENV) {
	if (!envFile) return;

	const values = {
		APP_ASSET: result.appAsset,
		APP_PLATFORM_LABEL: `${result.os}-${result.arch}`,
		UPDATER_SIG_FILE: result.updaterSignature,
		PLAIN_APPIMAGE_ASSET: result.plainAppImageAsset,
		PORTABLE_ASSET: result.portableAsset,
		EXTRA_APP_ASSETS: result.extraAssets.join("\n"),
	};

	for (const [key, value] of Object.entries(values)) {
		if (value.includes("\n")) {
			appendFileSync(envFile, `${key}<<RISUKO_RELEASE_ASSETS\n${value}\nRISUKO_RELEASE_ASSETS\n`);
		} else {
			appendFileSync(envFile, `${key}=${value}\n`);
		}
	}
}

function run() {
	const result = packageReleaseAssets();
	writeGithubEnvironment(result);
	console.log(
		JSON.stringify(
			{
				appAsset: result.appAsset,
				updaterSignature: result.updaterSignature,
				plainAppImageAsset: result.plainAppImageAsset,
				portableAsset: result.portableAsset,
				extraAssets: result.extraAssets,
			},
			null,
			2,
		),
	);
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
	run();
}
