import assert from "node:assert/strict";
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import test from "node:test";

import {
	canonicalAssetName,
	packageReleaseAssets,
	resolveCargoTargetDir,
	resolvePlatform,
	writeGithubEnvironment,
} from "./package-release-assets.mjs";

function write(path, contents) {
	mkdirSync(dirname(path), { recursive: true });
	writeFileSync(path, contents);
}

function withFixture(callback) {
	const root = mkdtempSync(join(tmpdir(), "risuko-release-assets-"));
	try {
		return callback(root);
	} finally {
		rmSync(root, { recursive: true, force: true });
	}
}

test("uses Node runtime architecture labels in canonical names", () => {
	assert.equal(
		canonicalAssetName("0.6.0", "darwin", "arm64", "dmg"),
		"Risuko_0.6.0_darwin_arm64.dmg",
	);
	assert.deepEqual(resolvePlatform("win32/ia32"), { os: "win32", arch: "ia32" });
	assert.throws(() => resolvePlatform("linux/ppc64"), /Unsupported release platform/);
});

test("packages macOS updater and DMG under canonical names", () => {
	withFixture((root) => {
		const bundleDir = join(root, "bundle");
		const outputDir = join(root, "out");
		write(join(bundleDir, "macos", "Risuko.app.tar.gz"), "mac-updater");
		write(join(bundleDir, "macos", "Risuko.app.tar.gz.sig"), "mac-signature");
		write(join(bundleDir, "dmg", "Risuko.dmg"), "mac-dmg");

		const result = packageReleaseAssets({
			root,
			bundleDir,
			outputDir,
			platform: "darwin/arm64",
			target: "aarch64-apple-darwin",
			tagged: true,
			version: "0.6.0",
		});

		assert.equal(result.appAsset, "Risuko_0.6.0_darwin_arm64.app.tar.gz");
		assert.equal(result.updaterSignature, `${result.appAsset}.sig`);
		assert.deepEqual(result.extraAssets, ["Risuko_0.6.0_darwin_arm64.dmg"]);
		assert.equal(readFileSync(join(outputDir, result.appAsset), "utf8"), "mac-updater");
		assert.equal(readFileSync(join(outputDir, result.updaterSignature), "utf8"), "mac-signature");
	});
});

test("does not list a duplicate plain AppImage when it is the updater payload", () => {
	withFixture((root) => {
		const bundleDir = join(root, "bundle");
		const outputDir = join(root, "out");
		write(join(bundleDir, "appimage", "Risuko.AppImage"), "linux-appimage");
		write(join(bundleDir, "appimage", "Risuko.AppImage.sig"), "linux-signature");
		write(join(bundleDir, "deb", "risuko.deb"), "linux-deb");
		write(join(bundleDir, "deb", "risuko.deb.sig"), "deb-signature");
		write(join(bundleDir, "rpm", "risuko.rpm"), "linux-rpm");
		write(join(bundleDir, "rpm", "risuko.rpm.sig"), "rpm-signature");

		const result = packageReleaseAssets({
			root,
			bundleDir,
			outputDir,
			platform: "linux/x64",
			target: "x86_64-unknown-linux-gnu",
			tagged: true,
			version: "0.6.0",
		});

		assert.equal(result.appAsset, "Risuko_0.6.0_linux_x64.AppImage");
		assert.equal(result.plainAppImageAsset, "");
		assert.deepEqual(result.extraAssets, []);
		assert.deepEqual(result.linuxPackages, [
			"Risuko_0.6.0_linux_x64.deb",
			"Risuko_0.6.0_linux_x64.rpm",
		]);
		assert.equal(readFileSync(join(outputDir, result.appAsset), "utf8"), "linux-appimage");
	});
});

test("adds a distinct plain AppImage and checksum slot for tarball updater payloads", () => {
	withFixture((root) => {
		const bundleDir = join(root, "bundle");
		const outputDir = join(root, "out");
		write(join(bundleDir, "appimage", "Risuko.AppImage"), "plain-appimage");
		write(join(bundleDir, "updater", "Risuko.AppImage.tar.gz"), "updater-tarball");
		write(join(bundleDir, "updater", "Risuko.AppImage.tar.gz.sig"), "tarball-signature");
		write(join(bundleDir, "deb", "risuko.deb"), "linux-deb");
		write(join(bundleDir, "rpm", "risuko.rpm"), "linux-rpm");

		const result = packageReleaseAssets({
			root,
			bundleDir,
			outputDir,
			platform: "linux/arm64",
			target: "aarch64-unknown-linux-gnu",
			tagged: true,
			version: "0.6.0",
		});

		assert.equal(result.appAsset, "Risuko_0.6.0_linux_arm64.AppImage.tar.gz");
		assert.equal(result.plainAppImageAsset, "Risuko_0.6.0_linux_arm64.AppImage");
		assert.deepEqual(result.extraAssets.slice(0, 2), [
			"Risuko_0.6.0_linux_arm64.AppImage",
			"Risuko_0.6.0_linux_arm64.AppImage.sha256",
		]);

		const envFile = join(root, "github-env");
		writeGithubEnvironment(result, envFile);
		assert.match(readFileSync(envFile, "utf8"), /EXTRA_APP_ASSETS<<RISUKO_RELEASE_ASSETS/);
		assert.match(readFileSync(envFile, "utf8"), /Risuko_0\.6\.0_linux_arm64\.AppImage\.sha256/);
	});
});

test("packages Windows signed setup and portable executables", () => {
	withFixture((root) => {
		const bundleDir = join(root, "bundle");
		const outputDir = join(root, "out");
		write(join(bundleDir, "nsis", "Risuko-setup.exe"), "windows-setup");
		write(join(bundleDir, "nsis", "Risuko-setup.exe.sig"), "windows-signature");
		write(
			join(root, "src-tauri", "target", "x86_64-pc-windows-msvc", "release", "Risuko.exe"),
			"windows-portable",
		);

		const result = packageReleaseAssets({
			root,
			bundleDir,
			outputDir,
			platform: "win32/x64",
			target: "x86_64-pc-windows-msvc",
			tagged: true,
			version: "0.6.0",
		});

		assert.equal(result.appAsset, "Risuko_0.6.0_win32_x64.setup.exe");
		assert.equal(result.portableAsset, "Risuko_0.6.0_win32_x64.portable.exe");
		assert.equal(readFileSync(join(outputDir, result.appAsset), "utf8"), "windows-setup");
		assert.equal(
			readFileSync(join(outputDir, result.portableAsset), "utf8"),
			"windows-portable",
		);
	});
});

test("reads Windows portable exe from CARGO_TARGET_DIR when set", () => {
	withFixture((root) => {
		const previous = process.env.CARGO_TARGET_DIR;
		const cargoTarget = join(root, "persistent-target");
		process.env.CARGO_TARGET_DIR = cargoTarget;
		try {
			assert.equal(resolveCargoTargetDir(root), cargoTarget);
			const bundleDir = join(root, "bundle");
			const outputDir = join(root, "out");
			write(join(bundleDir, "nsis", "Risuko-setup.exe"), "windows-setup");
			write(join(cargoTarget, "x86_64-pc-windows-msvc", "release", "Risuko.exe"), "from-env");

			const result = packageReleaseAssets({
				root,
				bundleDir,
				outputDir,
				platform: "win32/x64",
				target: "x86_64-pc-windows-msvc",
				tagged: false,
				version: "0.6.0",
			});

			assert.equal(result.portableAsset, "Risuko_0.6.0_win32_x64.portable.exe");
			assert.equal(readFileSync(join(outputDir, result.portableAsset), "utf8"), "from-env");
		} finally {
			if (previous === undefined) delete process.env.CARGO_TARGET_DIR;
			else process.env.CARGO_TARGET_DIR = previous;
		}
	});
});

test("treats an explicitly manual tag ref as an unsigned Actions artifact build", () => {
	withFixture((root) => {
		const bundleDir = join(root, "bundle");
		const outputDir = join(root, "out");
		write(join(bundleDir, "appimage", "Risuko.AppImage"), "linux-appimage");

		const result = packageReleaseAssets({
			root,
			bundleDir,
			outputDir,
			platform: "linux/x64",
			target: "x86_64-unknown-linux-gnu",
			githubRef: "refs/tags/v0.6.0",
			githubRefName: "v0.6.0",
			tagged: false,
		});

		assert.equal(result.appAsset, "Risuko_0.6.0_linux_x64.AppImage");
		assert.equal(result.updaterSignature, "");
		assert.equal(result.extraAssets.length, 0);
	});
});
