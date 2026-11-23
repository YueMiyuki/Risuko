import assert from "node:assert/strict";
import {
	mkdirSync,
	mkdtempSync,
	readlinkSync,
	rmSync,
	symlinkSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import {
	canonicalizeCargoTargetDir,
	cleanAndroidJniLibs,
} from "./android-jni-libs.mjs";

function withTemp(callback) {
	const root = mkdtempSync(join(tmpdir(), "risuko-android-jni-"));
	try {
		return callback(root);
	} finally {
		rmSync(root, { recursive: true, force: true });
	}
}

test("canonicalizes a relative CARGO_TARGET_DIR so jniLibs symlinks have no ..", () => {
	withTemp((root) => {
		const env = canonicalizeCargoTargetDir(
			{ CARGO_TARGET_DIR: "../cargo-target" },
			join(root, "app"),
		);
		assert.equal(env.CARGO_TARGET_DIR, join(root, "cargo-target"));
	});
});

test("leaves CARGO_TARGET_DIR unset when the env var is empty", () => {
	const env = canonicalizeCargoTargetDir({ CARGO_TARGET_DIR: "  " });
	assert.equal(env.CARGO_TARGET_DIR, undefined);
});

test("normalizes an absolute CARGO_TARGET_DIR that still contains ..", () => {
	withTemp((root) => {
		const messy = `${join(root, "app")}/../cargo-target`;
		const env = canonicalizeCargoTargetDir({ CARGO_TARGET_DIR: messy }, root);
		assert.equal(env.CARGO_TARGET_DIR, join(root, "cargo-target"));
		assert.equal(env.CARGO_TARGET_DIR.includes(".."), false);
	});
});

test("removes leftover jniLibs .so files including mixed symlink targets", () => {
	withTemp((root) => {
		const jni = join(root, "jniLibs");
		const oldTarget = join(root, "old", "librisuko_lib.so");
		const newTarget = join(root, "new", "librisuko_lib.so");
		mkdirSync(join(root, "old"), { recursive: true });
		mkdirSync(join(root, "new"), { recursive: true });
		writeFileSync(oldTarget, "old");
		writeFileSync(newTarget, "new");
		mkdirSync(join(jni, "armeabi-v7a"), { recursive: true });
		mkdirSync(join(jni, "x86"), { recursive: true });
		symlinkSync(newTarget, join(jni, "armeabi-v7a", "librisuko_lib.so"));
		symlinkSync(oldTarget, join(jni, "x86", "librisuko_lib.so"));

		cleanAndroidJniLibs(jni);

		assert.throws(() => readlinkSync(join(jni, "armeabi-v7a", "librisuko_lib.so")));
		assert.throws(() => readlinkSync(join(jni, "x86", "librisuko_lib.so")));
	});
});
