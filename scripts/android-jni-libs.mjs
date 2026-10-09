import { existsSync, rmSync } from "node:fs";
import { resolve } from "node:path";

/**
 * cargo-mobile2 writes jniLibs entries as symlinks to CARGO_TARGET_DIR.
 * A relative value like `$workspace/../../.cargo-target` is stored with `..`
 * in the symlink target; AGP mergeJniLibFolders then reports the same
 * librisuko_lib.so as a duplicate resource
 */
export function canonicalizeCargoTargetDir(env, cwd = process.cwd()) {
	const next = { ...env };
	const raw = next.CARGO_TARGET_DIR?.trim();
	if (!raw) {
		delete next.CARGO_TARGET_DIR;
		return next;
	}
	next.CARGO_TARGET_DIR = resolve(cwd, raw);
	return next;
}

/** Drop persisted ABI .so symlinks so a later build cannot merge two origins */
export function cleanAndroidJniLibs(jniLibsRoot) {
	if (!existsSync(jniLibsRoot)) {
		return;
	}
	rmSync(jniLibsRoot, { recursive: true, force: true });
}
