import { existsSync, rmSync } from "node:fs";
import { resolve } from "node:path";

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

export function cleanAndroidJniLibs(jniLibsRoot) {
	if (!existsSync(jniLibsRoot)) {
		return;
	}
	rmSync(jniLibsRoot, { recursive: true, force: true });
}
