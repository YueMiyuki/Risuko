import assert from "node:assert/strict";
import { test } from "node:test";
import { needsKeyCheck, splitStaged } from "./pre-commit.mjs";

test("fully staged files are re-staged and partly staged ones are left alone", () => {
	assert.deepEqual(
		splitStaged(["a.ts", "b.ts", "c.ts"], ["b.ts", "unrelated.ts"]),
		{ restage: ["a.ts", "c.ts"], skipped: ["b.ts"] },
	);
	assert.deepEqual(splitStaged([], ["b.ts"]), { restage: [], skipped: [] });
});

test("the key check runs when a commit touches a file it reads", () => {
	assert.equal(needsKeyCheck(["src/shared/configKeys.json"]), true);
	assert.equal(needsKeyCheck(["src/shared/configKeys.ts"]), true);
	assert.equal(needsKeyCheck(["src/shared/syncCategories.ts"]), true);
	assert.equal(
		needsKeyCheck(["src-tauri/risuko-engine/src/config/defaults.rs"]),
		true,
	);
	assert.equal(needsKeyCheck(["scripts/config-key-exceptions.json"]), true);
	assert.equal(needsKeyCheck(["scripts/check-config-keys.mjs"]), true);
	assert.equal(
		needsKeyCheck(["README.md", "src/shared/configKeys.json", "a.ts"]),
		true,
	);
});

test("the key check is skipped for unrelated files", () => {
	assert.equal(needsKeyCheck(["README.md", "src/renderer/App.vue"]), false);
	assert.equal(
		needsKeyCheck(["src-tauri/risuko-engine/src/config/mod.rs"]),
		false,
	);
	assert.equal(needsKeyCheck(["src/shared/constants.ts"]), false);
	assert.equal(needsKeyCheck([]), false);
});
