import assert from "node:assert/strict";
import { test } from "node:test";
import { pickDeclaredKeys } from "./syncFilter.ts";

test("declared keys are kept and undeclared keys are dropped", () => {
	assert.deepEqual(
		pickDeclaredKeys(["theme", "locale"], {
			theme: "dark",
			locale: "en-US",
			"completion-script-command": "sh",
		}),
		{ theme: "dark", locale: "en-US" },
	);
});

test("a camelCase spelling of a declared key is dropped", () => {
	assert.deepEqual(
		pickDeclaredKeys(["completion-script-command"], {
			completionScriptCommand: "sh",
		}),
		{},
	);
});

test("a parsed __proto__ key is neither copied nor used as the prototype", () => {
	const picked = pickDeclaredKeys(
		["theme"],
		JSON.parse('{"__proto__": {"x": 1}, "theme": "dark"}'),
	);
	assert.deepEqual(Object.keys(picked), ["theme"]);
	assert.equal(Object.getPrototypeOf(picked), Object.prototype);
	assert.equal((picked as { x?: unknown }).x, undefined);
});

test("no declared keys yield an empty object", () => {
	assert.deepEqual(pickDeclaredKeys([], { theme: "dark" }), {});
});
