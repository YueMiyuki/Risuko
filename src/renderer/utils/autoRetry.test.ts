import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { test } from "node:test";
import {
	AUTO_RETRY_DEFAULT_MAX_ATTEMPTS,
	AUTO_RETRY_MAX_ATTEMPTS_LIMIT,
	hasRetryBudget,
	isPermanentErrorCode,
	normalizeMaxAttempts,
} from "./autoRetry.ts";

const PERMANENT_CODES = [
	"300",
	"301",
	"302",
	"400",
	"401",
	"502",
	"530",
	"531",
	"533",
	"534",
	"540",
	"541",
	"542",
	"550",
	"551",
	"552",
	"553",
	"554",
];

test("errors that cannot succeed are permanent", () => {
	for (const code of PERMANENT_CODES) {
		assert.equal(isPermanentErrorCode(code), true, code);
	}
});

test("the engine and the window agree on which errors are permanent", () => {
	const source = readFileSync(
		resolve(
			import.meta.dirname,
			"../../../src-tauri/risuko-engine/src/engine/headless_policy.rs",
		),
		"utf8",
	);
	const list = source.match(
		/const PERMANENT_ERROR_CODES: &\[&str\] = &\[([^\]]*)\];/,
	);
	assert.ok(list, "the engine list was not found");
	const engineCodes = [...list[1].matchAll(/"([^"]+)"/g)].map((m) => m[1]);
	assert.ok(engineCodes.length >= 10, "the engine list looks truncated");

	for (const code of engineCodes.filter((code) => code !== "315")) {
		assert.equal(isPermanentErrorCode(code), true, code);
	}
	assert.ok(engineCodes.includes("315"));
	assert.equal(isPermanentErrorCode("315"), false);
	for (const code of PERMANENT_CODES) {
		assert.ok(engineCodes.includes(code), code);
	}
});

test("a permanent code is recognised as a string or a number", () => {
	assert.equal(isPermanentErrorCode("302"), true);
	assert.equal(isPermanentErrorCode(302), true);
	assert.equal(isPermanentErrorCode(401), true);
});

test("network and server errors stay retryable", () => {
	for (const code of ["203", "305", "315", "200", "500", "520", "532", 203]) {
		assert.equal(isPermanentErrorCode(code), false, String(code));
	}
});

test("an empty or missing code is not permanent", () => {
	assert.equal(isPermanentErrorCode(""), false);
	assert.equal(isPermanentErrorCode(undefined), false);
	assert.equal(isPermanentErrorCode(null), false);
});

test("a missing, empty or unusable maximum falls back to the default", () => {
	assert.equal(AUTO_RETRY_DEFAULT_MAX_ATTEMPTS, 5);
	for (const raw of [undefined, null, "", "  ", "abc", Number.NaN, -1, "-3"]) {
		assert.equal(normalizeMaxAttempts(raw), 5, String(raw));
	}
});

test("zero means unlimited", () => {
	assert.equal(normalizeMaxAttempts(0), 0);
	assert.equal(normalizeMaxAttempts("0"), 0);
});

test("the maximum is truncated to an integer and capped", () => {
	assert.equal(AUTO_RETRY_MAX_ATTEMPTS_LIMIT, 1000);
	assert.equal(normalizeMaxAttempts(7.9), 7);
	assert.equal(normalizeMaxAttempts("12"), 12);
	assert.equal(normalizeMaxAttempts(5000), 1000);
	assert.equal(normalizeMaxAttempts(1000), 1000);
});

test("retries continue while fewer attempts than the maximum were made", () => {
	assert.equal(hasRetryBudget(4, 5), true);
	assert.equal(hasRetryBudget(5, 5), false);
	assert.equal(hasRetryBudget(6, 5), false);
	assert.equal(hasRetryBudget(0, 1), true);
	assert.equal(hasRetryBudget(1, 1), false);
});

test("an unlimited maximum always leaves budget", () => {
	assert.equal(hasRetryBudget(99, 0), true);
	assert.equal(hasRetryBudget(0, 0), true);
});

test("a missing or non-finite attempt count counts as zero", () => {
	assert.equal(hasRetryBudget(undefined, 5), true);
	assert.equal(hasRetryBudget(null, 1), true);
	assert.equal(hasRetryBudget(Number.NaN, 1), true);
	assert.equal(hasRetryBudget(Number.POSITIVE_INFINITY, 1), true);
});
