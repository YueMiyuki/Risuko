import assert from "node:assert/strict";
import { test } from "node:test";
import {
	checkLocales,
	compareLocale,
	flattenEntries,
	flattenKeys,
	placeholders,
} from "./check-locales.mjs";

function areas(object) {
	return new Map(
		Object.entries(object).map(([area, keys]) => [area, new Set(keys)]),
	);
}

function texts(object) {
	return new Map(
		Object.entries(object).map(([area, entries]) => [
			area,
			new Map(Object.entries(entries)),
		]),
	);
}

function keysOf(textAreas) {
	return new Map(
		[...textAreas].map(([area, entries]) => [area, new Set(entries.keys())]),
	);
}

const reference = areas({ app: ["a", "b", "c"], task: ["x", "y"] });

test("nested objects contribute dotted paths and other values are leaves", () => {
	assert.deepEqual(
		flattenKeys({
			plain: "text",
			outer: { inner: "text", deeper: { leaf: "text" } },
			list: ["one", "two"],
			nothing: null,
		}),
		["plain", "outer.inner", "outer.deeper.leaf", "list", "nothing"],
	);
	assert.deepEqual(flattenKeys({ inner: "text" }, "outer"), ["outer.inner"]);
});

test("keys the locale lacks are reported as missing", () => {
	const locale = areas({ app: ["a", "c"], task: ["y", "x"] });
	assert.deepEqual(compareLocale(reference, locale), {
		missing: ["app:b"],
		extra: [],
	});
});

test("a whole area absent from the locale counts every key as missing", () => {
	const locale = areas({ app: ["a", "b", "c"] });
	assert.deepEqual(compareLocale(reference, locale), {
		missing: ["task:x", "task:y"],
		extra: [],
	});
});

test("keys the reference lacks are reported as extra", () => {
	const locale = areas({ app: ["a", "b", "c", "z"], task: ["x", "y", "w"] });
	assert.deepEqual(compareLocale(reference, locale), {
		missing: [],
		extra: ["app:z", "task:w"],
	});
});

test("an area the reference lacks counts every key as extra", () => {
	const locale = areas({
		app: ["a", "b", "c"],
		task: ["x", "y"],
		share: ["q", "p"],
	});
	assert.deepEqual(compareLocale(reference, locale), {
		missing: [],
		extra: ["share:p", "share:q"],
	});
});

test("a locale at its baseline passes", () => {
	const locales = new Map([["zh-CN", areas({ app: ["a"], task: ["x", "y"] })]]);
	const { rows, failures } = checkLocales({
		reference,
		locales,
		baseline: { "zh-CN": 2 },
	});
	assert.deepEqual(failures, []);
	assert.equal(rows.length, 1);
	assert.equal(rows[0].tag, "zh-CN");
	assert.equal(rows[0].missing, 2);
	assert.equal(rows[0].extra, 0);
});

test("a locale above its baseline fails", () => {
	const locales = new Map([["zh-CN", areas({ app: ["a"], task: ["x"] })]]);
	const { failures } = checkLocales({
		reference,
		locales,
		baseline: { "zh-CN": 2 },
	});
	assert.deepEqual(failures, ["zh-CN: 3 missing key(s), baseline allows 2"]);
});

test("a locale below its baseline passes", () => {
	const locales = new Map([["zh-CN", areas({ app: ["a", "b"], task: ["x"] })]]);
	const { failures } = checkLocales({
		reference,
		locales,
		baseline: { "zh-CN": 2 },
	});
	assert.deepEqual(failures, []);
});

test("extra keys fail even without a baseline entry and only five are listed", () => {
	const stray = ["k1", "k2", "k3", "k4", "k5", "k6"];
	const locales = new Map([["fr", areas({ app: ["a", ...stray] })]]);
	const { failures } = checkLocales({ reference, locales, baseline: {} });
	assert.deepEqual(failures, [
		"fr: 6 key(s) not in en-US: app:k1, app:k2, app:k3, app:k4, app:k5",
	]);
});

test("locales missing keys without a baseline entry are reported but not enforced", () => {
	const locales = new Map([["fr", areas({ app: ["a"] })]]);
	const { rows, failures } = checkLocales({ reference, locales, baseline: {} });
	assert.deepEqual(failures, []);
	assert.equal(rows[0].missing, 4);
});

test("a baseline tag with no locale folder is reported", () => {
	const locales = new Map([["fr", areas({ app: ["a"] })]]);
	const { failures } = checkLocales({
		reference,
		locales,
		baseline: { "zh-TW": 5 },
	});
	assert.deepEqual(failures, ["zh-TW: locale not found"]);
});

test("coverage is the share of reference keys present, with one decimal", () => {
	const locales = new Map([
		["full", areas({ app: ["a", "b", "c"], task: ["x", "y"] })],
		["twoFifths", areas({ app: ["a"], task: ["x"] })],
		["threeFifths", areas({ app: ["a", "b", "c"], task: [] })],
		["none", areas({})],
	]);
	const { rows } = checkLocales({ reference, locales, baseline: {} });
	const coverage = Object.fromEntries(
		rows.map((row) => [row.tag, row.coverage]),
	);
	assert.deepEqual(coverage, {
		full: 100,
		twoFifths: 40,
		threeFifths: 60,
		none: 0,
	});
	assert.ok(rows.every((row) => row.total === 5));
});

test("coverage is rounded to one decimal", () => {
	const locales = new Map([["fr", areas({ app: ["a"] })]]);
	const { rows } = checkLocales({
		reference: areas({ app: ["a", "b", "c"] }),
		locales,
		baseline: {},
	});
	assert.equal(rows[0].coverage, 33.3);
});

test("coverage ignores extra keys", () => {
	const locales = new Map([
		["fr", areas({ app: ["a", "b", "z"], task: ["x"] })],
	]);
	const { rows } = checkLocales({ reference, locales, baseline: {} });
	assert.equal(rows[0].coverage, 60);
	assert.equal(rows[0].missing, 2);
	assert.equal(rows[0].extra, 1);
});

test("nested entries keep their texts under dotted paths", () => {
	assert.deepEqual(flattenEntries({ plain: "one", outer: { inner: "two" } }), [
		["plain", "one"],
		["outer.inner", "two"],
	]);
});

test("a string without placeholders has none", () => {
	assert.deepEqual(placeholders("Plain text { not one } {{}}"), []);
	assert.deepEqual(placeholders(undefined), []);
});

test("a single placeholder is found", () => {
	assert.deepEqual(placeholders("Hello {{name}}!"), ["name"]);
});

test("repeated placeholders are listed once and sorted", () => {
	assert.deepEqual(
		placeholders("{{size}} of {{total}} ({{size}}) for {{name}}"),
		["name", "size", "total"],
	);
});

test("whitespace inside the braces is ignored", () => {
	assert.deepEqual(placeholders("{{ count }} left, {{count}} done {{\tb }}"), [
		"b",
		"count",
	]);
});

const referenceTexts = texts({
	app: { a: "Hello {{name}}", b: "{{done}} of {{total}}", c: "Plain" },
	task: { x: "{{count}} tasks" },
});

test("a translation whose placeholders differ from English fails", () => {
	const localeTexts = texts({
		app: { a: "Hallo {{nom}}", b: "{{done}} von", c: "Einfach {{extra}}" },
		task: { x: "{{count}} Aufgaben" },
	});
	const { failures } = checkLocales({
		reference: keysOf(referenceTexts),
		locales: new Map([["de", keysOf(localeTexts)]]),
		baseline: {},
		texts: {
			reference: referenceTexts,
			locales: new Map([["de", localeTexts]]),
		},
	});
	assert.deepEqual(failures, [
		"de: app:a placeholders differ (en-US: name; de: nom)",
		"de: app:b placeholders differ (en-US: done, total; de: done)",
		"de: app:c placeholders differ (en-US: none; de: extra)",
	]);
});

test("placeholders in a different order or repeated still match", () => {
	const localeTexts = texts({
		app: { a: "{{ name }}, {{name}}", b: "{{total}} : {{done}}" },
	});
	const { failures } = checkLocales({
		reference: keysOf(referenceTexts),
		locales: new Map([["de", keysOf(localeTexts)]]),
		baseline: {},
		texts: {
			reference: referenceTexts,
			locales: new Map([["de", localeTexts]]),
		},
	});
	assert.deepEqual(failures, []);
});

test("placeholders are checked for locales with a baseline too", () => {
	const localeTexts = texts({ task: { x: "{{anzahl}} Aufgaben" } });
	const { failures } = checkLocales({
		reference: keysOf(referenceTexts),
		locales: new Map([["zh-CN", keysOf(localeTexts)]]),
		baseline: { "zh-CN": 3 },
		texts: {
			reference: referenceTexts,
			locales: new Map([["zh-CN", localeTexts]]),
		},
	});
	assert.deepEqual(failures, [
		"zh-CN: task:x placeholders differ (en-US: count; zh-CN: anzahl)",
	]);
});
