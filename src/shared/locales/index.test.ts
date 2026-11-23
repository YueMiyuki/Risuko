import assert from "node:assert/strict";
import { test } from "node:test";
import { getLanguage, pickLanguage } from "./index.ts";

test("exact matches are returned unchanged", () => {
	for (const tag of ["ja", "pt-BR", "zh-TW", "en-US"]) {
		assert.equal(getLanguage(tag), tag);
	}
});

test("region tags resolve to the base language of every shipped locale", () => {
	const codes = [
		"ar",
		"bg",
		"ca",
		"de",
		"el",
		"es",
		"fa",
		"fr",
		"hu",
		"id",
		"it",
		"ja",
		"ko",
		"nb",
		"nl",
		"pl",
		"ro",
		"ru",
		"th",
		"tr",
		"uk",
		"vi",
	];
	for (const code of codes) {
		assert.equal(getLanguage(`${code}-XX`), code);
	}
});

test("english and portuguese fall back to their default regions", () => {
	assert.equal(getLanguage("en"), "en-US");
	assert.equal(getLanguage("en-GB"), "en-US");
	assert.equal(getLanguage("pt"), "pt-BR");
	assert.equal(getLanguage("pt-PT"), "pt-BR");
});

test("chinese tags pick simplified or traditional by script and region", () => {
	for (const tag of ["zh", "zh-Hans", "zh-Hans-CN", "zh-SG"]) {
		assert.equal(getLanguage(tag), "zh-CN");
	}
	for (const tag of [
		"zh-TW",
		"zh-HK",
		"zh-MO",
		"zh-Hant",
		"zh-Hant-HK",
		"zh-Hant-TW",
	]) {
		assert.equal(getLanguage(tag), "zh-TW");
	}
});

test("norwegian tags resolve to bokmal", () => {
	for (const tag of ["no", "no-NO", "nb-NO"]) {
		assert.equal(getLanguage(tag), "nb");
	}
});

test("letter case and underscores are tolerated", () => {
	assert.equal(getLanguage("JA-jp"), "ja");
	assert.equal(getLanguage("pt_BR"), "pt-BR");
});

test("unknown or malformed input gives english", () => {
	for (const input of [
		"xx",
		"xx-YY",
		"",
		undefined,
		null,
		42,
		"constructor-XX",
	]) {
		assert.equal(getLanguage(input), "en-US");
	}
});

test("the first system language that maps to a shipped locale wins", () => {
	assert.equal(pickLanguage(["xx-YY", "ja-JP", "en-US"]), "ja");
	assert.equal(pickLanguage(["en-GB", "ja-JP"]), "en-US");
	assert.equal(pickLanguage([]), "en-US");
	assert.equal(pickLanguage(["xx"]), "en-US");
	assert.equal(pickLanguage(["auto", "de-AT"]), "de");
});
