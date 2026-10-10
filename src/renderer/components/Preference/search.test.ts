import assert from "node:assert/strict";
import { existsSync, readdirSync, readFileSync } from "node:fs";
import { test } from "node:test";
import ts from "typescript";
import {
	buildPreferenceSearchEntries,
	filterPreferenceSearchEntries,
	getRouteForKey,
	normalizeSearchText,
	resolvePreferenceSearchTarget,
} from "./search.ts";

const LOCALES_ROOT = new URL("../../../shared/locales/", import.meta.url);
const SEARCH_NAMESPACES = ["preferences", "cloudSinks", "sync"] as const;
const SEARCH_ENVIRONMENT = { android: false, macOS: true, renderer: true };

type LocaleBundle = Record<string, Record<string, unknown>>;

function propertyName(property: ts.ObjectLiteralElementLike): string | null {
	if (!property.name) {
		return null;
	}
	if (ts.isIdentifier(property.name)) {
		return property.name.text;
	}
	if (ts.isStringLiteral(property.name) || ts.isNumericLiteral(property.name)) {
		return property.name.text;
	}
	return null;
}

function parseTranslationValue(node: ts.Expression): unknown {
	if (ts.isStringLiteral(node) || ts.isNoSubstitutionTemplateLiteral(node)) {
		return node.text;
	}
	if (ts.isNumericLiteral(node)) {
		return Number(node.text);
	}
	if (node.kind === ts.SyntaxKind.TrueKeyword) {
		return true;
	}
	if (node.kind === ts.SyntaxKind.FalseKeyword) {
		return false;
	}
	if (ts.isObjectLiteralExpression(node)) {
		return parseTranslationObject(node);
	}
	throw new Error(`Unsupported translation value: ${node.getText()}`);
}

function parseTranslationObject(
	node: ts.ObjectLiteralExpression,
): Record<string, unknown> {
	const result: Record<string, unknown> = {};
	for (const property of node.properties) {
		if (!ts.isPropertyAssignment(property)) {
			continue;
		}
		const key = propertyName(property);
		if (key) {
			result[key] = parseTranslationValue(property.initializer);
		}
	}
	return result;
}

function readLocaleNamespace(locale: string, namespace: string) {
	const file = new URL(`${locale}/${namespace}.ts`, LOCALES_ROOT);
	if (!existsSync(file)) {
		return {};
	}
	const source = ts.createSourceFile(
		file.href,
		readFileSync(file, "utf8"),
		ts.ScriptTarget.Latest,
		true,
	);
	const assignment = source.statements.find(ts.isExportAssignment);
	assert.ok(assignment && ts.isObjectLiteralExpression(assignment.expression));
	return parseTranslationObject(assignment.expression);
}

function readLocaleBundle(locale: string): LocaleBundle {
	return Object.fromEntries(
		SEARCH_NAMESPACES.map((namespace) => [
			namespace,
			readLocaleNamespace(locale, namespace),
		]),
	);
}

function translationFor(
	bundle: LocaleBundle,
	fallbackBundle: LocaleBundle,
	key: string,
): string | undefined {
	for (const candidate of [bundle, fallbackBundle]) {
		const value = key.split(".").reduce<unknown>((current, part) => {
			if (!current || typeof current !== "object") {
				return undefined;
			}
			return (current as Record<string, unknown>)[part];
		}, candidate);
		if (typeof value === "string") {
			return value;
		}
	}
	return undefined;
}

const fallback = {
	preferences: {
		"enable-proxy": "Enable proxy",
		"bt-listen-v6": "Listen on IPv6",
		"bt-create-subfolder": "Create a subfolder for multi-file torrents",
		"doh-url": "Endpoint URL",
		"speed-limit-enabled": "Enable speed limits",
		"gift-enabled": "Enable giFT delegation",
		"gift-port": "giFT Port",
	},
	cloudSinks: {
		s3Bucket: "Bucket",
	},
	sync: {
		"cloud-sync-auto": "Auto-sync",
	},
};

const translations = {
	"preferences.enable-proxy": "Proxy einschalten",
	"preferences.bt-listen-v6": "Listen on IPv6",
	"preferences.bt-create-subfolder":
		"Create a subfolder for multi-file torrents",
	"preferences.doh-url": "Endpoint URL",
	"preferences.speed-limit-enabled": "Enable speed limits",
	"cloudSinks.s3Bucket": "Bucket",
	"sync.cloud-sync-auto": "Auto-sync",
};

test("routes settings to the preference tab that owns them", () => {
	assert.equal(getRouteForKey("preferences.enable-proxy"), "advanced");
	assert.equal(getRouteForKey("preferences.bt-create-subfolder"), "advanced");
	assert.equal(getRouteForKey("preferences.theme-dark"), "appearance");
	assert.equal(getRouteForKey("preferences.usenet-max-entries"), "usenet");
	assert.equal(getRouteForKey("cloudSinks.s3Bucket"), "cloud-sinks");
	assert.equal(getRouteForKey("sync.cloud-sync-auto"), "sync");
	assert.equal(getRouteForKey("share.tools"), null);
	assert.equal(getRouteForKey("preferences.speed-limit-enabled"), "basic");
});

test("searches translated labels, translation keys, and fallback settings", () => {
	const entries = buildPreferenceSearchEntries(
		{ preferences: { "enable-proxy": "Proxy einschalten" } },
		fallback,
		(key) => translations[key as keyof typeof translations] || key,
	);

	assert.equal(
		filterPreferenceSearchEntries(entries, "proxy")[0]?.route,
		"advanced",
	);
	assert.equal(
		filterPreferenceSearchEntries(entries, "subfolder")[0]?.route,
		"advanced",
	);
	assert.equal(
		filterPreferenceSearchEntries(entries, "doh endpoint")[0]?.key,
		"preferences.doh-url",
	);
	assert.equal(
		filterPreferenceSearchEntries(entries, "bucket")[0]?.route,
		"cloud-sinks",
	);
	assert.equal(
		filterPreferenceSearchEntries(entries, "auto sync")[0]?.route,
		"sync",
	);
	assert.equal(
		filterPreferenceSearchEntries(entries, "ipv6")[0]?.key,
		"preferences.bt-listen-v6",
	);
});

test("gives conditional and dialog settings a stable visible fallback target", () => {
	const entries = buildPreferenceSearchEntries(
		fallback,
		fallback,
		(key) => translations[key as keyof typeof translations] || key,
	);

	assert.equal(
		entries.find((entry) => entry.key === "preferences.doh-url")?.target,
		"preferences.doh-enable",
	);
	assert.equal(
		entries.find((entry) => entry.key === "cloudSinks.s3Bucket")?.target,
		"cloudSinks.sinks",
	);
	const gift = filterPreferenceSearchEntries(entries, "gift");
	assert.ok(gift.length > 0);
	assert.ok(gift.every((entry) => entry.route === "advanced"));
	assert.equal(
		entries.find((entry) => entry.key === "preferences.gift-port")?.target,
		"preferences.gift-enabled",
	);
});

test("filters platform-only controls and does not silently cap matching results", () => {
	const platformLabels = {
		preferences: {
			"run-mode": "Run As",
			"font-family": "Font",
			"usenet-desktop-limits": "Desktop limits",
			"usenet-android-limits": "Android limits",
		},
	};
	const translate = (key: string) => {
		const value = key.split(".").reduce<unknown>((current, part) => {
			if (!current || typeof current !== "object") {
				return undefined;
			}
			return (current as Record<string, unknown>)[part];
		}, platformLabels);
		return typeof value === "string" ? value : key;
	};
	const androidEntries = buildPreferenceSearchEntries(
		platformLabels,
		platformLabels,
		translate,
		{ android: true, renderer: true },
	);
	const desktopEntries = buildPreferenceSearchEntries(
		platformLabels,
		platformLabels,
		translate,
		{ android: false, renderer: true },
	);

	assert.equal(
		androidEntries.some((entry) => entry.key === "preferences.font-family"),
		false,
	);
	assert.equal(
		androidEntries.some(
			(entry) => entry.key === "preferences.usenet-android-limits",
		),
		true,
	);
	assert.equal(
		desktopEntries.some(
			(entry) => entry.key === "preferences.usenet-desktop-limits",
		),
		true,
	);

	const matchingEntries = Array.from({ length: 100 }, (_, index) => ({
		key: `preferences.setting-${index}`,
		label: `Setting ${index}`,
		route: "basic" as const,
		target: `preferences.setting-${index}`,
		searchText: `setting ${index}`,
	}));
	assert.equal(
		filterPreferenceSearchEntries(matchingEntries, "setting").length,
		100,
	);
});

test("prefers a visible setting over a grouped fallback target", () => {
	const groupedTarget = { id: "auto-retry-row" };
	const intervalTarget = { id: "auto-retry-interval" };

	assert.equal(
		resolvePreferenceSearchTarget(
			"preferences.auto-retry-interval",
			"Auto Retry Interval",
			[
				{
					target: groupedTarget,
					keys: ["preferences.auto-retry", "preferences.auto-retry-interval"],
				},
			],
			[
				{
					target: intervalTarget,
					text: "Auto Retry Interval (seconds)",
				},
			],
		),
		intervalTarget,
	);
	assert.equal(
		resolvePreferenceSearchTarget(
			"preferences.auto-retry-interval",
			"Auto Retry Interval",
			[
				{
					target: groupedTarget,
					keys: ["preferences.auto-retry", "preferences.auto-retry-interval"],
				},
			],
			[],
		),
		groupedTarget,
	);
});

test("searches every bundled locale by its visible and English fallback labels", () => {
	const englishBundle = readLocaleBundle("en-US");
	const englishEntries = buildPreferenceSearchEntries(
		englishBundle,
		englishBundle,
		(key) => translationFor(englishBundle, englishBundle, key) || key,
		SEARCH_ENVIRONMENT,
	);
	const expectedKeys = new Set(englishEntries.map((entry) => entry.key));
	const locales = readdirSync(LOCALES_ROOT, { withFileTypes: true })
		.filter((entry) => entry.isDirectory())
		.map((entry) => entry.name)
		.sort();

	for (const locale of locales) {
		const localeBundle = readLocaleBundle(locale);
		const entries = buildPreferenceSearchEntries(
			localeBundle,
			englishBundle,
			(key) => translationFor(localeBundle, englishBundle, key) || key,
			SEARCH_ENVIRONMENT,
		);
		assert.deepEqual(
			new Set(entries.map((entry) => entry.key)),
			expectedKeys,
			`${locale} exposes the complete preference search catalog`,
		);

		for (const entry of entries) {
			assert.ok(
				filterPreferenceSearchEntries(entries, entry.label).some(
					(result) => result.key === entry.key,
				),
				`${locale} searches its visible label for ${entry.key}`,
			);
		}
		for (const englishEntry of englishEntries) {
			assert.ok(
				filterPreferenceSearchEntries(entries, englishEntry.label).some(
					(result) => result.key === englishEntry.key,
				),
				`${locale} searches the English fallback label for ${englishEntry.key}`,
			);
		}
	}
});

test("normalizes composed, marked, and non-Latin search text consistently", () => {
	assert.equal(normalizeSearchText("Café"), normalizeSearchText("Cafe"));
	assert.equal(normalizeSearchText("İndirme"), normalizeSearchText("indirme"));
	assert.equal(normalizeSearchText("Işık"), normalizeSearchText("ışık"));
	assert.equal(normalizeSearchText("Größe"), normalizeSearchText("GROSSE"));
	assert.equal(normalizeSearchText("ภาษาไทย"), normalizeSearchText("ภาษาไทย"));
	assert.equal(
		normalizeSearchText("简体中文"),
		normalizeSearchText("简体中文"),
	);
});
