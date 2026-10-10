#!/usr/bin/env node
import { readdirSync, readFileSync, realpathSync } from "node:fs";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import ts from "typescript";

const root = resolve(import.meta.dirname, "..");
const localesRoot = join(root, "src/shared/locales");

const FEATURE_KEYS = [
	"app.check-for-updates",
	"app.check-updates-now",
	"app.update-checking",
	"app.update-unavailable",
	"app.update-available",
	"app.update-download",
	"app.update-cancelled",
	"app.update-install",
	"app.update-relaunch",
	"app.update-progress",
	"app.update-error",
	"app.update-unsigned",
	"app.open-url",
	"app.open-url-failed",
	"app.increase-value",
	"app.decrease-value",
	"app.previous-month",
	"app.next-month",
	"app.browse",
	"about.tagline",
	"about.features",
	"about.check-updates",
	"about.copy-info",
	"about.copied",
	"about.copy-failed",
	"task.edit-task",
	"task.edit-dialog-title",
	"task.edit-confirm",
	"task.edit-success",
	"task.edit-fail",
	"task.edit-no-changes",
	"task.edit-restart-warning",
	"task.edit-progress-warning",
	"health.inspect-logs",
	"health.logs-title",
	"health.logs-file",
	"health.logs-level",
	"health.logs-all-levels",
	"health.logs-search",
	"health.logs-search-placeholder",
	"health.logs-refresh",
	"health.logs-loading",
	"health.logs-no-files",
	"health.logs-empty",
	"health.logs-error",
	"health.logs-truncated",
	"health.log-levels.trace",
	"health.log-levels.debug",
	"health.log-levels.info",
	"health.log-levels.warn",
	"health.log-levels.error",
	"health.log-levels.unknown",
	"task.filter-video",
	"task.filter-audio",
	"task.filter-image",
	"task.filter-document",
	"task.clear-filter",
	"task.open-source-url",
	"task.open-error-code-reference",
	"task.more-actions",
	"task.batch-remove-item",
	"task.torrent-preview-folder-toggle",
	"task.sort-ascending",
	"task.sort-descending",
	"task.torrent-preview-folder-select",
	"task.unknown-task-type",
	"preferences.history-directories",
	"preferences.favorite-directory",
	"preferences.unfavorite-directory",
	"preferences.remove-history-directory",
	"preferences.proxy-bypass-input-tips",
	"preferences.proxy-scope-label",
	"preferences.proxy-scope-download",
	"preferences.proxy-scope-download-desc",
	"preferences.proxy-scope-update-app",
	"preferences.proxy-scope-update-app-desc",
	"preferences.proxy-scope-update-trackers",
	"preferences.proxy-scope-update-trackers-desc",
	"preferences.auto-update",
	"preferences.auto-check-update",
	"preferences.last-check-update-time",
	"preferences.randomize-port",
	"preferences.generate-rpc-secret",
	"preferences.ed2k-kad",
	"preferences.ed2k-enable-kad",
	"preferences.ed2k-enable-kad-tips",
	"preferences.ed2k-kad-port",
	"preferences.ed2k-kad-port-tips",
	"preferences.ed2k-kad-port-invalid",
	"preferences.engine-overrides-reserved-keys",
	"preferences.engine-overrides-too-large",
	"rss.clear-filter",
	"rss.filter-unread",
	"rss.filter-matched",
	"rss.sort-newest",
	"rss.sort-oldest",
	"rss.sort-size-desc",
	"rss.sort-rule-match",
	"rss.mark-all-read",
	"rss.density-compact",
	"rss.density-comfortable",
	"rss.select-all",
	"window.close",
];

const FEATURE_INTERPOLATIONS = new Map([
	["preferences.engine-overrides-reserved-keys", ["keys"]],
]);

const LOCALES = [
	"ar",
	"bg",
	"ca",
	"de",
	"el",
	"en-US",
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
	"pt-BR",
	"ro",
	"ru",
	"th",
	"tr",
	"uk",
	"vi",
	"zh-CN",
	"zh-TW",
];

function propertyName(property) {
	if (!property.name) return null;
	if (ts.isIdentifier(property.name)) return property.name.text;
	if (ts.isStringLiteral(property.name) || ts.isNumericLiteral(property.name)) {
		return property.name.text;
	}
	return null;
}

function collectObject(object, prefix, keys, values) {
	for (const property of object.properties) {
		if (!ts.isPropertyAssignment(property) && !ts.isShorthandPropertyAssignment(property)) {
			continue;
		}
		const name = propertyName(property);
		if (!name) continue;
		const path = prefix ? `${prefix}.${name}` : name;
		keys.add(path);
		if (ts.isPropertyAssignment(property)) {
			if (ts.isObjectLiteralExpression(property.initializer)) {
				collectObject(property.initializer, path, keys, values);
			} else if (
				ts.isStringLiteral(property.initializer) ||
				ts.isNoSubstitutionTemplateLiteral(property.initializer)
			) {
				values.set(path, property.initializer.text);
			}
		}
	}
}

function collectLocaleKeys(locale) {
	const keys = new Set();
	const values = new Map();
	const dir = join(localesRoot, locale);
	for (const file of readdirSync(dir).filter((name) => name.endsWith(".ts"))) {
		const source = ts.createSourceFile(
			file,
			readFileSync(join(dir, file), "utf8"),
			ts.ScriptTarget.Latest,
			true,
		);
		for (const statement of source.statements) {
			if (!ts.isExportAssignment(statement)) continue;
			const expression = statement.expression;
			if (ts.isObjectLiteralExpression(expression)) {
				collectObject(expression, file.slice(0, -3), keys, values);
			}
		}
	}
	return { keys, values };
}

export function validateFeatureInterpolations(locale, keys, values) {
	const invalidInterpolations = [];
	for (const [key, variables] of FEATURE_INTERPOLATIONS) {
		if (!keys.has(key)) continue;
		const value = values.get(key);
		if (value === undefined) {
			invalidInterpolations.push(
				`${locale}: ${key} must be a string literal containing ${variables
					.map((variable) => `{{${variable}}}`)
					.join(", ")}`,
			);
			continue;
		}
		for (const variable of variables) {
			if (!value.includes(`{{${variable}}}`)) {
				invalidInterpolations.push(`${locale}: ${key} must include {{${variable}}}`);
			}
		}
	}
	return invalidInterpolations;
}

function main() {
	const missing = [];
	const invalidInterpolations = [];
	for (const locale of LOCALES) {
		const { keys, values } = collectLocaleKeys(locale);
		for (const required of FEATURE_KEYS) {
			const [namespace, ...parts] = required.split(".");
			const localKey = `${namespace}.${parts.join(".")}`;
			if (!keys.has(localKey)) missing.push(`${locale}: ${required}`);
		}
		invalidInterpolations.push(...validateFeatureInterpolations(locale, keys, values));
	}

	if (missing.length > 0) {
		console.error(`Missing ${missing.length} required translation key(s):`);
		for (const item of missing) console.error(`- ${item}`);
		process.exit(1);
	}

	if (invalidInterpolations.length > 0) {
		console.error(
			`Invalid interpolation in ${invalidInterpolations.length} translation value(s):`,
		);
		for (const item of invalidInterpolations) console.error(`- ${item}`);
		process.exit(1);
	}

	console.log(`i18n parity OK: ${LOCALES.length} locales, ${FEATURE_KEYS.length} feature keys`);
}

export function isDirectExecution(entryPath = process.argv[1]) {
	if (!entryPath) return false;
	try {
		return realpathSync(entryPath) === realpathSync(fileURLToPath(import.meta.url));
	} catch (error) {
		throw new Error(`Unable to resolve i18n checker entry path: ${entryPath}`, {
			cause: error,
		});
	}
}

if (isDirectExecution()) {
	main();
}
