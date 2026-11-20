import assert from "node:assert/strict";
import {
	mkdirSync,
	mkdtempSync,
	readdirSync,
	readFileSync,
	rmSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { loadLocaleObjects } from "./check-locales.mjs";
import {
	applyLocale,
	blankLineKeys,
	exportLocale,
	isPlainAreaSource,
	missingTranslations,
	parseAreaOrder,
	renderArea,
	renderIndex,
	validateTranslations,
} from "./locale-tool.mjs";

const REFERENCE_FILES = {
	"index.ts": `import task from "./task";
import app from "./app";
import share from "./share";

export default {
	task,
	app,
	share,
};
`,
	"task.ts": `export default {
	start: "Start",
	status: {
		active: "Active",

		done: "Done",
	},

	count: "{{count}} tasks",
};
`,
	"app.ts": `export default {
	yes: "Yes",
	no: "No",

	"quit-app": "Quit {{name}}",
};
`,
	"share.ts": `export default {
	title: "Share",
};
`,
};

const FRENCH_FILES = {
	"index.ts": `import app from "./app";
import task from "./task";

export default {
	app,
	task,
};
`,
	"app.ts": `export default {
	no: "Non",
	yes: "Oui",
};
`,
	"task.ts": `export default {
	status: {
		done: "Terminé",
	},
	start: "Démarrer",
};
`,
};

function makeLocales(overrides = {}) {
	const root = mkdtempSync(join(tmpdir(), "locale-tool-"));
	const locales = { "en-US": REFERENCE_FILES, fr: FRENCH_FILES, ...overrides };
	for (const [tag, files] of Object.entries(locales)) {
		mkdirSync(join(root, tag));
		for (const [name, content] of Object.entries(files)) {
			writeFileSync(join(root, tag, name), content);
		}
	}
	return root;
}

function snapshot(dir) {
	return Object.fromEntries(
		readdirSync(dir)
			.sort()
			.map((name) => [name, readFileSync(join(dir, name), "utf8")]),
	);
}

async function withLocales(overrides, run) {
	const root = makeLocales(overrides);
	try {
		await run(root);
	} finally {
		rmSync(root, { recursive: true, force: true });
	}
}

async function objectsOf(dir) {
	return Object.fromEntries(await loadLocaleObjects(dir));
}

test("the area order comes from the import lines of index.ts", () => {
	assert.deepEqual(parseAreaOrder(REFERENCE_FILES["index.ts"]), [
		"task",
		"app",
		"share",
	]);
});

test("an area renders as a tab-indented default export with JSON quoting", () => {
	assert.equal(
		renderArea({ plain: 'Say "hi"', nested: { inner: "a\\b" } }),
		'export default {\n\t"plain": "Say \\"hi\\"",\n\t"nested": {\n\t\t"inner": "a\\\\b"\n\t}\n};\n',
	);
	assert.equal(renderArea({}), "export default {};\n");
});

test("keys after a blank line in the source are found by their dotted path", () => {
	assert.deepEqual(
		[...blankLineKeys(REFERENCE_FILES["task.ts"])],
		["status.done", "count"],
	);
	assert.deepEqual([...blankLineKeys(REFERENCE_FILES["app.ts"])], ["quit-app"]);
});

test("an area keeps a blank line before the given keys but never before the first", () => {
	assert.equal(
		renderArea(
			{ a: "1", b: { c: "2", d: "3" } },
			new Set(["a", "b", "b.c", "b.d"]),
		),
		'export default {\n\t"a": "1",\n\n\t"b": {\n\t\t"c": "2",\n\n\t\t"d": "3"\n\t}\n};\n',
	);
});

test("the index imports and exports the given areas in order", () => {
	assert.equal(
		renderIndex(["task", "app"]),
		'import task from "./task";\nimport app from "./app";\n\nexport default {\n\ttask,\n\tapp,\n};\n',
	);
});

test("only a plain object of strings counts as a plain area file", () => {
	assert.ok(isPlainAreaSource(REFERENCE_FILES["task.ts"]));
	assert.ok(
		isPlainAreaSource(
			`export default {\n\turl: "https://example.com/a",\n\tquote: 'It\\'s "here"',\n\tpath: "C:\\\\Tools",\n};\n`,
		),
	);
	assert.ok(!isPlainAreaSource('export default {\n\t// note\n\ta: "b",\n};\n'));
	assert.ok(!isPlainAreaSource('export default {\n\ta: "b", /* note */\n};\n'));
	assert.ok(
		!isPlainAreaSource(
			'import shared from "./shared";\n\nexport default {\n\ta: shared,\n};\n',
		),
	);
	assert.ok(!isPlainAreaSource("export default {\n\ta: `b`,\n};\n"));
	assert.ok(!isPlainAreaSource('export default {\n\ta: "b" + "c",\n};\n'));
});

test("export lists missing keys in reference order with nested keys and missing areas", async () => {
	await withLocales({}, async (root) => {
		const missing = await exportLocale(root, "fr");
		assert.deepEqual(Object.entries(missing), [
			["task:status.active", "Active"],
			["task:count", "{{count}} tasks"],
			["app:quit-app", "Quit {{name}}"],
			["share:title", "Share"],
		]);
	});
});

test("missing translations come out in reference order", () => {
	const reference = new Map([
		[
			"b",
			new Map([
				["z", "Z"],
				["y.x", "X"],
			]),
		],
		["a", new Map([["k", "K"]])],
	]);
	const locale = new Map([["b", new Set(["z"])]]);
	assert.deepEqual(Object.entries(missingTranslations(reference, locale)), [
		["b:y.x", "X"],
		["a:k", "K"],
	]);
});

test("apply merges translations in English key order, creates the missing area and updates index.ts", async () => {
	await withLocales({}, async (root) => {
		const result = await applyLocale(root, "fr", {
			"task:status.active": "Actif",
			"app:quit-app": "Quitter {{name}}",
			"share:title": "Partager",
		});
		assert.deepEqual(result.problems, []);
		assert.equal(result.applied, 3);
		const dir = join(root, "fr");
		assert.deepEqual(Object.keys(snapshot(dir)), [
			"app.ts",
			"index.ts",
			"share.ts",
			"task.ts",
		]);
		assert.equal(
			readFileSync(join(dir, "task.ts"), "utf8"),
			'export default {\n\t"start": "Démarrer",\n\t"status": {\n\t\t"active": "Actif",\n\n\t\t"done": "Terminé"\n\t}\n};\n',
		);
		assert.equal(
			readFileSync(join(dir, "app.ts"), "utf8"),
			'export default {\n\t"yes": "Oui",\n\t"no": "Non",\n\n\t"quit-app": "Quitter {{name}}"\n};\n',
		);
		assert.equal(
			readFileSync(join(dir, "index.ts"), "utf8"),
			renderIndex(["task", "app", "share"]),
		);
		const objects = await objectsOf(dir);
		assert.deepEqual(objects, {
			app: { yes: "Oui", no: "Non", "quit-app": "Quitter {{name}}" },
			share: { title: "Partager" },
			task: { start: "Démarrer", status: { active: "Actif", done: "Terminé" } },
		});
		assert.deepEqual(Object.keys(objects.app), ["yes", "no", "quit-app"]);
	});
});

test("apply keeps existing translations unless told to overwrite", async () => {
	await withLocales({}, async (root) => {
		const kept = await applyLocale(root, "fr", {
			"app:yes": "Ouais",
			"task:count": "{{count}} tâches",
		});
		assert.deepEqual(kept.problems, []);
		assert.equal(kept.applied, 1);
		assert.equal(kept.kept, 1);
		let objects = await objectsOf(join(root, "fr"));
		assert.equal(objects.app.yes, "Oui");
		assert.equal(objects.task.count, "{{count}} tâches");

		const replaced = await applyLocale(
			root,
			"fr",
			{ "app:yes": "Ouais" },
			{ overwrite: true },
		);
		assert.deepEqual(replaced.problems, []);
		assert.equal(replaced.applied, 1);
		assert.equal(replaced.kept, 0);
		objects = await objectsOf(join(root, "fr"));
		assert.equal(objects.app.yes, "Ouais");
		assert.equal(objects.app.no, "Non");
	});
});

test("apply refuses unknown keys, empty values and placeholder mismatches without writing", async () => {
	await withLocales({}, async (root) => {
		const dir = join(root, "fr");
		const before = snapshot(dir);
		const result = await applyLocale(root, "fr", {
			"app:nope": "x",
			"task:status": "x",
			"no-colon": "x",
			"app:yes": "",
			"app:no": "   ",
			"task:start": 3,
			"app:quit-app": "Quitter",
			"task:count": "{{nombre}} tâches",
			"share:title": "Partager",
		});
		assert.deepEqual(result.problems, [
			"app:nope: not a key in en-US",
			"task:status: not a key in en-US",
			"no-colon: not a key in en-US",
			"app:yes: must be a non-empty string",
			"app:no: must be a non-empty string",
			"task:start: must be a non-empty string",
			"app:quit-app: placeholders differ (en-US: name; translation: none)",
			"task:count: placeholders differ (en-US: count; translation: nombre)",
		]);
		assert.deepEqual(snapshot(dir), before);
	});
});

test("apply refuses a translations file that is not an object", async () => {
	await withLocales({}, async (root) => {
		const before = snapshot(join(root, "fr"));
		const result = await applyLocale(root, "fr", ["app:yes", "Oui"]);
		assert.deepEqual(result.problems, [
			"the translations file must hold one JSON object",
		]);
		assert.deepEqual(snapshot(join(root, "fr")), before);
	});
});

test("apply refuses a locale with keys or files it would not regenerate", async () => {
	await withLocales(
		{
			fr: {
				...FRENCH_FILES,
				"app.ts": 'export default {\n\tyes: "Oui",\n\told: "Vieux",\n};\n',
				"task.ts": 'export default {\n\t// keep\n\tstart: "Démarrer",\n};\n',
				"extra.ts": "export default {};\n",
			},
		},
		async (root) => {
			const dir = join(root, "fr");
			const before = snapshot(dir);
			const result = await applyLocale(root, "fr", {
				"share:title": "Partager",
			});
			assert.deepEqual(result.problems, [
				"fr/extra.ts: not an en-US area",
				"fr/task.ts: not a plain object of strings; edit it by hand",
				"fr: app:old is not in en-US",
			]);
			assert.deepEqual(snapshot(dir), before);
		},
	);
});

test("apply refuses a locale whose index.ts holds more than the area imports", async () => {
	await withLocales(
		{
			fr: {
				...FRENCH_FILES,
				"index.ts": `${FRENCH_FILES["index.ts"]}export const rtl = true;\n`,
			},
		},
		async (root) => {
			const dir = join(root, "fr");
			const before = snapshot(dir);
			const result = await applyLocale(root, "fr", {});
			assert.deepEqual(result.problems, [
				"fr/index.ts: not a plain list of area imports; edit it by hand",
			]);
			assert.deepEqual(snapshot(dir), before);
		},
	);
});

test("apply refuses the reference and unknown locales", async () => {
	await withLocales({}, async (root) => {
		const before = snapshot(join(root, "en-US"));
		assert.deepEqual((await applyLocale(root, "en-US", {})).problems, [
			"en-US is the reference locale",
		]);
		assert.deepEqual((await applyLocale(root, "xx", {})).problems, [
			"xx: no such locale",
		]);
		assert.deepEqual(snapshot(join(root, "en-US")), before);
		await assert.rejects(exportLocale(root, "../fr"), /no such locale/);
	});
});

test("apply with nothing to add only puts keys into English order", async () => {
	await withLocales({}, async (root) => {
		const dir = join(root, "fr");
		const before = await objectsOf(dir);
		const result = await applyLocale(root, "fr", {});
		assert.deepEqual(result.problems, []);
		assert.equal(result.applied, 0);
		const after = await objectsOf(dir);
		assert.deepEqual(after, before);
		assert.deepEqual(Object.keys(after.app), ["yes", "no"]);
		assert.deepEqual(Object.keys(after.task), ["start", "status"]);
		assert.deepEqual(Object.keys(snapshot(dir)), [
			"app.ts",
			"index.ts",
			"task.ts",
		]);
		assert.equal(
			readFileSync(join(dir, "index.ts"), "utf8"),
			renderIndex(["task", "app"]),
		);
	});
});

test("export, translate, apply and export again leaves nothing missing", async () => {
	await withLocales({}, async (root) => {
		const missing = await exportLocale(root, "fr");
		const translated = Object.fromEntries(
			Object.entries(missing).map(([key, text]) => [key, `FR ${text}`]),
		);
		const result = await applyLocale(root, "fr", translated);
		assert.deepEqual(result.problems, []);
		assert.equal(result.applied, 4);
		assert.deepEqual(await exportLocale(root, "fr"), {});
		const objects = await objectsOf(join(root, "fr"));
		assert.equal(objects.task.count, "FR {{count}} tasks");
	});
});

test("validation reports every offending key against the reference texts", () => {
	const reference = new Map([["app", new Map([["a", "{{x}} and {{y}}"]])]]);
	assert.deepEqual(
		validateTranslations(reference, {
			"app:a": "{{y}} et {{x}}",
		}),
		[],
	);
	assert.deepEqual(
		validateTranslations(reference, { "app:a": "{{x}}", "app:b": "b" }),
		[
			"app:a: placeholders differ (en-US: x, y; translation: x)",
			"app:b: not a key in en-US",
		],
	);
});
