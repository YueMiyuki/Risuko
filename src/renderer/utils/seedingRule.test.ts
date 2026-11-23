import assert from "node:assert/strict";
import { test } from "node:test";
import {
	globalSeedingSummary,
	type SeedingChoice,
	seedingChoiceOf,
	seedingRuleOptions,
} from "./seedingRule.ts";

test("each choice writes the three seeding options the engine reads", () => {
	assert.deepEqual(seedingRuleOptions("global", { ratio: 2, minutes: 30 }), {
		keepSeeding: null,
		seedTime: null,
		seedRatio: null,
	});
	assert.deepEqual(seedingRuleOptions("forever", { ratio: 2, minutes: 30 }), {
		keepSeeding: "true",
		seedTime: "0",
		seedRatio: "0",
	});
	assert.deepEqual(seedingRuleOptions("limits", { ratio: 1.5, minutes: 90 }), {
		keepSeeding: "true",
		seedTime: "90",
		seedRatio: "1.5",
	});
	assert.deepEqual(seedingRuleOptions("none", { ratio: 2, minutes: 30 }), {
		keepSeeding: "false",
		seedTime: "0",
		seedRatio: "0",
	});
});

test("a limit of only a ratio or only minutes is a valid rule", () => {
	assert.deepEqual(seedingRuleOptions("limits", { ratio: 2, minutes: 0 }), {
		keepSeeding: "true",
		seedTime: "0",
		seedRatio: "2",
	});
	assert.deepEqual(seedingRuleOptions("limits", { ratio: 0, minutes: 45 }), {
		keepSeeding: "true",
		seedTime: "45",
		seedRatio: "0",
	});
});

test("stopping at limits needs a ratio or minutes above 0", () => {
	assert.equal(seedingRuleOptions("limits", { ratio: 0, minutes: 0 }), null);
	assert.equal(seedingRuleOptions("limits", { ratio: -1, minutes: 0 }), null);
	assert.equal(
		seedingRuleOptions("limits", { ratio: Number.NaN, minutes: 0 }),
		null,
	);
});

test("minutes are written as a whole number the engine can parse", () => {
	assert.equal(
		seedingRuleOptions("limits", { ratio: 0, minutes: 12.6 })?.seedTime,
		"13",
	);
});

test("the options written for a choice read back as the same choice and values", () => {
	const cases: Array<[SeedingChoice, { ratio: number; minutes: number }]> = [
		["global", { ratio: 0, minutes: 0 }],
		["forever", { ratio: 0, minutes: 0 }],
		["limits", { ratio: 1.5, minutes: 0 }],
		["limits", { ratio: 0, minutes: 120 }],
		["limits", { ratio: 2, minutes: 525600 }],
		["none", { ratio: 0, minutes: 0 }],
	];
	for (const [choice, limits] of cases) {
		const written = seedingRuleOptions(choice, limits);
		assert.ok(written);
		const options = Object.fromEntries(
			Object.entries(written).filter(([, value]) => value !== null),
		);
		const state = seedingChoiceOf(options);
		assert.equal(state.choice, choice);
		assert.equal(state.ratio, limits.ratio);
		assert.equal(state.minutes, limits.minutes);
		assert.equal(state.inPlace, false);
		assert.equal(state.leftover, false);
	}
});

test("a task without its own switch follows the global setting", () => {
	assert.deepEqual(seedingChoiceOf({}), {
		choice: "global",
		ratio: 0,
		minutes: 0,
		inPlace: false,
		leftover: false,
	});
});

test("limits without a switch are leftovers of an earlier settings change", () => {
	const state = seedingChoiceOf({ seedTime: "30" });
	assert.equal(state.choice, "global");
	assert.equal(state.leftover, true);
	assert.equal(state.minutes, 30);
	assert.equal(seedingChoiceOf({ seedRatio: 0 }).leftover, true);
});

test("a limit above 0 means stopping at limits even with the switch off", () => {
	const state = seedingChoiceOf({ keepSeeding: "false", seedRatio: 2 });
	assert.equal(state.choice, "limits");
	assert.equal(state.ratio, 2);
	assert.equal(state.minutes, 0);
});

test("the switch is read from booleans, numbers and text", () => {
	assert.equal(seedingChoiceOf({ keepSeeding: true }).choice, "forever");
	assert.equal(seedingChoiceOf({ keepSeeding: "true" }).choice, "forever");
	assert.equal(seedingChoiceOf({ keepSeeding: 1 }).choice, "forever");
	assert.equal(
		seedingChoiceOf({ keepSeeding: false, seedTime: 0, seedRatio: "0" }).choice,
		"none",
	);
	assert.equal(seedingChoiceOf({ keepSeeding: "false" }).choice, "none");
	assert.equal(
		seedingChoiceOf({ keepSeeding: true, seedTime: "45", seedRatio: "0" })
			.minutes,
		45,
	);
});

test("a torrent seeded in place is marked so", () => {
	assert.equal(seedingChoiceOf({ risukoSeedInPlace: true }).inPlace, true);
	assert.equal(seedingChoiceOf({ risukoSeedInPlace: "true" }).inPlace, true);
	assert.equal(
		seedingChoiceOf({
			risukoSeedInPlace: true,
			keepSeeding: true,
			seedTime: 0,
			seedRatio: 0,
		}).choice,
		"forever",
	);
	assert.equal(seedingChoiceOf({ risukoSeedInPlace: false }).inPlace, false);
});

test("the global switch off without limits means no seeding", () => {
	assert.deepEqual(
		globalSeedingSummary({ keepSeeding: false, seedTime: 0, seedRatio: 0 }),
		{ kind: "off", ratio: 0, minutes: 0 },
	);
	assert.equal(globalSeedingSummary({}).kind, "off");
});

test("the global switch on with only minutes stops after those minutes", () => {
	assert.deepEqual(
		globalSeedingSummary({ keepSeeding: true, seedTime: 525600, seedRatio: 0 }),
		{ kind: "minutes", ratio: 0, minutes: 525600 },
	);
});

test("the global switch on without limits seeds until stopped", () => {
	assert.deepEqual(
		globalSeedingSummary({
			keepSeeding: "true",
			seedTime: "0",
			seedRatio: "0",
		}),
		{ kind: "forever", ratio: 0, minutes: 0 },
	);
});

test("the global switch on with both limits stops at whichever comes first", () => {
	assert.deepEqual(
		globalSeedingSummary({ keepSeeding: true, seedTime: 525600, seedRatio: 1 }),
		{ kind: "both", ratio: 1, minutes: 525600 },
	);
});

test("a global ratio seeds even with the switch off", () => {
	assert.deepEqual(
		globalSeedingSummary({ keepSeeding: false, seedTime: 0, seedRatio: 1 }),
		{ kind: "ratio", ratio: 1, minutes: 0 },
	);
});
