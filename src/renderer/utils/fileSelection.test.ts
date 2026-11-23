import assert from "node:assert/strict";
import { test } from "node:test";
import {
	selectionState,
	sortedIndexList,
	splitBySelection,
} from "./fileSelection.ts";

test("selection state comes from counts", () => {
	assert.equal(selectionState(0, new Set()), "none");
	assert.equal(selectionState(5, new Set()), "none");
	assert.equal(selectionState(5, new Set([1, 3])), "partial");
	assert.equal(selectionState(3, new Set([1, 2, 3])), "all");
	assert.equal(selectionState(3, new Set([1, 2, 3, 4])), "partial");
});

test("index list is numerically sorted", () => {
	assert.equal(sortedIndexList(new Set([10, 2, 33])), "2,10,33");
	assert.equal(sortedIndexList(new Set()), "");
});

test("items with nothing selected are set aside", () => {
	const none = { id: "a", selectFile: "none" };
	const all = { id: "b", selectFile: "all" };
	const some = { id: "c", selectFile: "1,3" };
	const blank = { id: "d", selectFile: "" };
	const missing: { id: string; selectFile?: string } = { id: "e" };
	const lastNone = { id: "f", selectFile: "none" };

	const { selectable, empty } = splitBySelection(
		[none, all, some, blank, missing, lastNone],
		"none",
	);
	assert.deepEqual(empty, [none, lastNone]);
	assert.deepEqual(selectable, [all, some, blank, missing]);

	const nothing = splitBySelection([], "none");
	assert.deepEqual(nothing, { selectable: [], empty: [] });
});
