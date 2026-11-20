import assert from "node:assert/strict";
import { test } from "node:test";
import { fetchAllPages } from "./taskPages.ts";

type Row = { gid: string };

const rows = (...gids: string[]): Row[] => gids.map((gid) => ({ gid }));

function pager(pages: Array<Row[] | null | undefined>) {
	const calls: Array<[number, number]> = [];
	const fetchPage = async (offset: number, num: number) => {
		calls.push([offset, num]);
		return pages[calls.length - 1] ?? [];
	};
	return { calls, fetchPage };
}

test("one short page is the whole list", async () => {
	const { calls, fetchPage } = pager([rows("a", "b")]);
	const out = await fetchAllPages(fetchPage, 3);
	assert.deepEqual(
		out.map((row) => row.gid),
		["a", "b"],
	);
	assert.deepEqual(calls, [[0, 3]]);
});

test("a full page is followed by a request that comes back empty", async () => {
	const { calls, fetchPage } = pager([rows("a", "b", "c"), []]);
	const out = await fetchAllPages(fetchPage, 3);
	assert.deepEqual(
		out.map((row) => row.gid),
		["a", "b", "c"],
	);
	assert.deepEqual(calls, [
		[0, 3],
		[3, 3],
	]);
});

test("older pages go in front of newer ones", async () => {
	const { calls, fetchPage } = pager([
		rows("g", "h", "i"),
		rows("d", "e", "f"),
		rows("b", "c"),
	]);
	const out = await fetchAllPages(fetchPage, 3);
	assert.deepEqual(
		out.map((row) => row.gid),
		["b", "c", "d", "e", "f", "g", "h", "i"],
	);
	assert.deepEqual(calls, [
		[0, 3],
		[3, 3],
		[6, 3],
	]);
});

test("a task seen in two pages is kept once, from the newer page", async () => {
	const newer = { gid: "c", from: "newer" };
	const older = { gid: "c", from: "older" };
	const { calls, fetchPage } = pager([
		[newer, { gid: "d", from: "newer" }, { gid: "e", from: "newer" }],
		[{ gid: "a", from: "older" }, { gid: "b", from: "older" }, older],
		[],
	]);
	const out = await fetchAllPages(fetchPage, 3);
	assert.deepEqual(
		out.map((row) => row.gid),
		["a", "b", "c", "d", "e"],
	);
	assert.equal(
		out.find((row) => row.gid === "c"),
		newer,
	);
	assert.equal(calls.length, 3);
});

test("stops at the row limit and keeps the newest rows", async () => {
	const { calls, fetchPage } = pager([
		rows("e", "f"),
		rows("c", "d"),
		rows("a", "b"),
	]);
	const out = await fetchAllPages(fetchPage, 2, 3);
	assert.deepEqual(
		out.map((row) => row.gid),
		["d", "e", "f"],
	);
	assert.equal(calls.length, 2);
});

test("a page that is not an array ends the list", async () => {
	const first = pager([null]);
	assert.deepEqual(await fetchAllPages(first.fetchPage, 2), []);
	assert.equal(first.calls.length, 1);

	const later = pager([rows("c", "d"), undefined]);
	const out = await fetchAllPages(later.fetchPage, 2);
	assert.deepEqual(
		out.map((row) => row.gid),
		["c", "d"],
	);
	assert.equal(later.calls.length, 2);
});

test("a rejected page rejects the whole call", async () => {
	let call = 0;
	const fetchPage = async () => {
		call += 1;
		if (call === 2) {
			throw new Error("engine gone");
		}
		return rows("c", "d");
	};
	await assert.rejects(fetchAllPages(fetchPage, 2), /engine gone/);
});
