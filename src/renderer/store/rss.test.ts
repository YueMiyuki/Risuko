import assert from "node:assert/strict";
import path from "node:path";
import { after, before, beforeEach, test } from "node:test";
import { fileURLToPath } from "node:url";
import { createPinia, setActivePinia } from "pinia";
import { createServer, type Plugin, type ViteDevServer } from "vite";

type RssTestState = {
	itemsError: Error | null;
	warnings: unknown[][];
};

type RssStoreModule = typeof import("./rss.ts");

const root = path.resolve(
	path.dirname(fileURLToPath(import.meta.url)),
	"../../..",
);
const storePath = path.join(root, "src/renderer/store/rss.ts");
const globals = globalThis as typeof globalThis & {
	__RISUKO_RSS_TEST__?: RssTestState;
};

const mockModules: Record<string, string> = {
	"virtual:risuko-rss-api": `
		const state = () => globalThis.__RISUKO_RSS_TEST__;
		export default {
			async addRssFeed(url) {
				return { id: "feed-1", url, title: "Feed" };
			},
			async getRssItems() {
				if (state().itemsError) {
					throw state().itemsError;
				}
				return [{ id: "item-1", title: "Item" }];
			},
		};
	`,
	"virtual:risuko-rss-preference": `
		export function usePreferenceStore() {
			return { config: {} };
		}
	`,
	"virtual:risuko-rss-logger": `
		export default {
			log() {},
			debug() {},
			info() {},
			warn(...args) { globalThis.__RISUKO_RSS_TEST__.warnings.push(args); },
			error() {},
		};
	`,
};

function createMockPlugin(): Plugin {
	return {
		name: "risuko-rss-test-mocks",
		resolveId(id) {
			return Object.hasOwn(mockModules, id) ? id : undefined;
		},
		load(id) {
			return mockModules[id];
		},
	};
}

let state: RssTestState = { itemsError: null, warnings: [] };
let rssModule: RssStoreModule;
let server: ViteDevServer;

before(async () => {
	server = await createServer({
		root,
		appType: "custom",
		logLevel: "silent",
		plugins: [createMockPlugin()],
		resolve: {
			alias: [
				{ find: "@/api", replacement: "virtual:risuko-rss-api" },
				{
					find: "@/store/preference",
					replacement: "virtual:risuko-rss-preference",
				},
				{
					find: "@shared/utils/logger",
					replacement: "virtual:risuko-rss-logger",
				},
				{ find: /^@\//, replacement: `${root}/src/renderer/` },
			],
		},
	});
	rssModule = (await server.ssrLoadModule(storePath)) as RssStoreModule;
});

beforeEach(() => {
	state = { itemsError: null, warnings: [] };
	globals.__RISUKO_RSS_TEST__ = state;
	setActivePinia(createPinia());
});

after(async () => {
	delete globals.__RISUKO_RSS_TEST__;
	await server.close();
});

test("adding a feed loads its items", async () => {
	const store = rssModule.useRssStore();
	const feed = await store.addFeed("https://example.com/feed.xml");
	assert.equal(feed.id, "feed-1");
	assert.deepEqual(
		store.feeds.map((item) => item.id),
		["feed-1"],
	);
	assert.equal(store.items["feed-1"].length, 1);
	assert.equal(store.loading, false);
});

test("a failed first item fetch does not fail a feed that was already added", async () => {
	state.itemsError = new Error("items unavailable");
	const store = rssModule.useRssStore();
	const feed = await store.addFeed("https://example.com/feed.xml");
	assert.equal(feed.id, "feed-1");
	assert.deepEqual(
		store.feeds.map((item) => item.id),
		["feed-1"],
	);
	assert.equal(store.items["feed-1"], undefined);
	assert.equal(store.loading, false);
	assert.ok(
		state.warnings.some((args) =>
			String(args[0]).includes("initial item fetch failed"),
		),
	);
});
