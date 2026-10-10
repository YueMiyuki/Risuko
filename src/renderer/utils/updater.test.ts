import assert from "node:assert/strict";
import path from "node:path";
import { after, before, beforeEach, test } from "node:test";
import { fileURLToPath } from "node:url";
import { createServer, type Plugin, type ViteDevServer } from "vite";

type Platform = "linux" | "windows" | "macos" | "android" | "unknown";

type ToastEvent = {
	kind: string;
	args: unknown[];
};

type UpdateTestState = {
	platform: Platform;
	invokeResult:
		| unknown
		| ((command: string, args: unknown) => unknown | Promise<unknown>);
	invokeCalls: Array<[string, unknown]>;
	checkResult: unknown | ((options: unknown) => unknown | Promise<unknown>);
	checkCalls: unknown[];
	confirmQueue: Array<{ confirmed: boolean; checkboxChecked: boolean }>;
	confirmCalls: unknown[];
	toastEvents: ToastEvent[];
	warningEvents: unknown[][];
	relaunchCalls: number;
	store: {
		config: Record<string, unknown>;
		saveCalls: Array<Record<string, unknown>>;
		save: (config: Record<string, unknown>) => Promise<void>;
	};
};

type UpdaterModule = typeof import("./updater.ts");

const root = path.resolve(
	path.dirname(fileURLToPath(import.meta.url)),
	"../../..",
);
const updaterPath = path.join(root, "src/renderer/utils/updater.ts");
const globals = globalThis as typeof globalThis & {
	__RISUKO_UPDATER_TEST__?: UpdateTestState;
	__TAURI_INTERNALS__?: unknown;
};

function createState(): UpdateTestState {
	const state = {
		platform: "linux" as Platform,
		invokeResult: (command) =>
			command === "is_signed_updater_available" ? true : null,
		invokeCalls: [],
		checkResult: null,
		checkCalls: [],
		confirmQueue: [],
		confirmCalls: [],
		toastEvents: [],
		warningEvents: [],
		relaunchCalls: 0,
		store: {
			config: {
				autoCheckUpdate: false,
				lastCheckUpdateTime: 0,
			},
			saveCalls: [],
			save: async (config: Record<string, unknown>) => {
				state.store.saveCalls.push(config);
				state.store.config = { ...state.store.config, ...config };
			},
		},
	} satisfies UpdateTestState;

	return state;
}

let state = createState();
let updater: UpdaterModule;
let server: ViteDevServer;

const mockModules: Record<string, string> = {
	"virtual:risuko-updater-core": `
		export async function invoke(command, args) {
			const state = globalThis.__RISUKO_UPDATER_TEST__;
			state.invokeCalls.push([command, args]);
			const result = state.invokeResult;
			return typeof result === "function" ? result(command, args) : result;
		}
	`,
	"virtual:risuko-updater-plugin": `
		export async function check(options) {
			const state = globalThis.__RISUKO_UPDATER_TEST__;
			state.checkCalls.push(options);
			const result = state.checkResult;
			return typeof result === "function" ? result(options) : result;
		}
	`,
	"virtual:risuko-updater-process": `
		export async function relaunch() {
			globalThis.__RISUKO_UPDATER_TEST__.relaunchCalls += 1;
		}
	`,
	"virtual:risuko-updater-locale": `
		export function getLocaleManager() {
			return { getI18n: () => ({ t: (key) => key }) };
		}
	`,
	"virtual:risuko-updater-confirm": `
		export async function confirm(options) {
			const state = globalThis.__RISUKO_UPDATER_TEST__;
			state.confirmCalls.push(options);
			return state.confirmQueue.shift() ?? { confirmed: false, checkboxChecked: false };
		}
	`,
	"virtual:risuko-updater-platform": `
		export default {
			renderer: () => true,
			macOS: () => globalThis.__RISUKO_UPDATER_TEST__.platform === "macos",
			windows: () => globalThis.__RISUKO_UPDATER_TEST__.platform === "windows",
			linux: () => globalThis.__RISUKO_UPDATER_TEST__.platform === "linux",
			android: () => globalThis.__RISUKO_UPDATER_TEST__.platform === "android",
			mas: () => false,
		};
	`,
	"virtual:risuko-updater-preference": `
		export function usePreferenceStore() {
			return globalThis.__RISUKO_UPDATER_TEST__.store;
		}
	`,
	"virtual:risuko-updater-logger": `
		export default {
			log() {},
			debug() {},
			info() {},
			warn(...args) { globalThis.__RISUKO_UPDATER_TEST__.warningEvents.push(args); },
			error() {},
		};
	`,
	"virtual:risuko-updater-toast": `
		const emit = (kind, ...args) => {
			globalThis.__RISUKO_UPDATER_TEST__.toastEvents.push({ kind, args });
			return args[1]?.id ?? "updater-test-toast";
		};
		export const toast = {
			info: (...args) => emit("info", ...args),
			error: (...args) => emit("error", ...args),
			success: (...args) => emit("success", ...args),
			loading: (...args) => emit("loading", ...args),
		};
	`,
	"virtual:risuko-updater-config-types": "export {};",
};

function createMockPlugin(): Plugin {
	return {
		name: "risuko-updater-test-mocks",
		resolveId(id) {
			return Object.hasOwn(mockModules, id) ? id : undefined;
		},
		load(id) {
			return mockModules[id];
		},
	};
}

function resetUpdaterState(): void {
	updater.updaterState.status = "idle";
	updater.updaterState.version = null;
	updater.updaterState.progress = null;
	updater.updaterState.lastCheckedAt = 0;
	updater.updaterState.error = null;
}

function useDesktop(stateForTest: UpdateTestState): void {
	stateForTest.platform = "linux";
	globals.__TAURI_INTERNALS__ = {};
}

async function waitForCheckCall(): Promise<void> {
	for (let attempt = 0; attempt < 20; attempt += 1) {
		if (state.checkCalls.length > 0) {
			return;
		}
		await new Promise<void>((resolve) => setImmediate(resolve));
	}
	throw new Error("updater check was not called");
}

before(async () => {
	server = await createServer({
		root,
		appType: "custom",
		logLevel: "silent",
		plugins: [createMockPlugin()],
		resolve: {
			alias: [
				{
					find: "@shared/types/config",
					replacement: "virtual:risuko-updater-config-types",
				},
				{
					find: "@shared/utils/logger",
					replacement: "virtual:risuko-updater-logger",
				},
				{
					find: "@tauri-apps/api/core",
					replacement: "virtual:risuko-updater-core",
				},
				{
					find: "@tauri-apps/plugin-updater",
					replacement: "virtual:risuko-updater-plugin",
				},
				{
					find: "@tauri-apps/plugin-process",
					replacement: "virtual:risuko-updater-process",
				},
				{
					find: "@/components/Locale",
					replacement: "virtual:risuko-updater-locale",
				},
				{
					find: "@/components/ui/confirm-dialog",
					replacement: "virtual:risuko-updater-confirm",
				},
				{
					find: "@/shims/platform",
					replacement: "virtual:risuko-updater-platform",
				},
				{
					find: "@/store/preference",
					replacement: "virtual:risuko-updater-preference",
				},
				{ find: "vue-sonner", replacement: "virtual:risuko-updater-toast" },
			],
		},
	});
	updater = (await server.ssrLoadModule(updaterPath)) as UpdaterModule;
});

beforeEach(() => {
	state = createState();
	globals.__RISUKO_UPDATER_TEST__ = state;
	globals.__TAURI_INTERNALS__ = {};
	resetUpdaterState();
});

after(async () => {
	delete globals.__RISUKO_UPDATER_TEST__;
	delete globals.__TAURI_INTERNALS__;
	await server.close();
});

test("automatic checks are opt-in and throttled for 24 hours", () => {
	const now = Date.now();
	assert.equal(
		updater.shouldRunAutomaticCheck({
			locale: "en-US",
			autoCheckUpdate: false,
			lastCheckUpdateTime: 0,
		}),
		false,
	);
	assert.equal(
		updater.shouldRunAutomaticCheck({
			locale: "en-US",
			autoCheckUpdate: true,
			lastCheckUpdateTime: now - 60 * 60 * 1000,
		}),
		false,
	);
	assert.equal(
		updater.shouldRunAutomaticCheck({
			locale: "en-US",
			autoCheckUpdate: true,
			lastCheckUpdateTime: now - 24 * 60 * 60 * 1000 - 1,
		}),
		true,
	);
});

test("automatic checks skip the native command while inside the throttle window", async () => {
	useDesktop(state);
	state.store.config = {
		autoCheckUpdate: true,
		lastCheckUpdateTime: Date.now() - 1,
	};

	assert.equal(await updater.checkForUpdates("automatic"), null);
	assert.equal(state.invokeCalls.length, 0);
	assert.equal(state.checkCalls.length, 0);
});

test("unsupported mobile and non-Tauri runtimes are no-ops", async () => {
	state.platform = "android";
	globals.__TAURI_INTERNALS__ = {};
	assert.equal(updater.isDesktopUpdaterAvailable(), false);
	assert.equal(await updater.checkForUpdates("manual"), null);

	state.platform = "linux";
	delete globals.__TAURI_INTERNALS__;
	assert.equal(updater.isDesktopUpdaterAvailable(), false);
	assert.equal(await updater.checkForUpdates("manual"), null);
	assert.equal(state.invokeCalls.length, 0);
	assert.equal(state.checkCalls.length, 0);
});

test("unsigned desktop builds do not invoke the updater plugin", async () => {
	useDesktop(state);
	state.store.config = { autoCheckUpdate: true, lastCheckUpdateTime: 0 };
	state.invokeResult = (command) =>
		command === "is_signed_updater_available" ? false : null;

	assert.equal(await updater.checkForUpdates("manual"), null);
	assert.deepEqual(state.invokeCalls, [
		["is_signed_updater_available", undefined],
	]);
	assert.equal(state.checkCalls.length, 0);
	assert.equal(state.confirmCalls.length, 0);
	assert.ok(
		state.toastEvents.some(
			(event) =>
				event.kind === "error" && event.args[0] === "app.update-unsigned",
		),
	);
	const toastCount = state.toastEvents.length;
	assert.equal(await updater.checkForUpdates("automatic"), null);
	assert.equal(state.invokeCalls.length, 2);
	assert.equal(state.checkCalls.length, 0);
	assert.equal(state.toastEvents.length, toastCount);
});

test("passes the resolved update-app proxy to the updater command and persists a successful check", async () => {
	useDesktop(state);
	state.store.config = { autoCheckUpdate: true, lastCheckUpdateTime: 0 };
	state.invokeResult = (command) =>
		command === "is_signed_updater_available"
			? true
			: "http://proxy.example:8080";
	state.checkResult = null;

	assert.equal(await updater.checkForUpdates("manual"), null);
	assert.deepEqual(state.invokeCalls, [
		["is_signed_updater_available", undefined],
		[
			"resolve_configured_proxy",
			{ scope: "update-app", url: "https://risuko.app/api/update" },
		],
	]);
	assert.deepEqual(state.checkCalls, [
		{ proxy: "http://proxy.example:8080", timeout: 15_000 },
	]);
	assert.equal(state.store.saveCalls.length, 1);
	assert.equal(
		state.store.saveCalls[0].lastCheckUpdateTime,
		updater.updaterState.lastCheckedAt,
	);
	assert.equal(updater.updaterState.status, "up-to-date");
});

test("omits the proxy option when native resolution selects a direct connection", async () => {
	useDesktop(state);
	state.store.config = { autoCheckUpdate: true, lastCheckUpdateTime: 0 };
	state.invokeResult = (command) =>
		command === "is_signed_updater_available" ? true : null;
	state.checkResult = null;

	assert.equal(await updater.checkForUpdates("manual"), null);
	assert.deepEqual(state.checkCalls, [{ proxy: undefined, timeout: 15_000 }]);
});

test("does not advance the automatic-check throttle after a failed check", async () => {
	useDesktop(state);
	state.store.config = { autoCheckUpdate: true, lastCheckUpdateTime: 0 };
	state.checkResult = () => Promise.reject(new Error("network unavailable"));

	assert.equal(await updater.checkForUpdates("manual"), null);
	assert.equal(state.store.saveCalls.length, 0);
	assert.equal(updater.updaterState.lastCheckedAt, 0);
	assert.equal(updater.updaterState.status, "error");
});

test("continues the update flow when persisting the check time fails", async () => {
	useDesktop(state);
	state.store.config = { autoCheckUpdate: true, lastCheckUpdateTime: 0 };
	state.store.save = async (config) => {
		state.store.saveCalls.push(config);
		throw new Error("preference store unavailable");
	};

	let downloadCalls = 0;
	let closeCalls = 0;
	const update = {
		version: "9.9.9",
		download: async (onEvent: (event: unknown) => void) => {
			downloadCalls += 1;
			onEvent({ event: "Started", data: { contentLength: 1 } });
			onEvent({ event: "Progress", data: { chunkLength: 1 } });
			onEvent({ event: "Finished" });
		},
		install: async () => undefined,
		close: async () => {
			closeCalls += 1;
		},
	};
	state.checkResult = update;
	state.confirmQueue = [
		{ confirmed: true, checkboxChecked: false },
		{ confirmed: false, checkboxChecked: false },
	];

	assert.strictEqual(await updater.checkForUpdates("manual"), update);
	assert.equal(downloadCalls, 1);
	assert.equal(closeCalls, 1);
	assert.equal(state.store.saveCalls.length, 1);
	assert.equal(state.confirmCalls.length, 2);
	assert.ok(
		state.warningEvents.some((args) =>
			String(args[0]).includes("failed to persist update check time"),
		),
	);
});

test("relaunches through the native command so the engine stops first", async () => {
	useDesktop(state);
	state.store.config = { autoCheckUpdate: true, lastCheckUpdateTime: 0 };
	state.invokeResult = (command) =>
		command === "is_signed_updater_available" ? true : null;
	const update = {
		version: "9.9.9",
		download: async () => undefined,
		install: async () => undefined,
		close: async () => undefined,
	};
	state.checkResult = update;
	state.confirmQueue = [
		{ confirmed: true, checkboxChecked: false },
		{ confirmed: true, checkboxChecked: false },
		{ confirmed: true, checkboxChecked: false },
	];

	await updater.checkForUpdates("manual");
	assert.ok(state.invokeCalls.some(([command]) => command === "relaunch_app"));
	assert.equal(state.relaunchCalls, 0);
});

test("coalesces concurrent automatic and manual checks before the signing gate resolves", async () => {
	useDesktop(state);
	state.store.config = { autoCheckUpdate: true, lastCheckUpdateTime: 0 };
	let releaseSigningGate!: (value: boolean) => void;
	let releaseCheck!: (value: null) => void;
	state.invokeResult = (command) => {
		if (command === "is_signed_updater_available") {
			return new Promise<boolean>((resolve) => {
				releaseSigningGate = resolve;
			});
		}
		return null;
	};
	state.checkResult = () =>
		new Promise<null>((resolve) => {
			releaseCheck = resolve;
		});

	const automatic = updater.checkForUpdates("automatic");
	const manual = updater.checkForUpdates("manual");
	for (
		let attempt = 0;
		attempt < 20 && state.invokeCalls.length === 0;
		attempt += 1
	) {
		await new Promise<void>((resolve) => setImmediate(resolve));
	}
	assert.deepEqual(state.invokeCalls, [
		["is_signed_updater_available", undefined],
	]);
	assert.equal(state.checkCalls.length, 0);

	releaseSigningGate(true);
	await waitForCheckCall();

	assert.equal(state.checkCalls.length, 1);
	assert.equal(
		state.toastEvents.filter(
			(event) =>
				event.kind === "info" && event.args[0] === "app.update-checking",
		).length,
		1,
	);

	releaseCheck(null);
	await Promise.all([automatic, manual]);
	assert.equal(state.store.saveCalls.length, 1);
	assert.equal(updater.updaterState.status, "up-to-date");
	assert.ok(
		state.toastEvents.some(
			(event) =>
				event.kind === "success" && event.args[0] === "app.update-unavailable",
		),
	);
});

test("reports bounded download progress through the shared updater state and toast", () => {
	const bytes = { downloaded: 0, total: null as number | null };
	updater.updateDownloadProgress(
		{ event: "Started", data: { contentLength: 200 } },
		bytes,
		"download-toast",
	);
	assert.deepEqual(bytes, {
		downloaded: 0,
		total: 200,
		lastReportedProgress: null,
	});
	assert.equal(updater.updaterState.progress, 0);

	updater.updateDownloadProgress(
		{ event: "Progress", data: { chunkLength: 50 } },
		bytes,
		"download-toast",
	);
	assert.equal(updater.updaterState.progress, 25);
	assert.ok(
		state.toastEvents.some(
			(event) =>
				event.kind === "loading" &&
				(event.args[1] as { id?: unknown } | undefined)?.id ===
					"download-toast",
		),
	);

	updater.updateDownloadProgress(
		{ event: "Finished" },
		bytes,
		"download-toast",
	);
	assert.equal(updater.updaterState.progress, 100);
});
