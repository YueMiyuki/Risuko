import assert from "node:assert/strict";
import { test } from "node:test";
import {
	finalizeAdvancedChanges,
	isPortList,
	isPortValue,
	resolveDohProvider,
} from "./advancedPreferenceSave.ts";

const PRESETS = {
	cloudflare: {
		url: "https://cloudflare-dns.com/dns-query",
		bootstrap: "1.1.1.1",
	},
	custom: { url: "", bootstrap: "" },
};

test("pending engine override text is parsed and reserved keys are dropped", () => {
	const data: Record<string, unknown> = {
		engineOverridesText: '{"split": 8, "rpc-secret": "x"}',
	};
	const result = finalizeAdvancedChanges(
		data,
		{ engineOverridesText: data.engineOverridesText },
		PRESETS,
	);
	assert.deepEqual(result, {
		error: null,
		droppedOverrideKeys: ["rpc-secret"],
	});
	assert.deepEqual(data, { engineOverrides: { split: 8 } });
});

test("empty override text clears the overrides and bad text is refused", () => {
	const cleared: Record<string, unknown> = { engineOverridesText: "  " };
	assert.equal(
		finalizeAdvancedChanges(cleared, { engineOverridesText: "  " }, PRESETS)
			.error,
		null,
	);
	assert.deepEqual(cleared, { engineOverrides: {} });

	for (const text of ["{", "[1]", '"x"']) {
		assert.deepEqual(
			finalizeAdvancedChanges(
				{ engineOverridesText: text },
				{ engineOverridesText: text },
				PRESETS,
			),
			{
				error: "preferences.engine-overrides-invalid",
				droppedOverrideKeys: [],
			},
		);
	}
});

test("switching DNS over HTTPS on fills in the preset address", () => {
	const data: Record<string, unknown> = { dohEnable: true };
	const result = finalizeAdvancedChanges(
		data,
		{
			dohEnable: true,
			dohProvider: "cloudflare",
			dohUrl: "",
			dohBootstrap: "",
		},
		PRESETS,
	);
	assert.equal(result.error, null);
	assert.deepEqual(data, {
		dohEnable: true,
		dohUrl: "https://cloudflare-dns.com/dns-query",
		dohBootstrap: "1.1.1.1",
	});
});

test("a custom DNS over HTTPS provider needs an https address when enabled", () => {
	const form = {
		dohEnable: true,
		dohProvider: "custom",
		dohUrl: "dns.example",
	};
	assert.deepEqual(
		finalizeAdvancedChanges({ dohProvider: "custom" }, form, PRESETS),
		{ error: "preferences.doh-url-invalid", droppedOverrideKeys: [] },
	);
	const data: Record<string, unknown> = { dohUrl: " https://dns.example/q " };
	assert.equal(
		finalizeAdvancedChanges(
			data,
			{ ...form, dohUrl: " https://dns.example/q ", dohBootstrap: " 9.9.9.9 " },
			PRESETS,
		).error,
		null,
	);
	assert.deepEqual(data, {
		dohUrl: "https://dns.example/q",
		dohBootstrap: "9.9.9.9",
	});
});

test("an out-of-range Kad port is refused", () => {
	assert.deepEqual(
		finalizeAdvancedChanges({ ed2kKadPort: 70000 }, {}, PRESETS),
		{
			error: "preferences.ed2k-kad-port-invalid",
			droppedOverrideKeys: [],
		},
	);
	assert.equal(
		finalizeAdvancedChanges({ ed2kKadPort: "" }, {}, PRESETS).error,
		null,
	);
	assert.equal(
		finalizeAdvancedChanges({ ed2kKadPort: 4672 }, {}, PRESETS).error,
		null,
	);
});

test("a port is an integer from 1 to 65535 as a number or digits", () => {
	for (const value of [1, 6881, 65535, "1", "4662", " 6881 ", "65535"]) {
		assert.equal(isPortValue(value), true, String(value));
	}
	for (const value of [
		0,
		65536,
		1.5,
		Number.NaN,
		"0",
		"65536",
		"12a",
		"1.5",
		"-1",
		"",
		" ",
		null,
		undefined,
	]) {
		assert.equal(isPortValue(value), false, String(value));
	}
});

test("a port list holds ports and ascending ranges separated by commas", () => {
	for (const value of [
		"6881",
		"6881-6889",
		"6881, 6890-6899",
		" 6881 - 6889 ",
		"6881-6881",
		6881,
	]) {
		assert.equal(isPortList(value), true, String(value));
	}
	for (const value of [
		"6889-6881",
		"6881-",
		"-6881",
		"70000",
		"6881-70000",
		"6881,,6882",
		"6881,",
		"1-2-3",
		"abc",
		"",
		0,
		null,
		undefined,
	]) {
		assert.equal(isPortList(value), false, String(value));
	}
});

test("the BT port must be a valid port list", () => {
	for (const listenPort of ["6881", "6881-6889, 6900", 21301]) {
		assert.equal(
			finalizeAdvancedChanges({ listenPort }, {}, PRESETS).error,
			null,
		);
	}
	for (const listenPort of ["70000", "abc", "6889-6881"]) {
		assert.deepEqual(finalizeAdvancedChanges({ listenPort }, {}, PRESETS), {
			error: "preferences.listen-port-invalid",
			droppedOverrideKeys: [],
		});
	}
	assert.equal(
		finalizeAdvancedChanges({ listenPort: "" }, {}, PRESETS).error,
		null,
	);
});

test("the DHT port must be a valid port list", () => {
	for (const dhtListenPort of ["26701", "26701-26709", 26701]) {
		assert.equal(
			finalizeAdvancedChanges({ dhtListenPort }, {}, PRESETS).error,
			null,
		);
	}
	for (const dhtListenPort of ["0", "26701,,26702", "x"]) {
		assert.deepEqual(finalizeAdvancedChanges({ dhtListenPort }, {}, PRESETS), {
			error: "preferences.dht-listen-port-invalid",
			droppedOverrideKeys: [],
		});
	}
	assert.equal(
		finalizeAdvancedChanges({ dhtListenPort: "" }, {}, PRESETS).error,
		null,
	);
});

test("the ED2K port must be a single port", () => {
	for (const ed2kPort of [4662, "4662", "65535"]) {
		assert.equal(
			finalizeAdvancedChanges({ ed2kPort }, {}, PRESETS).error,
			null,
		);
	}
	for (const ed2kPort of [70000, "4662-4670", "4662,4663", "abc"]) {
		assert.deepEqual(finalizeAdvancedChanges({ ed2kPort }, {}, PRESETS), {
			error: "preferences.ed2k-port-invalid",
			droppedOverrideKeys: [],
		});
	}
	assert.equal(
		finalizeAdvancedChanges({ ed2kPort: "" }, {}, PRESETS).error,
		null,
	);
});

test("unset port fields mean the default", () => {
	for (const value of [null, undefined]) {
		assert.equal(
			finalizeAdvancedChanges(
				{ listenPort: value, dhtListenPort: value, ed2kPort: value },
				{},
				PRESETS,
			).error,
			null,
		);
	}
});

test("the stored provider is derived from the saved address", () => {
	const url = PRESETS.cloudflare.url;
	assert.equal(resolveDohProvider("custom", url, PRESETS), "cloudflare");
	assert.equal(
		resolveDohProvider("cloudflare", "https://x.example", PRESETS),
		"cloudflare",
	);
	assert.equal(
		resolveDohProvider(undefined, "https://x.example", PRESETS),
		"custom",
	);
	assert.equal(resolveDohProvider("custom", "", PRESETS), "custom");
	assert.equal(resolveDohProvider("toString", "", PRESETS), "cloudflare");
});
