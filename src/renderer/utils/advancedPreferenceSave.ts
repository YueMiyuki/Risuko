type DohPresets = Readonly<
	Record<string, { readonly url: string; readonly bootstrap: string }>
>;

const ENGINE_OVERRIDES_MAX_KEYS = 64;
const ENGINE_OVERRIDES_MAX_KEY_LENGTH = 64;
const ENGINE_OVERRIDES_MAX_VALUE_LENGTH = 2048;

const isEngineOverrideValue = (value: unknown) =>
	value === null ||
	typeof value === "boolean" ||
	(typeof value === "number" && Number.isFinite(value)) ||
	(typeof value === "string" &&
		value.length <= ENGINE_OVERRIDES_MAX_VALUE_LENGTH);

export function validateEngineOverrideBounds(
	input: Record<string, unknown>,
): boolean {
	const entries = Object.entries(input || {});
	if (entries.length > ENGINE_OVERRIDES_MAX_KEYS) {
		return false;
	}
	return entries.every(
		([key, value]) =>
			key.length <= ENGINE_OVERRIDES_MAX_KEY_LENGTH &&
			isEngineOverrideValue(value),
	);
}

export function sanitizeEngineOverrides(input: Record<string, unknown>): {
	filtered: Record<string, unknown>;
	droppedKeys: string[];
} {
	const filtered: Record<string, unknown> = {};
	const droppedKeys: string[] = [];
	for (const [key, value] of Object.entries(input || {})) {
		const normalized = `${key}`.toLowerCase();
		if (
			normalized.startsWith("rpc-") ||
			normalized.endsWith("-port") ||
			normalized.endsWith("-secret")
		) {
			droppedKeys.push(key);
			continue;
		}
		filtered[key] = value;
	}
	return { filtered, droppedKeys };
}

export function resolveDohProvider(
	storedProvider: unknown,
	storedUrl: unknown,
	presets: DohPresets,
): string {
	const provider = typeof storedProvider === "string" ? storedProvider : "";
	const known = Object.hasOwn(presets, provider);
	const url = `${storedUrl || ""}`.trim();
	if (url) {
		for (const [name, preset] of Object.entries(presets)) {
			if (preset.url && preset.url === url) {
				return name;
			}
		}
		return known && provider !== "custom" ? provider : "custom";
	}
	return known ? provider : "cloudflare";
}

export function isPortValue(value: unknown): boolean {
	if (typeof value === "number") {
		return Number.isInteger(value) && value >= 1 && value <= 65535;
	}
	if (typeof value !== "string") {
		return false;
	}
	const text = value.trim();
	return /^\d+$/.test(text) && isPortValue(Number(text));
}

export function isPortList(value: unknown): boolean {
	if (typeof value === "number") {
		return isPortValue(value);
	}
	if (typeof value !== "string") {
		return false;
	}
	return value.split(",").every((part) => {
		const bounds = part.split("-");
		if (bounds.length === 1) {
			return isPortValue(bounds[0]);
		}
		if (bounds.length !== 2) {
			return false;
		}
		const [low, high] = bounds;
		return isPortValue(low) && isPortValue(high) && Number(low) <= Number(high);
	});
}

export interface AdvancedSaveForm {
	engineOverridesText?: unknown;
	dohProvider?: unknown;
	dohEnable?: unknown;
	dohUrl?: unknown;
	dohBootstrap?: unknown;
}

export interface AdvancedSaveResult {
	error: string | null;
	droppedOverrideKeys: string[];
}

const fail = (error: string): AdvancedSaveResult => ({
	error,
	droppedOverrideKeys: [],
});

const isUnsetPort = (value: unknown) =>
	value === "" || value === null || value === undefined;

export function finalizeAdvancedChanges(
	data: Record<string, unknown>,
	form: AdvancedSaveForm,
	presets: DohPresets,
): AdvancedSaveResult {
	let droppedOverrideKeys: string[] = [];

	if ("engineOverridesText" in data) {
		const raw = `${form.engineOverridesText || ""}`.trim();
		if (!raw) {
			data.engineOverrides = {};
		} else {
			let parsed: unknown;
			try {
				parsed = JSON.parse(raw);
			} catch {
				return fail("preferences.engine-overrides-invalid");
			}
			if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
				return fail("preferences.engine-overrides-invalid");
			}
			const { filtered, droppedKeys } = sanitizeEngineOverrides(
				parsed as Record<string, unknown>,
			);
			if (!validateEngineOverrideBounds(filtered)) {
				return fail("preferences.engine-overrides-too-large");
			}
			data.engineOverrides = filtered;
			droppedOverrideKeys = droppedKeys;
		}
		delete data.engineOverridesText;
	}

	if (
		"dohProvider" in data ||
		"dohEnable" in data ||
		"dohUrl" in data ||
		"dohBootstrap" in data
	) {
		const provider =
			typeof form.dohProvider === "string" ? form.dohProvider : "";
		const userBootstrap = `${form.dohBootstrap || ""}`.trim();
		if (provider !== "custom" && Object.hasOwn(presets, provider)) {
			const preset = presets[provider];
			data.dohUrl = preset.url;
			data.dohBootstrap = userBootstrap || preset.bootstrap;
		} else {
			data.dohUrl = `${form.dohUrl || ""}`.trim();
			data.dohBootstrap = userBootstrap;
		}
		if (
			form.dohEnable &&
			provider === "custom" &&
			!/^https:\/\//i.test(`${data.dohUrl}`)
		) {
			return fail("preferences.doh-url-invalid");
		}
	}

	if ("ed2kKadPort" in data) {
		const raw = data.ed2kKadPort;
		if (raw !== "" && raw !== null && raw !== undefined) {
			const port = Number(raw);
			if (!(Number.isInteger(port) && port >= 1 && port <= 65535)) {
				return fail("preferences.ed2k-kad-port-invalid");
			}
		}
	}

	if (
		"listenPort" in data &&
		!isUnsetPort(data.listenPort) &&
		!isPortList(data.listenPort)
	) {
		return fail("preferences.listen-port-invalid");
	}

	if (
		"dhtListenPort" in data &&
		!isUnsetPort(data.dhtListenPort) &&
		!isPortList(data.dhtListenPort)
	) {
		return fail("preferences.dht-listen-port-invalid");
	}

	if (
		"ed2kPort" in data &&
		!isUnsetPort(data.ed2kPort) &&
		!isPortValue(data.ed2kPort)
	) {
		return fail("preferences.ed2k-port-invalid");
	}

	return { error: null, droppedOverrideKeys };
}
