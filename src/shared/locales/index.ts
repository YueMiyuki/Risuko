export const availableLanguages = [
	{
		value: "auto",
		label: "Auto",
	},
	{
		value: "ar",
		label: "عربي",
	},
	{
		value: "bg",
		label: "Българският език",
	},
	{
		value: "ca",
		label: "Català",
	},
	{
		value: "de",
		label: "Deutsch",
	},
	{
		value: "el",
		label: "Ελληνικά",
	},
	{
		value: "en-US",
		label: "English",
	},
	{
		value: "es",
		label: "Español",
	},
	{
		value: "fa",
		label: "فارسی",
	},
	{
		value: "fr",
		label: "Français",
	},
	{
		value: "hu",
		label: "Hungarian",
	},
	{
		value: "id",
		label: "Indonesia",
	},
	{
		value: "it",
		label: "Italiano",
	},
	{
		value: "ja",
		label: "日本語",
	},
	{
		value: "ko",
		label: "한국어",
	},
	{
		value: "nb",
		label: "Norsk Bokmål",
	},
	{
		value: "nl",
		label: "Nederlands",
	},
	{
		value: "pl",
		label: "Polski",
	},
	{
		value: "pt-BR",
		label: "Português (Brasil)",
	},
	{
		value: "ro",
		label: "Română",
	},
	{
		value: "ru",
		label: "Русский",
	},
	{
		value: "th",
		label: "แบบไทย",
	},
	{
		value: "tr",
		label: "Türkçe",
	},
	{
		value: "uk",
		label: "Українська",
	},
	{
		value: "vi",
		label: "Tiếng Việt",
	},
	{
		value: "zh-CN",
		label: "简体中文",
	},
	{
		value: "zh-TW",
		label: "繁體中文",
	},
];

const checkLngIsAvailable = (locale: string) => {
	return availableLanguages.some(
		(lng) => lng.value !== "auto" && lng.value === locale,
	);
};

const REGIONLESS_DEFAULTS: Record<string, string> = {
	en: "en-US",
	pt: "pt-BR",
	no: "nb",
};

const TRADITIONAL_CHINESE = /^zh-(hant|tw|hk|mo)\b/;

const matchLanguage = (locale: unknown): string | null => {
	if (typeof locale !== "string" || !locale) {
		return null;
	}
	const tag = locale.trim().replace(/_/g, "-");
	const lower = tag.toLowerCase();
	const exact = availableLanguages.find(
		(lng) => lng.value !== "auto" && lng.value.toLowerCase() === lower,
	);
	if (exact) {
		return exact.value;
	}
	if (lower === "zh" || lower.startsWith("zh-")) {
		return TRADITIONAL_CHINESE.test(lower) ? "zh-TW" : "zh-CN";
	}
	const base = lower.split("-")[0];
	if (Object.hasOwn(REGIONLESS_DEFAULTS, base)) {
		return REGIONLESS_DEFAULTS[base];
	}
	return checkLngIsAvailable(base) ? base : null;
};

export const pickLanguage = (candidates: readonly unknown[]): string => {
	for (const candidate of candidates) {
		const match = matchLanguage(candidate);
		if (match) {
			return match;
		}
	}
	return "en-US";
};

const getSystemLocales = (): readonly unknown[] => {
	if (typeof navigator === "undefined") {
		return [];
	}
	return Array.isArray(navigator.languages)
		? navigator.languages
		: [navigator.language];
};

export const getLanguage = (locale: unknown = "en-US"): string => {
	if (locale === "auto") {
		return pickLanguage(getSystemLocales());
	}
	return matchLanguage(locale) ?? "en-US";
};
