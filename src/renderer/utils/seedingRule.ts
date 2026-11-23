export type SeedingChoice = "global" | "forever" | "limits" | "none";

export type SeedingLimits = { ratio: number; minutes: number };

export type SeedingRuleOptions = {
	keepSeeding: string | null;
	seedTime: string | null;
	seedRatio: string | null;
};

export type SeedingState = SeedingLimits & {
	choice: SeedingChoice;
	inPlace: boolean;
	leftover: boolean;
};

export type GlobalSeedingKind =
	| "off"
	| "forever"
	| "ratio"
	| "minutes"
	| "both";

export type GlobalSeedingSummary = SeedingLimits & { kind: GlobalSeedingKind };

function readFlag(value: unknown): boolean | null {
	if (typeof value === "boolean") {
		return value;
	}
	if (typeof value === "number") {
		return value !== 0;
	}
	if (typeof value === "string") {
		const normalized = value.trim().toLowerCase();
		if (["true", "1", "yes", "on"].includes(normalized)) {
			return true;
		}
		if (["false", "0", "no", "off"].includes(normalized)) {
			return false;
		}
	}
	return null;
}

function readLimit(value: unknown): number {
	if (typeof value !== "number" && typeof value !== "string") {
		return 0;
	}
	const parsed = typeof value === "number" ? value : Number(value.trim());
	return Number.isFinite(parsed) && parsed > 0 ? parsed : 0;
}

function readMinutes(value: unknown): number {
	return Math.round(readLimit(value));
}

function isPresent(value: unknown): boolean {
	return value !== undefined && value !== null;
}

export function seedingRuleOptions(
	choice: SeedingChoice,
	{ ratio, minutes }: SeedingLimits,
): SeedingRuleOptions | null {
	switch (choice) {
		case "global":
			return { keepSeeding: null, seedTime: null, seedRatio: null };
		case "forever":
			return { keepSeeding: "true", seedTime: "0", seedRatio: "0" };
		case "none":
			return { keepSeeding: "false", seedTime: "0", seedRatio: "0" };
		case "limits": {
			const nextRatio = readLimit(ratio);
			const nextMinutes = readMinutes(minutes);
			if (nextRatio <= 0 && nextMinutes <= 0) {
				return null;
			}
			return {
				keepSeeding: "true",
				seedTime: String(nextMinutes),
				seedRatio: String(nextRatio),
			};
		}
		default:
			return null;
	}
}

export function seedingChoiceOf(
	options: Record<string, unknown> = {},
): SeedingState {
	const inPlace =
		options.risukoSeedInPlace === true || options.risukoSeedInPlace === "true";
	if (inPlace) {
		return {
			choice: "forever",
			ratio: 0,
			minutes: 0,
			inPlace: true,
			leftover: false,
		};
	}
	const ratio = readLimit(options.seedRatio);
	const minutes = readMinutes(options.seedTime);
	const keep = readFlag(options.keepSeeding);
	if (keep === null) {
		return {
			choice: "global",
			ratio,
			minutes,
			inPlace: false,
			leftover: isPresent(options.seedTime) || isPresent(options.seedRatio),
		};
	}
	const choice: SeedingChoice =
		ratio > 0 || minutes > 0 ? "limits" : keep ? "forever" : "none";
	return { choice, ratio, minutes, inPlace: false, leftover: false };
}

export function globalSeedingSummary({
	keepSeeding,
	seedTime,
	seedRatio,
}: {
	keepSeeding?: unknown;
	seedTime?: unknown;
	seedRatio?: unknown;
}): GlobalSeedingSummary {
	const ratio = readLimit(seedRatio);
	const minutes = readMinutes(seedTime);
	const keep = readFlag(keepSeeding) === true || ratio > 0 || minutes > 0;
	let kind: GlobalSeedingKind = "forever";
	if (!keep) {
		kind = "off";
	} else if (ratio > 0 && minutes > 0) {
		kind = "both";
	} else if (ratio > 0) {
		kind = "ratio";
	} else if (minutes > 0) {
		kind = "minutes";
	}
	return { kind, ratio, minutes };
}
