export const AUTO_RETRY_DEFAULT_MAX_ATTEMPTS = 5;
export const AUTO_RETRY_MAX_ATTEMPTS_LIMIT = 1000;

const PERMANENT_ERROR_CODES = new Set([
	"300",
	"301",
	"302",
	"400",
	"401",
	"502",
	"530",
	"531",
	"533",
	"534",
	"540",
	"541",
	"542",
	"550",
	"551",
	"552",
	"553",
	"554",
]);

export function isPermanentErrorCode(code: unknown): boolean {
	if (code === null || code === undefined) {
		return false;
	}
	return PERMANENT_ERROR_CODES.has(`${code}`.trim());
}

export function normalizeMaxAttempts(raw: unknown): number {
	if (raw === null || raw === undefined) {
		return AUTO_RETRY_DEFAULT_MAX_ATTEMPTS;
	}
	if (typeof raw === "string" && raw.trim() === "") {
		return AUTO_RETRY_DEFAULT_MAX_ATTEMPTS;
	}
	const parsed = Number(raw);
	if (!Number.isFinite(parsed) || parsed < 0) {
		return AUTO_RETRY_DEFAULT_MAX_ATTEMPTS;
	}
	return Math.min(Math.floor(parsed), AUTO_RETRY_MAX_ATTEMPTS_LIMIT);
}

export function hasRetryBudget(
	attemptsSoFar: unknown,
	maxAttempts: number,
): boolean {
	if (maxAttempts === 0) {
		return true;
	}
	const attempts = Number(attemptsSoFar);
	return (Number.isFinite(attempts) ? attempts : 0) < maxAttempts;
}
