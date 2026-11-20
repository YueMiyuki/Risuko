type SpeedLimit = number | string;

const ENGINE_SPEED_LIMIT_OPTIONS = {
	maxOverallDownloadLimit: "max-overall-download-limit",
	maxOverallUploadLimit: "max-overall-upload-limit",
	btUploadRateLimit: "bt-upload-rate-limit",
} as const;

type SpeedLimitConfig = Partial<
	Record<keyof typeof ENGINE_SPEED_LIMIT_OPTIONS, SpeedLimit>
>;

export function touchesEngineSpeedLimits(config: object): boolean {
	return Object.keys(ENGINE_SPEED_LIMIT_OPTIONS).some((key) => key in config);
}

export function engineSpeedLimitOptions(
	config: SpeedLimitConfig,
	limitsOff: boolean,
): Record<string, SpeedLimit> {
	const options: Record<string, SpeedLimit> = {};
	for (const [key, option] of Object.entries(ENGINE_SPEED_LIMIT_OPTIONS)) {
		options[option] = limitsOff
			? 0
			: (config[key as keyof SpeedLimitConfig] ?? 0);
	}
	return options;
}

export function formatSpeedLimit(amount: number, unit: string): string | 0 {
	return amount > 0 ? `${amount}${unit}` : 0;
}
