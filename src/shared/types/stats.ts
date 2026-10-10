interface DownloadStatsTaskInput {
	gid: string;
	kind: string;
	firstCompletedLength?: number;
	completedLength: number;
	downloadSpeedSum: number;
	uploadSpeedSum: number;
	samples: number;
}

export interface DownloadStatsMinuteInput {
	minute: number;
	month: string;
	tasks: DownloadStatsTaskInput[];
}

export interface DownloadStatsQuery {
	start: number;
	end: number;
	startMonth: string;
	endMonth: string;
}

export interface ProtocolTotal {
	protocol: string;
	receivedBytes: number;
}

export interface MonthlyProtocolTotal {
	month: string;
	total: number;
	protocols: ProtocolTotal[];
}

interface ProtocolSpeedPoint {
	protocol: string;
	downloadSpeed: number;
	uploadSpeed: number;
	receivedBytes: number;
	samples: number;
}

export interface SpeedPoint {
	minute: number;
	protocols: ProtocolSpeedPoint[];
}

export interface DownloadStatsView {
	monthly: MonthlyProtocolTotal[];
	speed: SpeedPoint[];
	protocolTotals: ProtocolTotal[];
}

export interface DownloadStatsStore {
	version: number;
	baselines: Record<string, unknown>;
	monthly: Record<string, Record<string, number>>;
	speed: Record<string, unknown>;
}
