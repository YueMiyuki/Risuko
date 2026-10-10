import type { UsenetTaskOptions } from "./usenet";

interface FileUri {
	uri: string;
	status: string;
}

export interface DownloadFile {
	index: string;
	path: string;
	length: string;
	completedLength: string;
	selected: string;
	uris: FileUri[];
}

interface BitTorrentInfo {
	info?: {
		name?: string;
		dirName?: string;
	};
	infoHash?: string;
	infoHashV2?: string;
	metaVersion?: "v1" | "v2" | "hybrid";
	announceList?: string[][];
}

export interface UsenetRepairFailure {
	neededBlocks: number;
	availableBlocks: number;
	partialsRetained: boolean;
}

type Ed2kKadLookupState =
	| "disabled"
	| "bootstrapping"
	| "searching"
	| "complete"
	| "timeout"
	| "error";

interface Ed2kKadTaskStatus {
	state: Ed2kKadLookupState;
	queriedNodes: number;
	discoveredSources: number;
	error?: string | null;
}

export interface DownloadTask {
	gid: string;
	status: string;
	kind?: string;
	totalLength: string;
	completedLength: string;
	downloadSpeed: string;
	uploadSpeed: string;
	connections: string;
	dir: string;
	files: DownloadFile[];
	errorCode?: string;
	errorMessage?: string;
	createdAt?: string;
	startAt?: string;
	scheduleMissed?: boolean;
	infoHash?: string;
	infoHashV2?: string;
	metaVersion?: "v1" | "v2" | "hybrid";
	bittorrent?: BitTorrentInfo;
	seeder?: string;
	numSeeders?: string;
	ed2kLink?: string;
	ed2kKad?: Ed2kKadTaskStatus;
	numPeers?: string;
	m3u8Link?: string;
	pieceLength?: string;
	numPieces?: string;
	options?: Record<string, string>;
	tag?: string;
	usenet?: {
		options?: UsenetTaskOptions;
		files?: Array<{
			name: string;
			subject: string;
			groups: string[];
			segments: Array<{ number: number; bytes: number; messageId: string }>;
		}>;
	};
	usenetStage?: string;
	usenetWarning?: string;
	usenetRepairFailure?: UsenetRepairFailure;
	chunkProgress?: { completedLength: string; totalLength: string }[];
}

export interface PeerInfo {
	ip: string;
	port: string;
	percent: number;
	amChoking: string;
	peerChoking: string;
	seeder: string;
	peerId?: string;
	peerClientName?: string;
	amInterested?: string;
	peerInterested?: string;
	downloadSpeed?: number;
	uploadSpeed?: number;
	downloaded?: number;
	uploaded?: number;
	progress?: number;
	incoming?: boolean;
	snubbed?: boolean;
	handshaking?: boolean;
	optimisticUnchoke?: boolean;
	bitfield?: string;
}

export interface GlobalStat {
	numActive: string;
	numWaiting: string;
	numStopped: string;
	numStoppedTotal: string;
	numCompleted?: string;
	numStoppedError?: string;
	numScheduled?: string;
	numPaused?: string;
	downloadSpeed: string;
	uploadSpeed: string;
}

export interface EngineInfo {
	version: string;
	enabledFeatures: string[];
}

export interface LowSpeedEvaluationResult {
	strikeMap: Record<string, number>;
	recoverAtMap: Record<string, number>;
	recoverGids: string[];
}

export interface AutoRetryPlanResult {
	attemptMap: Record<string, number>;
	nextAttempt: number;
	delayMs: number;
}

export interface MediaFormat {
	format_id: string;
	ext: string;
	resolution: string;
	fps: number | null;
	vcodec: string;
	acodec: string;
	filesize: number | null;
	filesize_approx: number | null;
	tbr: number | null;
	abr: number | null;
	vbr: number | null;
	format_note: string;
	audio_only: boolean;
	video_only: boolean;
}

export interface MediaInfo {
	id: string;
	title: string;
	description: string | null;
	duration: number | null;
	uploader: string | null;
	upload_date: string | null;
	thumbnail: string | null;
	view_count: number | null;
	channel: string | null;
	webpage_url: string;
	formats: MediaFormat[];
	is_playlist: boolean;
	playlist_count: number | null;
}
