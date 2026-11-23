import { SELECTED_ALL_FILES } from "@shared/constants";
import type { MediaFormat } from "@shared/types/task";
import { isMediaUri } from "@shared/utils";

type BatchItemKind = "torrent" | "uri" | "magnet" | "metalink" | "nzb";

type BatchItemStatus = "queued" | "submitting" | "success" | "failed";

type BatchItemResolveState =
	| "idle"
	| "loading"
	| "ready"
	| "error"
	| "preview-disabled";

interface BatchItemFileSummary {
	idx: number;
	name: string;
	path: string;
	length: number;
}

export interface BatchQueueItem {
	id: string;
	kind: BatchItemKind;
	label: string;

	path?: string;
	displayName?: string;
	resolveState: BatchItemResolveState;
	resolveError?: string;
	previewDisabled?: boolean;
	fileCount?: number;
	totalLength?: number;

	uri?: string;
	magnetFiles?: BatchItemFileSummary[];

	mirrors?: string[];

	isMedia?: boolean;
	forceYtdlp?: boolean;
	mediaFormatId?: string;
	mediaFormats?: MediaFormat[];
	mediaInfoState?: BatchItemResolveState;
	mediaInfoError?: string;
	mediaTitle?: string;

	selectFile: string;
	out?: string;

	status: BatchItemStatus;
	gid?: string;
	error?: string;
	refusal?: string;
}

const createFileBatchItem = (
	kind: Extract<BatchItemKind, "torrent" | "metalink" | "nzb">,
	resolveState: BatchItemResolveState,
	path: string,
	name?: string,
): BatchQueueItem => {
	const segs = `${path}`.split(/[/\\]/);
	const fallback = segs[segs.length - 1] || path;
	return {
		id: crypto.randomUUID(),
		kind,
		label: name || fallback,
		path,
		displayName: name || fallback,
		resolveState,
		selectFile: SELECTED_ALL_FILES,
		status: "queued",
	};
};

const createTorrentBatchItem = (path: string, name?: string): BatchQueueItem =>
	createFileBatchItem("torrent", "idle", path, name);

const createMetalinkBatchItem = (path: string, name?: string): BatchQueueItem =>
	createFileBatchItem("metalink", "preview-disabled", path, name);

const createNzbBatchItem = (path: string, name?: string): BatchQueueItem =>
	createFileBatchItem("nzb", "preview-disabled", path, name);

const TORRENT_EXTS = ["torrent"];
const METALINK_EXTS = ["meta4", "metalink"];
const NZB_EXTS = ["nzb"];
const extRe = (exts: string[]) => new RegExp(`\\.(${exts.join("|")})$`, "i");
const TORRENT_RE = extRe(TORRENT_EXTS);
const METALINK_RE = extRe(METALINK_EXTS);
const NZB_RE = extRe(NZB_EXTS);
export const TASK_FILE_RE = extRe([
	...TORRENT_EXTS,
	...METALINK_EXTS,
	...NZB_EXTS,
]);

export const batchItemForFilePath = (
	path: string,
	name?: string,
): BatchQueueItem | null => {
	if (TORRENT_RE.test(path)) {
		return createTorrentBatchItem(path, name);
	}
	if (METALINK_RE.test(path)) {
		return createMetalinkBatchItem(path, name);
	}
	if (NZB_RE.test(path)) {
		return createNzbBatchItem(path, name);
	}
	return null;
};

export const createUriBatchItem = (
	uri: string,
	mediaRefusal?: string,
): BatchQueueItem => {
	const trimmed = uri.trim();
	const isMagnet = trimmed.toLowerCase().startsWith("magnet:");
	const label = trimmed.length > 80 ? `${trimmed.slice(0, 77)}…` : trimmed;
	const media = !isMagnet && isMediaUri(trimmed);
	const refused = media && !!mediaRefusal;
	return {
		id: crypto.randomUUID(),
		kind: isMagnet ? "magnet" : "uri",
		label,
		uri: trimmed,
		resolveState: isMagnet ? "idle" : "ready",
		selectFile: SELECTED_ALL_FILES,
		status: refused ? "failed" : "queued",
		...(refused ? { error: mediaRefusal, refusal: mediaRefusal } : {}),
		isMedia: media && !refused,
		mediaInfoState: "idle",
	};
};
