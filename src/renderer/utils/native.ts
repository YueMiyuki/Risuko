import {
	APP_THEME,
	TASK_STATUS,
	TEMP_DOWNLOAD_SUFFIX,
} from "@shared/constants";
import type { DownloadTask } from "@shared/types/task";
import {
	getFileNameFromFile,
	isMagnetTask,
	stripTempDownloadSuffix,
} from "@shared/utils";
import logger from "@shared/utils/logger";
import { invoke } from "@tauri-apps/api/core";
import { toast } from "vue-sonner";

export function safUriToFilesystemPath(uri: string): string {
	if (typeof uri !== "string" || !uri.startsWith("content://")) {
		return uri;
	}
	const match = uri.match(
		/com\.android\.externalstorage\.documents\/(?:tree|document)\/([^?#/]+)/,
	);
	if (!match) {
		return uri;
	}
	const decoded = decodeURIComponent(match[1]);
	const sep = decoded.indexOf(":");
	if (sep < 0) {
		return uri;
	}
	const storage = decoded.slice(0, sep);
	const rel = decoded.slice(sep + 1);
	if (storage === "primary") {
		return rel ? `/storage/emulated/0/${rel}` : "/storage/emulated/0";
	}
	return rel ? `/storage/${storage}/${rel}` : `/storage/${storage}`;
}

function joinPath(...parts: string[]): string {
	const joined = parts.filter(Boolean).join("/");
	return joined.replace(/[/\\]+/g, "/");
}

function dirname(path = ""): string {
	const value = `${path || ""}`.replace(/[/\\]+$/g, "");
	const index = Math.max(value.lastIndexOf("/"), value.lastIndexOf("\\"));
	if (index < 0) {
		return value;
	}
	if (index === 0) {
		return value.charAt(0) === "/" || value.charAt(0) === "\\"
			? value.charAt(0)
			: value;
	}
	if (value.charCodeAt(index - 1) === 58) {
		return value.slice(0, index + 1);
	}
	return value.slice(0, index);
}

const chunkMetaPath = (partPath = ""): string => `${partPath}.chunks`;

const sleep = (ms: number): Promise<void> =>
	new Promise((resolve) => window.setTimeout(resolve, ms));

const cleanupTempSidecars = async (paths: string[]): Promise<void> => {
	let pending = [
		...new Set(paths.map((path) => `${path || ""}`.trim())),
	].filter(Boolean);
	for (const delay of [0, 250, 1000]) {
		if (pending.length === 0) {
			break;
		}
		if (delay > 0) {
			await sleep(delay);
		}
		const retry: string[] = [];
		for (const sidecarPath of pending) {
			try {
				const sidecarFound: boolean = await invoke("trash_item", {
					path: sidecarPath,
				});
				if (sidecarFound) {
					logger.info(`[Risuko] trashed temp sidecar: "${sidecarPath}"`);
				}
			} catch (sidecarErr) {
				logger.warn(
					`[Risuko] trash temp sidecar "${sidecarPath}" failed: ${sidecarErr}`,
				);
				retry.push(sidecarPath);
			}
		}
		pending = retry;
	}
};

export const showItemInFolder = async (
	fullPath: string,
	{ errorMsg, fallbackPath }: { errorMsg?: string; fallbackPath?: string } = {},
) => {
	const revealPath = `${fullPath || ""}`.trim();
	const fallback = `${fallbackPath || ""}`.trim();
	if (!revealPath && !fallback) {
		return;
	}

	logger.info(
		`[Risuko] showItemInFolder: path="${revealPath}", fallback="${fallback}"`,
	);

	try {
		await invoke("reveal_in_folder", { path: revealPath || fallback });
	} catch (err) {
		logger.warn(`[Risuko] showItemInFolder fail: ${err}`);

		if (fallback && fallback !== revealPath) {
			try {
				await invoke("reveal_in_folder", { path: fallback });
				return;
			} catch (fallbackErr) {
				logger.warn(`[Risuko] showItemInFolder fallback fail: ${fallbackErr}`);
			}
		}

		if (errorMsg) {
			toast.error(errorMsg);
		}
	}
};

export const openItem = async (fullPath: string) => {
	if (!fullPath) {
		return;
	}
	return invoke("open_path", { path: fullPath });
};

const btFolderName = (bittorrent: {
	info?: { name?: string; dirName?: string };
}): string => bittorrent?.info?.dirName || bittorrent?.info?.name || "";

export const getTaskFullPath = (
	task: DownloadTask,
	options: { normalizeCompletedPath?: boolean } = {},
): string => {
	const { normalizeCompletedPath = true } = options;
	const { dir, files, bittorrent } = task;
	let result = dir;

	if (isMagnetTask(task)) {
		return result;
	}

	const isBtMultiFile =
		!!bittorrent?.info?.name && Array.isArray(files) && files.length > 1;
	if (isBtMultiFile) {
		return joinPath(result, btFolderName(bittorrent));
	}

	const file = Array.isArray(files) && files.length > 0 ? files[0] : undefined;
	const path = file?.path || "";

	if (path) {
		result = path;
	} else if (files?.length === 1) {
		const fileName = getFileNameFromFile(file);
		if (fileName) {
			result = joinPath(result, fileName);
		}
	}

	if (normalizeCompletedPath && task?.status === TASK_STATUS.COMPLETE) {
		return stripTempDownloadSuffix(result);
	}

	return result;
};

export const getTaskRevealDir = (task: DownloadTask): string => {
	if (!task) {
		return "";
	}
	const dir = `${task?.dir || ""}`.trim();
	if (!dir || isMagnetTask(task)) {
		return dir;
	}
	const { files, bittorrent } = task;
	const file = Array.isArray(files) && files.length > 0 ? files[0] : undefined;
	const filePath = `${file?.path || ""}`.trim();
	if (
		(task as DownloadTask & { _isFileEntry?: boolean })._isFileEntry &&
		filePath
	) {
		return dirname(stripTempDownloadSuffix(filePath)) || dir;
	}
	const isBtMultiFile =
		!!bittorrent?.info?.name && Array.isArray(files) && files.length > 1;
	if (isBtMultiFile) {
		return joinPath(dir, btFolderName(bittorrent));
	}
	return dir;
};

export const getTaskRevealPath = (task: DownloadTask): string => {
	if (!task) {
		return "";
	}

	if (isMagnetTask(task)) {
		const result = `${task?.dir || ""}`.trim();
		logger.info(`[Risuko] getTaskRevealPath (magnet): "${result}"`);
		return result;
	}

	const files = Array.isArray(task?.files) ? task.files : [];
	const candidate =
		`${files.find((file) => `${file?.path || ""}`.trim())?.path || ""}`.trim();
	if (!candidate) {
		const fallback = getTaskFullPath(task);
		logger.info(
			`[Risuko] getTaskRevealPath (no file.path, using getTaskFullPath): "${fallback}"`,
		);
		return fallback;
	}

	const result =
		task?.status === TASK_STATUS.COMPLETE
			? stripTempDownloadSuffix(candidate)
			: candidate;
	logger.info(`[Risuko] getTaskRevealPath (from file.path): "${result}"`);
	return result;
};

export const finalizeCompletedDownloadPath = async (
	task: DownloadTask,
): Promise<string> => {
	if (!task) {
		return "";
	}

	if (isMagnetTask(task)) {
		return getTaskFullPath(task);
	}

	const sourcePath = getTaskFullPath(task, {
		normalizeCompletedPath: false,
	});
	if (!sourcePath.toLowerCase().endsWith(TEMP_DOWNLOAD_SUFFIX)) {
		return sourcePath;
	}

	const targetPath = stripTempDownloadSuffix(sourcePath);
	if (!targetPath || targetPath === sourcePath) {
		return sourcePath;
	}

	try {
		await invoke("rename_path", {
			fromPath: sourcePath,
			toPath: targetPath,
		});
		return targetPath;
	} catch (err) {
		logger.warn(`[Risuko] rename completed temp file failed: ${err}`);
		return sourcePath;
	}
};

export const moveTaskFilesToTrash = async (
	task: DownloadTask,
): Promise<boolean> => {
	const { dir, status, bittorrent } = task;
	const files = Array.isArray(task.files) ? task.files : [];

	if (isMagnetTask(task)) {
		await cleanupGeneratedTorrentSidecars(task);
		return true;
	}

	logger.info(
		`[Risuko] moveTaskFilesToTrash: dir="${dir}", status="${status}", files=${files.length}`,
	);

	const isBtMultiFile = !!bittorrent?.info?.name && files.length > 1;
	if (isBtMultiFile) {
		const torrentFolder = joinPath(dir, btFolderName(bittorrent));
		logger.info(`[Risuko] trashing torrent folder: "${torrentFolder}"`);
		try {
			const found: boolean = await invoke("trash_item", {
				path: torrentFolder,
			});
			if (found) {
				logger.info(`[Risuko] trashed torrent folder: "${torrentFolder}"`);
			}
		} catch (err) {
			logger.warn(`[Risuko] trash torrent folder failed: ${err}`);
			let trashedAny = false;
			for (const file of files) {
				const filePath = `${file?.path || ""}`.trim();
				if (!filePath || filePath === dir) {
					continue;
				}
				try {
					const found: boolean = await invoke("trash_item", { path: filePath });
					if (found) {
						trashedAny = true;
					}
				} catch (fileErr) {
					logger.warn(`[Risuko] trash file "${filePath}" failed: ${fileErr}`);
				}
			}
			if (!trashedAny) {
				await cleanupGeneratedTorrentSidecars(task);
				return false;
			}
		}
		await cleanupGeneratedTorrentSidecars(task);
		return true;
	}

	const path = getTaskFullPath(task);
	logger.info(
		`[Risuko] moveTaskFilesToTrash: path="${path}", dir="${dir}", status="${status}"`,
	);

	if (!path || dir === path) {
		throw new Error("task.file-path-error");
	}

	const partPath =
		status !== TASK_STATUS.COMPLETE &&
		!path.toLowerCase().endsWith(TEMP_DOWNLOAD_SUFFIX)
			? `${path}${TEMP_DOWNLOAD_SUFFIX}`
			: null;

	const tempSidecarPaths = (() => {
		const set = new Set<string>();
		const add = (value?: string | null) => {
			const p = `${value || ""}`.trim();
			if (!p) {
				return;
			}
			set.add(p);
		};

		add(`${path}.ytdl`);
		if (partPath) {
			add(`${partPath}.ytdl`);
			add(chunkMetaPath(partPath));
		} else if (path.toLowerCase().endsWith(TEMP_DOWNLOAD_SUFFIX)) {
			add(chunkMetaPath(path));
		}

		return [...set];
	})();
	let shouldCleanupTempSidecars = false;

	try {
		const found: boolean = await invoke("trash_item", { path });
		if (found) {
			logger.info(`[Risuko] trashed: "${path}"`);
			shouldCleanupTempSidecars = true;
		} else if (partPath) {
			const partFound: boolean = await invoke("trash_item", { path: partPath });
			if (partFound) {
				logger.info(`[Risuko] trashed .part file: "${partPath}"`);
			}
			shouldCleanupTempSidecars = true;
		} else {
			shouldCleanupTempSidecars = true;
		}
	} catch (err) {
		logger.warn(`[Risuko] trash "${path}" failed: ${err}`);
		if (partPath) {
			try {
				const partFound: boolean = await invoke("trash_item", {
					path: partPath,
				});
				if (partFound) {
					logger.info(`[Risuko] trashed .part file: "${partPath}"`);
					shouldCleanupTempSidecars = true;
					await cleanupGeneratedTorrentSidecars(task);
					return true;
				}
			} catch (partErr) {
				logger.warn(
					`[Risuko] trash .part "${partPath}" also failed: ${partErr}`,
				);
			}
		}
		await cleanupGeneratedTorrentSidecars(task);
		return false;
	} finally {
		if (shouldCleanupTempSidecars) {
			await cleanupTempSidecars(tempSidecarPaths);
		}
	}

	await cleanupGeneratedTorrentSidecars(task);

	return true;
};

const cleanupGeneratedTorrentSidecars = async (
	task: DownloadTask,
): Promise<number> => {
	try {
		const result = await invoke<number>(
			"cleanup_generated_torrent_sidecars_for_task",
			{
				task,
			},
		);
		return Number.isFinite(result) ? result : 0;
	} catch (err) {
		logger.warn(`[Risuko] cleanup generated torrent sidecars failed: ${err}`);
		return 0;
	}
};

export const getSystemTheme = (): string => {
	if (window.matchMedia?.("(prefers-color-scheme: dark)").matches) {
		return APP_THEME.DARK;
	}
	return APP_THEME.LIGHT;
};
