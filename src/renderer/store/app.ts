import { ADD_TASK_TYPE } from "@shared/constants";
import type { DownloadTask, EngineInfo } from "@shared/types/task";
import { bytesToSize } from "@shared/utils";
import logger from "@shared/utils/logger";
import { defineStore } from "pinia";
import api from "@/api";
import is from "@/shims/platform";
import type { BatchQueueItem } from "@/store/batchQueue";
import { useTaskStore } from "@/store/task";
import { getSystemTheme } from "@/utils/native";

const BASE_INTERVAL = 1500;
const PER_INTERVAL = 80;
const MIN_INTERVAL = 800;
const MAX_INTERVAL = 6000;

const normalizeNonNegativeNumber = (value: unknown): number => {
	const parsed = Number(value);
	if (!Number.isFinite(parsed) || parsed <= 0) {
		return 0;
	}
	return parsed;
};

const calcRendererProgress = (
	tasks: Pick<DownloadTask, "totalLength" | "completedLength">[] = [],
) => {
	if (tasks.length === 0) {
		return -1;
	}

	let total = 0;
	let completed = 0;
	for (const task of tasks) {
		const totalLength = normalizeNonNegativeNumber(task?.totalLength);
		if (totalLength === 0) {
			continue;
		}

		total += totalLength;
		completed += normalizeNonNegativeNumber(task?.completedLength);
	}

	if (total === 0) {
		return 2;
	}

	return completed / total;
};

export const useAppStore = defineStore("app", {
	state: () => ({
		systemTheme: getSystemTheme(),
		trayFocused: false,
		aboutPanelVisible: false,
		engineInfo: {
			version: "",
			enabledFeatures: [],
		},
		interval: BASE_INTERVAL,
		stat: {
			downloadSpeed: 0,
			uploadSpeed: 0,
			numActive: 0,
			numWaiting: 0,
			numStopped: 0,
			numPaused: 0,
		},
		addTaskVisible: false,
		missedScheduledVisible: false,
		missedScheduledTasks: [] as DownloadTask[],
		addTaskType: ADD_TASK_TYPE.URI,
		addTaskUrl: "",
		addTaskTorrents: [] as { name: string; path: string }[],
		addTaskQueue: [] as BatchQueueItem[],
		addTaskOptions: {},
		progress: 0,
		androidDownloadNotificationVisible: false,
		cloudflareDialog: {
			visible: false,
			gid: "",
			host: "",
			url: "",
			taskName: "",
			retried: false,
			lastImportedNames: [] as string[],
		},
		cloudflareSkipHosts: [] as string[],
		cloudflareLastRetryGids: new Set<string>(),
	}),
	actions: {
		updateSystemTheme(theme: string) {
			this.systemTheme = theme;
		},
		updateTrayFocused(focused: boolean) {
			this.trayFocused = focused;
		},
		showAboutPanel() {
			this.aboutPanelVisible = true;
		},
		hideAboutPanel() {
			this.aboutPanelVisible = false;
		},
		async fetchEngineInfo() {
			try {
				const data = (await api.getVersion()) as unknown as EngineInfo;
				this.engineInfo = { ...this.engineInfo, ...data };
				return data;
			} catch (err: unknown) {
				logger.warn("[Risuko] fetchEngineInfo failed:", (err as Error).message);
				return null;
			}
		},
		async fetchGlobalStat() {
			try {
				const data = await api.getGlobalStat();
				const stat: Record<string, number> = {};
				Object.keys(data).forEach((key) => {
					stat[key] = Number(data[key]);
				});

				const { numActive } = stat;
				if (numActive > 0) {
					const interval = BASE_INTERVAL - PER_INTERVAL * numActive;
					this.updateInterval(interval);
				} else {
					stat.downloadSpeed = 0;
					this.increaseInterval();
				}
				const prev = this.stat as Record<string, number>;
				let changed = false;
				for (const k of Object.keys(stat)) {
					if (prev[k] !== stat[k]) {
						changed = true;
						break;
					}
				}
				if (changed) {
					this.stat = stat;
				}
				useTaskStore().updateTaskCountsFromStat(stat);
			} catch (err: unknown) {
				logger.warn("[Risuko] fetchGlobalStat failed:", (err as Error).message);
			}
		},
		increaseInterval(millisecond = 100) {
			if (this.interval < MAX_INTERVAL) {
				this.interval += millisecond;
			}
		},
		showAddTaskDialog(taskType: string) {
			this.addTaskType = taskType;
			this.addTaskVisible = true;
		},
		hideAddTaskDialog() {
			this.addTaskVisible = false;
			this.addTaskUrl = "";
			this.addTaskTorrents = [];
			this.addTaskQueue = [];
		},
		showMissedScheduled(tasks: DownloadTask[]) {
			this.missedScheduledTasks = tasks;
			this.missedScheduledVisible = true;
		},
		hideMissedScheduled() {
			this.missedScheduledVisible = false;
			this.missedScheduledTasks = [];
		},
		showCloudflareDialog(payload: {
			gid: string;
			host: string;
			url: string;
			taskName: string;
		}) {
			const retried = this.cloudflareLastRetryGids.has(payload.gid);
			this.cloudflareDialog = {
				visible: true,
				retried,
				lastImportedNames: this.cloudflareDialog.lastImportedNames,
				...payload,
			};
		},
		hideCloudflareDialog() {
			this.cloudflareDialog = {
				visible: false,
				gid: "",
				host: "",
				url: "",
				taskName: "",
				retried: false,
				lastImportedNames: [],
			};
		},
		markCloudflareRetried(gid: string, importedNames: string[]) {
			this.cloudflareLastRetryGids.add(gid);
			if (this.cloudflareDialog) {
				this.cloudflareDialog.retried = true;
				this.cloudflareDialog.lastImportedNames = importedNames;
			}
		},
		clearCloudflareRetryFlag(gid: string) {
			this.cloudflareLastRetryGids.delete(gid);
		},
		addCloudflareSkipHost(host: string) {
			if (host && !this.cloudflareSkipHosts.includes(host)) {
				this.cloudflareSkipHosts.push(host);
			}
		},
		updateAddTaskUrl(uri = "") {
			this.addTaskUrl = uri;
		},
		addTaskAddTorrents({
			fileList,
		}: {
			fileList: { name: string; path: string }[];
		}) {
			this.addTaskTorrents = [...fileList];
		},
		enqueueBatchItems(items: BatchQueueItem[]) {
			if (!items || items.length === 0) {
				return;
			}
			this.addTaskQueue = [...this.addTaskQueue, ...items];
		},
		removeBatchItem(id: string) {
			this.addTaskQueue = this.addTaskQueue.filter((item) => item.id !== id);
		},
		updateBatchItem(id: string, patch: Partial<Omit<BatchQueueItem, "id">>) {
			this.addTaskQueue = this.addTaskQueue.map((item) =>
				item.id === id
					? ({ ...item, ...patch, id: item.id } as BatchQueueItem)
					: item,
			);
		},
		resetBatchQueue() {
			this.addTaskQueue = [];
		},
		updateAddTaskOptions(options = {}) {
			this.addTaskOptions = { ...options };
		},
		updateInterval(ms: number) {
			this.interval = Math.min(MAX_INTERVAL, Math.max(MIN_INTERVAL, ms));
		},
		resetInterval() {
			this.interval = BASE_INTERVAL;
		},
		async syncAndroidDownloadNotification({
			progress,
			tasks,
		}: {
			progress: number;
			tasks: Pick<DownloadTask, "totalLength" | "completedLength">[];
		}) {
			if (!is.android()) {
				return;
			}
			if (tasks.length === 0) {
				if (!this.androidDownloadNotificationVisible) {
					return;
				}
				try {
					await api.clearAndroidDownloadNotification();
					this.androidDownloadNotificationVisible = false;
				} catch (err: unknown) {
					logger.warn(
						"[Risuko] clearAndroidDownloadNotification failed:",
						(err as Error).message,
					);
				}
				return;
			}

			let total = 0;
			let completed = 0;
			for (const task of tasks) {
				total += normalizeNonNegativeNumber(task?.totalLength);
				completed += normalizeNonNegativeNumber(task?.completedLength);
			}

			const normalizedProgress =
				Number.isFinite(progress) && progress >= 0 ? progress : 0;
			const percent = Math.max(
				0,
				Math.min(100, Math.round(normalizedProgress * 100)),
			);
			const detail =
				total > 0
					? `${bytesToSize(completed, 1)} / ${bytesToSize(total, 1)} · ${percent}%`
					: `${percent}% complete`;

			try {
				await api.updateAndroidDownloadNotification({
					progress: percent,
					activeCount: tasks.length,
					detail,
				});
				this.androidDownloadNotificationVisible = true;
			} catch (err: unknown) {
				logger.warn(
					"[Risuko] updateAndroidDownloadNotification failed:",
					(err as Error).message,
				);
			}
		},
		async fetchProgress(prefetched?: DownloadTask[]) {
			try {
				const tasks =
					prefetched ??
					(await api.fetchActiveTaskList({
						keys: ["totalLength", "completedLength"],
					}));

				if (tasks.length === 0) {
					this.progress = -1;
					await this.syncAndroidDownloadNotification({ progress: -1, tasks });
					return;
				}

				const progress = calcRendererProgress(tasks);
				this.progress = progress === 2 ? -1 : progress;
				await this.syncAndroidDownloadNotification({
					progress: this.progress,
					tasks,
				});
			} catch (err: unknown) {
				logger.warn("[Risuko] fetchProgress failed:", (err as Error).message);
			}
		},
	},
});
