import { TASK_STATUS } from "@shared/constants";
import type { DownloadStatsMinuteInput } from "@shared/types/stats";
import type { DownloadFile, DownloadTask, PeerInfo } from "@shared/types/task";
import { calcProgress, checkTaskIsBT, getTaskName } from "@shared/utils";
import logger from "@shared/utils/logger";
import { defineStore } from "pinia";
import api from "@/api";
import { useAppStore } from "@/store/app";
import { reuseUnchangedRows, TASK_LIST_KEYS } from "@/utils/taskRows";

const DEFAULT_TASKS_PER_PAGE = 20;
const TASKS_PER_PAGE_OPTIONS = [10, 20, 30, 40, 50];
const TASKS_PER_PAGE_STORAGE_KEY = "risuko.tasks-per-page";
const SORT_BY_STORAGE_KEY = "risuko.task-sort-by";
const SORT_ORDER_STORAGE_KEY = "risuko.task-sort-order";

export const SPEED_HISTORY_LIMIT = 60;

const SPEED_HISTORY_GID_LIMIT = 200;

type SpeedSample = { download: number; upload: number };

const speedHistoryCache = new Map<string, SpeedSample[]>();
type StatsTaskAccumulator = {
	gid: string;
	kind: string;
	firstCompletedLength: number;
	completedLength: number;
	downloadSpeedSum: number;
	uploadSpeedSum: number;
	samples: number;
};
const statsAccumulator = new Map<string, StatsTaskAccumulator>();
let statsMinute: number | null = null;
let statsMonth = "";

export function getSpeedHistory(gid: string): SpeedSample[] {
	return speedHistoryCache.get(gid) || [];
}

function minuteStartSeconds(ms = Date.now()): number {
	return Math.floor(ms / 60000) * 60;
}

function monthLabel(ms = Date.now()): string {
	const date = new Date(ms);
	const month = `${date.getMonth() + 1}`.padStart(2, "0");
	return `${date.getFullYear()}-${month}`;
}

function nonNegativeNumber(value: unknown): number {
	const parsed = Number(value || 0);
	return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : 0;
}

function flushStatsMinute(): Promise<void> {
	if (statsMinute === null || statsAccumulator.size === 0) {
		statsAccumulator.clear();
		return Promise.resolve();
	}

	const input: DownloadStatsMinuteInput = {
		minute: statsMinute,
		month: statsMonth,
		tasks: [...statsAccumulator.values()],
	};
	statsAccumulator.clear();

	return api
		.recordDownloadStatsMinute(input)
		.then(() =>
			import("@/store/sync").then(({ useSyncStore }) => {
				useSyncStore().markCategoryChanged("stats");
			}),
		)
		.catch((err) => {
			logger.warn(
				"[Risuko] recordDownloadStatsMinute failed:",
				(err as Error).message,
			);
		});
}

export function flushDownloadStatsMinute(): Promise<void> {
	return flushStatsMinute();
}

function ensureStatsMinute(nowMs = Date.now()) {
	const minute = minuteStartSeconds(nowMs);
	if (statsMinute !== null && minute !== statsMinute) {
		flushStatsMinute();
	}
	if (statsMinute === null || minute !== statsMinute) {
		statsMinute = minute;
		statsMonth = monthLabel(nowMs);
	}
}

function collectStatsSample(
	task: DownloadTask,
	download: number,
	upload: number,
) {
	if (!task.gid) {
		return;
	}
	ensureStatsMinute();
	const kind = (task.kind || (checkTaskIsBT(task) ? "torrent" : "unknown"))
		.trim()
		.toLowerCase();
	const current = statsAccumulator.get(task.gid) || {
		gid: task.gid,
		kind: kind || "unknown",
		firstCompletedLength: nonNegativeNumber(task.completedLength),
		completedLength: 0,
		downloadSpeedSum: 0,
		uploadSpeedSum: 0,
		samples: 0,
	};
	current.kind = kind || current.kind;
	current.completedLength = Math.max(
		current.completedLength,
		nonNegativeNumber(task.completedLength),
	);
	current.downloadSpeedSum += download;
	current.uploadSpeedSum += upload;
	current.samples += 1;
	statsAccumulator.set(task.gid, current);
}

function sampleSpeedsFromTasks(tasks: DownloadTask[]): boolean {
	ensureStatsMinute();
	let changed = false;
	for (const task of tasks) {
		const status = task.status;
		const isActive =
			status === TASK_STATUS.ACTIVE || status === TASK_STATUS.SEEDING;
		if (!isActive || !task.gid) {
			continue;
		}
		const isBT = checkTaskIsBT(task);
		const isSeeder = isBT && task.seeder === "true";
		const download = isSeeder
			? 0
			: Math.max(0, Number(task.downloadSpeed || 0));
		const upload = isBT ? Math.max(0, Number(task.uploadSpeed || 0)) : 0;
		collectStatsSample(task, download, upload);
		const sample: SpeedSample = { download, upload };
		const prev = speedHistoryCache.get(task.gid) || [];
		const next = [...prev, sample].slice(-SPEED_HISTORY_LIMIT);
		speedHistoryCache.delete(task.gid);
		speedHistoryCache.set(task.gid, next);
		while (speedHistoryCache.size > SPEED_HISTORY_GID_LIMIT) {
			const oldest = speedHistoryCache.keys().next().value;
			if (oldest === undefined) {
				break;
			}
			speedHistoryCache.delete(oldest);
		}
		changed = true;
	}
	return changed;
}

type TaskSortBy = "default" | "name" | "size" | "time";
type TaskSortOrder = "asc" | "desc";
type DisplayTask = DownloadTask & {
	_displayKey: string;
};

const clampTasksPerPage = (value: number) => {
	const normalized = Number(value);
	if (!Number.isFinite(normalized)) {
		return DEFAULT_TASKS_PER_PAGE;
	}

	const intValue = Math.floor(normalized);
	if (TASKS_PER_PAGE_OPTIONS.includes(intValue)) {
		return intValue;
	}

	return DEFAULT_TASKS_PER_PAGE;
};

const loadTasksPerPage = () => {
	const saved = localStorage.getItem(TASKS_PER_PAGE_STORAGE_KEY);
	if (saved === null) {
		return DEFAULT_TASKS_PER_PAGE;
	}
	return clampTasksPerPage(Number(saved));
};

const loadSortBy = (): TaskSortBy => {
	const saved = localStorage.getItem(SORT_BY_STORAGE_KEY);
	if (saved === "name" || saved === "size" || saved === "time") {
		return saved;
	}
	return "default";
};

const loadSortOrder = (): TaskSortOrder => {
	const saved = localStorage.getItem(SORT_ORDER_STORAGE_KEY);
	if (saved === "desc") {
		return "desc";
	}
	return "asc";
};

let fetchListSeq = 0;

const getTaskSortName = (task: DownloadTask | DisplayTask): string => {
	return getTaskName(task, { defaultName: "", maxLen: -1 }).toLowerCase();
};

const getTaskSortSize = (task: DownloadTask | DisplayTask): number => {
	return Number(task.totalLength) || 0;
};

const getTaskSortTime = (task: DownloadTask | DisplayTask): number => {
	return Number(task.createdAt) || 0;
};

export const useTaskStore = defineStore("task", {
	state: () => ({
		currentList: "active",
		taskDetailVisible: false,
		currentTaskGid: "",
		currentTaskItem: null as (DownloadTask & { peers?: PeerInfo[] }) | null,
		currentTaskFiles: [] as DownloadFile[],
		currentTaskPeers: [] as PeerInfo[],
		seedingList: [] as string[],
		taskList: [] as DownloadTask[],
		selectedGidList: [] as string[],
		speedHistoryRev: 0,
		taskOrderMap: {
			all: [] as string[],
			active: [] as string[],
			waiting: [] as string[],
			scheduled: [] as string[],
			completed: [] as string[],
			stopped: [] as string[],
		},
		taskCountMap: {
			all: 0,
			active: 0,
			waiting: 0,
			scheduled: 0,
			completed: 0,
			stopped: 0,
		} as Record<string, number>,
		tasksPerPage: loadTasksPerPage(),
		filterText: "",
		filterTag: "",
		sortBy: loadSortBy() as TaskSortBy,
		sortOrder: loadSortOrder() as TaskSortOrder,
		currentPageMap: {
			all: 1,
			active: 1,
			waiting: 1,
			scheduled: 1,
			completed: 1,
			stopped: 1,
		},
	}),
	getters: {
		currentPage(state) {
			return state.currentPageMap[state.currentList] || 1;
		},
		displayTaskList(state) {
			return state.taskList as DisplayTask[];
		},
		totalLength(state) {
			return state.taskList.reduce(
				(sum, task) => sum + Number(task.totalLength || 0),
				0,
			);
		},
		totalCompletedLength(state) {
			return state.taskList.reduce(
				(sum, task) => sum + Number(task.completedLength || 0),
				0,
			);
		},
		totalProgressPercent() {
			const result = calcProgress(
				this.totalLength,
				this.totalCompletedLength,
				1,
			);
			return `${result}`.replace(/\.0$/, "");
		},
		filteredTaskList(state) {
			const filter = state.filterText.trim().toLowerCase();
			const tagFilter = state.filterTag.trim().toLowerCase();
			let list = this.displayTaskList;
			if (filter) {
				list = list.filter((task: DisplayTask) => {
					const name = getTaskSortName(task);
					return name.includes(filter);
				});
			}
			if (tagFilter) {
				list = list.filter((task: DisplayTask) => {
					const taskTag = (task.tag || "").toLowerCase();
					return taskTag === tagFilter;
				});
			}
			return list;
		},
		sortedTaskList(state) {
			const list = this.filteredTaskList;
			if (state.sortBy === "default") {
				return list;
			}
			const sorted = [...list];
			const order = state.sortOrder === "desc" ? -1 : 1;
			sorted.sort((a: DisplayTask, b: DisplayTask) => {
				let cmp = 0;
				switch (state.sortBy) {
					case "name":
						cmp = getTaskSortName(a).localeCompare(getTaskSortName(b));
						break;
					case "size":
						cmp = getTaskSortSize(a) - getTaskSortSize(b);
						break;
					case "time":
						cmp = getTaskSortTime(a) - getTaskSortTime(b);
						break;
				}
				return cmp * order;
			});
			return sorted;
		},
		totalPages(state) {
			return Math.max(
				1,
				Math.ceil(this.sortedTaskList.length / state.tasksPerPage),
			);
		},
		paginatedTaskList(state) {
			const currentPage = state.currentPageMap[state.currentList] || 1;
			const start = (currentPage - 1) * state.tasksPerPage;
			const end = start + state.tasksPerPage;
			return this.sortedTaskList.slice(start, end);
		},
		selectedGids(state): string[] {
			return state.selectedGidList;
		},
		selectedTaskRows(state): DisplayTask[] {
			if (state.selectedGidList.length === 0) {
				return [];
			}
			const want = new Set(state.selectedGidList);
			return (this.displayTaskList as DisplayTask[]).filter((task) =>
				want.has(task._displayKey || task.gid),
			);
		},
	},
	actions: {
		applyTaskOrder(type: string, tasks: DownloadTask[] = []) {
			const order = this.taskOrderMap[type];
			if (!order || order.length === 0 || tasks.length < 2) {
				return tasks;
			}

			const orderIndex = new Map<string, number>(
				order.map((gid, index) => [gid, index]),
			);
			const fallbackIndex = new Map<string, number>(
				tasks.map((task, index) => [task.gid, index]),
			);

			return [...tasks].sort(
				(a, b) =>
					(orderIndex.get(a.gid) ?? Number.MAX_SAFE_INTEGER) -
						(orderIndex.get(b.gid) ?? Number.MAX_SAFE_INTEGER) ||
					(fallbackIndex.get(a.gid) || 0) - (fallbackIndex.get(b.gid) || 0),
			);
		},
		updateTaskOrder(type: string, gids: string[] = []) {
			this.taskOrderMap = {
				...this.taskOrderMap,
				[type]: [...gids],
			};
		},
		changeCurrentList(currentList: string) {
			this.currentList = currentList;
			this.selectedGidList = [];
			this.filterText = "";
			this.filterTag = "";
			this.fetchList();
		},
		updateCurrentPage(listType: string, page: number) {
			const maxPage = this.totalPages;
			const normalizedPage = Math.min(
				Math.max(Math.floor(Number(page) || 1), 1),
				maxPage,
			);
			this.currentPageMap = {
				...this.currentPageMap,
				[listType]: normalizedPage,
			};
			this.selectedGidList = [];
		},
		ensurePageInRange(listType = this.currentList) {
			const currentPage = this.currentPageMap[listType] || 1;
			const maxPage = this.totalPages;
			if (currentPage > maxPage) {
				this.updateCurrentPage(listType, maxPage);
			}
			if (currentPage < 1) {
				this.updateCurrentPage(listType, 1);
			}
		},
		changeCurrentPage(page: number) {
			this.updateCurrentPage(this.currentList, page);
		},
		setTasksPerPage(value: number) {
			const next = clampTasksPerPage(value);
			this.tasksPerPage = next;
			this.ensurePageInRange(this.currentList);
			localStorage.setItem(TASKS_PER_PAGE_STORAGE_KEY, `${next}`);
		},
		setFilterText(text: string) {
			this.filterText = text;
			this.ensurePageInRange(this.currentList);
		},
		setFilterTag(tag: string) {
			this.filterTag = tag;
			this.ensurePageInRange(this.currentList);
		},
		setSortBy(sortBy: TaskSortBy) {
			this.sortBy = sortBy;
			this.ensurePageInRange(this.currentList);
			localStorage.setItem(SORT_BY_STORAGE_KEY, sortBy);
		},
		setSortOrder(order: TaskSortOrder) {
			this.sortOrder = order;
			this.ensurePageInRange(this.currentList);
			localStorage.setItem(SORT_ORDER_STORAGE_KEY, order);
		},
		toggleSortOrder() {
			this.setSortOrder(this.sortOrder === "asc" ? "desc" : "asc");
		},
		async fetchList() {
			const type = this.currentList;
			const seq = ++fetchListSeq;
			try {
				let fetchType = type;
				if (type === "completed") {
					fetchType = "stopped";
				}
				const rawData = (await api.fetchTaskList({
					type: fetchType,
					keys: TASK_LIST_KEYS,
				})) as DownloadTask[];

				if (type !== this.currentList || seq !== fetchListSeq) {
					return [];
				}

				let data: DownloadTask[];
				if (type === "completed") {
					data = rawData.filter(
						(task: DownloadTask) => task.status === TASK_STATUS.COMPLETE,
					);
				} else if (type === "stopped") {
					data = rawData.filter(
						(task: DownloadTask) => task.status !== TASK_STATUS.COMPLETE,
					);
				} else {
					data = rawData;
				}

				for (const task of data) {
					(task as DisplayTask)._displayKey = task.gid;
				}
				data = reuseUnchangedRows(this.taskList, data);
				const orderedData = this.applyTaskOrder(type, data);
				this.taskList = orderedData;
				this.taskCountMap = {
					...this.taskCountMap,
					[type]: orderedData.length,
				};
				this.ensurePageInRange(type);
				this.updateTaskOrder(
					type,
					orderedData.map((task) => task.gid),
				);

				const visibleKeys = new Set(
					this.displayTaskList.map((row) => row._displayKey),
				);
				this.selectedGidList = this.selectedGidList.filter((key) =>
					visibleKeys.has(key),
				);
				return orderedData;
			} catch (err: unknown) {
				logger.warn("[Risuko] fetchList failed:", (err as Error).message);
				if (type !== this.currentList || seq !== fetchListSeq) {
					return [];
				}
				this.taskList = [];
				this.selectedGidList = [];
				return [];
			}
		},
		updateTaskCountsFromStat(stat: Record<string, number>) {
			const activeCount = stat.numActive || 0;
			const scheduledCount = stat.numScheduled || 0;
			const waitingCount = Math.max(0, (stat.numWaiting || 0) - scheduledCount);
			const completedCount = stat.numCompleted || 0;
			const stoppedCount = stat.numStoppedError || 0;
			const prev = this.taskCountMap;
			const next = {
				all:
					activeCount +
					waitingCount +
					scheduledCount +
					completedCount +
					stoppedCount,
				active: activeCount,
				waiting: waitingCount,
				scheduled: scheduledCount,
				completed: completedCount,
				stopped: stoppedCount,
			};
			if (
				prev.all !== next.all ||
				prev.active !== next.active ||
				prev.waiting !== next.waiting ||
				prev.scheduled !== next.scheduled ||
				prev.completed !== next.completed ||
				prev.stopped !== next.stopped
			) {
				this.taskCountMap = next;
			}
		},
		async sampleActiveSpeeds(prefetched?: DownloadTask[]) {
			try {
				if (prefetched) {
					if (sampleSpeedsFromTasks(prefetched) && !document.hidden) {
						this.speedHistoryRev++;
					}
					return;
				}
				if (this.currentList === "active" && this.taskList.length > 0) {
					if (sampleSpeedsFromTasks(this.taskList) && !document.hidden) {
						this.speedHistoryRev++;
					}
					return;
				}

				const tasks = (await api.fetchTaskList({
					type: "active",
					keys: [
						"gid",
						"kind",
						"status",
						"completedLength",
						"downloadSpeed",
						"uploadSpeed",
						"seeder",
						"bittorrent",
					],
				})) as DownloadTask[];
				if (sampleSpeedsFromTasks(tasks) && !document.hidden) {
					this.speedHistoryRev++;
				}
			} catch {}
		},
		selectTasks(list: string[]) {
			this.selectedGidList = list;
		},
		selectAllTask() {
			const selectableKeys = this.paginatedTaskList
				.map((task) => task._displayKey || task.gid || "")
				.filter(Boolean);
			const selectedKeys = new Set(this.selectedGidList);
			const allSelected =
				selectableKeys.length > 0 &&
				selectableKeys.every((key) => selectedKeys.has(key));
			this.selectedGidList = allSelected ? [] : selectableKeys;
		},
		async fetchItem(gid: string) {
			try {
				const data = await api.fetchTaskItem({ gid });
				this.updateCurrentTaskItem(data);
				return data;
			} catch (err: unknown) {
				logger.warn("[Risuko] fetchItem failed:", (err as Error).message);
				return null;
			}
		},
		async fetchItemWithPeers(gid: string) {
			try {
				const data = await api.fetchTaskItemWithPeers({ gid });
				if (!data) {
					return null;
				}
				this.updateCurrentTaskItem(data);
				return data;
			} catch (err: unknown) {
				logger.warn(
					"[Risuko] fetchItemWithPeers failed:",
					(err as Error).message,
				);
				return null;
			}
		},
		async showTaskDetailByGid(gid: string) {
			try {
				const task = await api.fetchTaskItem({ gid });
				if (!task) {
					return null;
				}
				this.updateCurrentTaskItem(task);
				this.currentTaskGid = task.gid;
				this.taskDetailVisible = true;
				return task;
			} catch (err: unknown) {
				logger.warn(
					"[Risuko] showTaskDetailByGid failed:",
					(err as Error).message,
				);
				return null;
			}
		},
		showTaskDetail(task: DisplayTask) {
			this.updateCurrentTaskItem(task);
			this.currentTaskGid = task.gid;
			this.taskDetailVisible = true;
		},
		hideTaskDetail() {
			this.taskDetailVisible = false;
		},
		updateCurrentTaskItem(
			task: (DownloadTask & { peers?: PeerInfo[] }) | null,
		) {
			this.currentTaskItem = task;
			if (task) {
				this.currentTaskFiles = task.files;
				this.currentTaskPeers = task.peers ?? this.currentTaskPeers;
			} else {
				this.currentTaskFiles = [];
				this.currentTaskPeers = [];
			}
		},
		updateCurrentTaskGid(gid: string) {
			this.currentTaskGid = gid;
		},
		updateCurrentTaskDetail() {
			return this.currentTaskGid
				? this.fetchItemWithPeers(this.currentTaskGid)
				: null;
		},
		addUri(data: {
			uris: string[];
			outs: string[];
			options: Record<string, string>;
		}) {
			const { uris, outs, options } = data;
			return api.addUri({ uris, outs, options }).then(() => {
				this.fetchList();
				const appStore = useAppStore();
				appStore.updateAddTaskOptions({});
			});
		},
		addMedia(data: { url: string; options?: Record<string, unknown> }) {
			const { url, options } = data;
			return api.addMedia({ url, options }).then(() => {
				this.fetchList();
				const appStore = useAppStore();
				appStore.updateAddTaskOptions({});
			});
		},
		addTorrent(data: { torrentPath: string; options: Record<string, string> }) {
			const { torrentPath, options } = data;
			return api.addTorrent({ torrentPath, options }).then(() => {
				this.fetchList();
				const appStore = useAppStore();
				appStore.updateAddTaskOptions({});
			});
		},
		async addTorrents(data: {
			paths: string[];
			options: Record<string, unknown>;
		}) {
			const { paths, options } = data;
			const results = await api.addTorrentsByPaths({ paths, options });
			this.fetchList();
			const appStore = useAppStore();
			appStore.updateAddTaskOptions({});
			return results;
		},
		async addMetalinks(data: {
			paths: string[];
			options: Record<string, unknown>;
		}) {
			const { paths, options } = data;
			const results = await api.addMetalinksByPaths({ paths, options });
			this.fetchList();
			const appStore = useAppStore();
			appStore.updateAddTaskOptions({});
			return results;
		},
		async addNzbs(data: { paths: string[]; options: Record<string, unknown> }) {
			const { paths, options } = data;
			const results = await api.addNzbsByPaths({ paths, options });
			this.fetchList();
			const appStore = useAppStore();
			appStore.updateAddTaskOptions({});
			return results;
		},
		getTaskOption(gid: string) {
			return api.getOption({ gid }).catch((err: unknown) => {
				logger.warn("[Risuko] getTaskOption failed:", (err as Error).message);
				return {};
			});
		},
		changeTaskOption(payload: {
			gid: string;
			options: Record<string, string>;
		}) {
			const { gid, options } = payload;
			return api.changeOption({ gid, options });
		},
		removeTask(task: DownloadTask) {
			const { gid } = task;

			if (gid === this.currentTaskGid) {
				this.hideTaskDetail();
			}

			return api
				.removeTask({ gid })
				.then(() => api.removeTaskRecord({ gid }).catch(() => undefined))
				.finally(() => {
					speedHistoryCache.delete(gid);
					useAppStore().clearCloudflareRetryFlag(gid);
					this.fetchList();
					this.saveSession();
				});
		},
		forcePauseTask(task: Pick<DownloadTask, "gid" | "status">) {
			const { gid, status } = task;
			if (status !== TASK_STATUS.ACTIVE) {
				return Promise.resolve(true);
			}

			return api.forcePauseTask({ gid }).finally(() => {
				this.fetchList();
				this.saveSession();
			});
		},
		pauseTask(task: DownloadTask) {
			const { gid } = task;
			const isBT = checkTaskIsBT(task);
			const promise = isBT
				? api.forcePauseTask({ gid })
				: api.pauseTask({ gid });
			promise.finally(() => {
				this.fetchList();
				this.saveSession();
			});
			return promise;
		},
		resumeTask(task: DownloadTask) {
			const { gid } = task;
			return api.resumeTask({ gid }).finally(() => {
				this.fetchList();
				this.saveSession();
			});
		},
		pauseAllTask() {
			return api
				.pauseAllTask()
				.catch(() => {
					return api.forcePauseAllTask();
				})
				.finally(() => {
					this.fetchList();
					this.saveSession();
				});
		},
		resumeAllTask() {
			return api.resumeAllTask().finally(() => {
				this.fetchList();
				this.saveSession();
			});
		},
		addToSeedingList(gid: string) {
			if (this.seedingList.includes(gid)) {
				return;
			}

			this.seedingList = [...this.seedingList, gid];
		},
		removeFromSeedingList(gid: string) {
			this.seedingList = this.seedingList.filter((g) => g !== gid);
		},
		stopSeeding({ gid }: { gid: string }) {
			return this.pauseTask({ gid, status: "active" }).then(() => {
				const options = {
					seedTime: 0,
				};
				return this.changeTaskOption({ gid, options });
			});
		},
		removeTaskRecord(task: DownloadTask) {
			const { gid, status } = task;
			if (gid === this.currentTaskGid) {
				this.hideTaskDetail();
			}

			const { ERROR, COMPLETE, REMOVED } = TASK_STATUS;
			if ([ERROR, COMPLETE, REMOVED].indexOf(status) === -1) {
				return;
			}
			return api.removeTaskRecord({ gid }).finally(() => this.fetchList());
		},
		saveSession() {
			api.saveSession();
		},
		purgeTaskRecord() {
			return api.purgeTaskRecord().finally(() => this.fetchList());
		},
		toggleTask(task: DownloadTask) {
			const { status } = task;
			const { ACTIVE, WAITING, PAUSED } = TASK_STATUS;
			if (status === ACTIVE) {
				return this.pauseTask(task);
			} else if (status === WAITING || status === PAUSED) {
				return this.resumeTask(task);
			}
		},
		batchResumeSelectedTasks() {
			const gids: string[] = this.selectedGids;
			if (gids.length === 0) {
				return;
			}

			return api.batchResumeTask({ gids }).finally(() => {
				this.fetchList();
				this.saveSession();
			});
		},
		batchPauseSelectedTasks() {
			const gids: string[] = this.selectedGids;
			if (gids.length === 0) {
				return;
			}

			return api.batchPauseTask({ gids }).finally(() => {
				this.fetchList();
				this.saveSession();
			});
		},
		batchRemoveTask(gids: string[]) {
			return api
				.batchRemoveTask({ gids })
				.then(() =>
					Promise.all(
						gids.map((gid) =>
							api.removeTaskRecord({ gid }).catch(() => undefined),
						),
					),
				)
				.finally(() => {
					const appStore = useAppStore();
					for (const gid of gids) {
						speedHistoryCache.delete(gid);
						appStore.clearCloudflareRetryFlag(gid);
					}
					this.fetchList();
					this.saveSession();
				});
		},
		async reorderTasks(gids: string[], targetGid: string, after: boolean) {
			if (gids.length === 0 || !targetGid || gids.includes(targetGid)) {
				return;
			}
			const moveSet = new Set(gids);
			const moved = this.taskList.filter((task) => moveSet.has(task.gid));
			const remaining = this.taskList.filter((task) => !moveSet.has(task.gid));
			const targetIdx = remaining.findIndex((task) => task.gid === targetGid);
			const insertAt =
				targetIdx < 0 ? remaining.length : after ? targetIdx + 1 : targetIdx;
			const nextList = [
				...remaining.slice(0, insertAt),
				...moved,
				...remaining.slice(insertAt),
			];
			this.taskList = nextList;
			this.updateTaskOrder(
				this.currentList,
				nextList.map((task) => task.gid),
			);

			try {
				await api.reorderTasks({ gids, targetGid, after });
				await this.fetchList();
				this.saveSession();
			} catch (err: unknown) {
				logger.warn("[Risuko] reorderTasks failed:", (err as Error).message);
				this.updateTaskOrder(this.currentList, []);
				await this.fetchList();
			}
		},
		async setSchedule(gid: string, startAt: number) {
			await api.setTaskSchedule({ gid, startAt });
			await this.fetchList();
			this.saveSession();
		},
		async updateTask(
			gid: string,
			patch: {
				uris?: string[];
				dir?: string;
				out?: string;
				trackers?: string[];
				options?: Record<string, unknown>;
			},
		) {
			const outcome = await api.updateTask({ gid, patch });
			await this.fetchList();
			this.saveSession();
			return outcome;
		},
		async startNow(gid: string) {
			await api.startTaskNow({ gid });
			await this.fetchList();
			this.saveSession();
		},
	},
});
