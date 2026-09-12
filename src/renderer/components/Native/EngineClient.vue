<template>
  <div style="display: none"></div>
</template>

<script lang="ts">
import type { DownloadTask } from "@shared/types/task";
import {
	formatUsenetRepairFailure,
	getTaskName,
	parseBooleanConfig,
	taskBenefitsFromLowSpeedRecovery,
} from "@shared/utils";
import logger from "@shared/utils/logger";
import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import { toast } from "vue-sonner";
import api from "@/api";
import is from "@/shims/platform";
import { useAppStore } from "@/store/app";
import { usePreferenceStore } from "@/store/preference";
import { useTaskStore } from "@/store/task";
import {
	findHttpSourceUrl,
	getErrorCodeReferenceUrl,
	openExternalUrl,
} from "@/utils/external";
import {
	finalizeCompletedDownloadPath,
	getTaskFullPath,
	showItemInFolder,
} from "@/utils/native";

const RETRY_STRATEGY_STATIC = "static";
const RETRY_STRATEGY_EXPONENTIAL = "exponential";
const AUTO_RETRY_MAX_DELAY_MS = 15 * 60 * 1000;
const LOW_SPEED_RECOVERY_COOLDOWN_MS = 30 * 1000;
const LOW_SPEED_RESTART_WAIT_MS = 500;

const normalizePositiveNumber = (
	value: unknown,
	fallback: number,
	min = 0,
	max = Number.MAX_SAFE_INTEGER,
) => {
	const parsed = Number(value);
	if (!Number.isFinite(parsed)) {
		return fallback;
	}
	return Math.min(Math.max(parsed, min), max);
};

export default {
	name: "engine-client",
	data() {
		return {
			timer: null,
			initTimer: null,
			isPolling: false,
			isDestroyed: false,
			notifyActionUnlisten: null as Promise<{
				unregister: () => Promise<void>;
			}> | null,
			noSleepDesired: false,
			noSleepApplied: false,
			noSleepSyncing: false,
			noSleepResyncNeeded: false,
			startupAutoResumeHandled: false,
			autoRetryTimerMap: {} as Record<string, ReturnType<typeof setTimeout>>,
			autoRetryAttemptMap: {} as Record<string, number>,
			autoRetryPlanTicketMap: {} as Record<string, number>,
			lowSpeedStrikeMap: {} as Record<string, number>,
			lowSpeedRecoverAtMap: {} as Record<string, number>,
			lowSpeedRecoveringMap: {} as Record<string, boolean>,
			eventUnlisteners: [] as (() => void)[],
			shutdownCountdownTimer: null as ReturnType<typeof setInterval> | null,
			shutdownCountdownSecondsLeft: 0,
			shutdownToastId: null as string | number | null,
		};
	},
	computed: {
		uploadSpeed() {
			return useAppStore().stat.uploadSpeed;
		},
		downloadSpeed() {
			return useAppStore().stat.downloadSpeed;
		},
		speed() {
			return useAppStore().stat.uploadSpeed + useAppStore().stat.downloadSpeed;
		},
		interval() {
			return useAppStore().interval;
		},
		downloading() {
			return useAppStore().stat.numActive > 0;
		},
		progress() {
			return useAppStore().progress;
		},
		seedingList() {
			return useTaskStore().seedingList;
		},
		taskDetailVisible() {
			return useTaskStore().taskDetailVisible;
		},
		currentTaskGid() {
			return useTaskStore().currentTaskGid;
		},
		currentTaskItem() {
			return useTaskStore().currentTaskItem;
		},
		taskNotification() {
			return usePreferenceStore().config.taskNotification;
		},
		traySpeedometer() {
			return parseBooleanConfig(usePreferenceStore().config.traySpeedometer);
		},
		showProgressBar() {
			return parseBooleanConfig(usePreferenceStore().config.showProgressBar);
		},
		resumeAllWhenAppLaunched() {
			return parseBooleanConfig(
				usePreferenceStore().config.resumeAllWhenAppLaunched,
			);
		},
		autoRetryEnabled() {
			return parseBooleanConfig(usePreferenceStore().config.autoRetry);
		},
		autoRetryStrategy() {
			const value = usePreferenceStore().config.autoRetryStrategy;
			return value === RETRY_STRATEGY_EXPONENTIAL
				? RETRY_STRATEGY_EXPONENTIAL
				: RETRY_STRATEGY_STATIC;
		},
		autoRetryIntervalSeconds() {
			const raw = usePreferenceStore().config.autoRetryInterval;
			return Math.floor(normalizePositiveNumber(raw, 5, 1, 300));
		},
		autoDetectLowSpeedTasks() {
			return parseBooleanConfig(
				usePreferenceStore().config.autoDetectLowSpeedTasks,
			);
		},
		lowSpeedThresholdBytes() {
			const raw = usePreferenceStore().config.lowSpeedThreshold;
			const kb = normalizePositiveNumber(raw, 20, 1, 10240);
			return Math.floor(kb * 1024);
		},
		lowSpeedStrikeThreshold() {
			const raw = usePreferenceStore().config.lowSpeedStrikeThreshold;
			return Math.floor(normalizePositiveNumber(raw, 5, 1, 20));
		},
		preventSleepWhileDownloading() {
			const raw = usePreferenceStore().config.preventSleepWhileDownloading;
			return raw === undefined ? true : parseBooleanConfig(raw);
		},
		shutdownWhenComplete() {
			const raw = usePreferenceStore().config.shutdownWhenComplete;
			return parseBooleanConfig(raw);
		},
	},
	watch: {
		speed() {
			this.syncTraySpeedTooltip();
		},
		traySpeedometer(val) {
			this.syncTraySpeedTooltip(!!val);
		},
		downloading(val, oldVal) {
			if (val !== oldVal) {
				this.noSleepDesired = !!val && this.preventSleepWhileDownloading;
				this.syncNoSleepState();
			}
		},
		preventSleepWhileDownloading(val) {
			const desired = !!val && this.downloading;
			if (desired !== this.noSleepDesired) {
				this.noSleepDesired = desired;
				this.syncNoSleepState();
			}
		},
		shutdownWhenComplete(val) {
			if (!val) {
				this.cancelShutdown();
			}
		},
		progress(val) {
			invoke("on_progress_change", {
				progress: val,
				showProgressBar: this.showProgressBar,
			}).catch(() => {});
		},
		showProgressBar(val) {
			invoke("on_progress_change", {
				progress: this.progress,
				showProgressBar: parseBooleanConfig(val),
			}).catch(() => {});
		},
		interval() {
			if (this.timer) {
				this.startPolling();
			}
		},
		autoRetryEnabled(val) {
			if (val) {
				return;
			}
			this.clearAllAutoRetryTimers();
		},
		autoDetectLowSpeedTasks(val) {
			if (val) {
				return;
			}
			this.lowSpeedStrikeMap = {};
			this.lowSpeedRecoverAtMap = {};
			this.lowSpeedRecoveringMap = {};
		},
	},
	methods: {
		clearAutoRetryTimer(gid: string) {
			const timer = this.autoRetryTimerMap[gid];
			if (timer) {
				clearTimeout(timer);
			}
			delete this.autoRetryTimerMap[gid];
		},
		clearAutoRetryState(gid: string) {
			this.clearAutoRetryTimer(gid);
			delete this.autoRetryAttemptMap[gid];
			delete this.autoRetryPlanTicketMap[gid];
		},
		clearAllAutoRetryTimers() {
			for (const gid of Object.keys(this.autoRetryTimerMap)) {
				this.clearAutoRetryTimer(gid);
			}
			this.autoRetryAttemptMap = {};
			this.autoRetryPlanTicketMap = {};
		},
		normalizeAutoRetryAttemptMap(raw: unknown = {}) {
			const result: Record<string, number> = {};
			if (!raw || typeof raw !== "object") {
				return result;
			}

			for (const [gid, value] of Object.entries(raw)) {
				const key = `${gid || ""}`.trim();
				if (!key) {
					continue;
				}

				result[key] = Math.floor(
					normalizePositiveNumber(value, 0, 0, Number.MAX_SAFE_INTEGER),
				);
			}
			return result;
		},
		async scheduleAutoRetry(gid: string) {
			const taskGid = `${gid || ""}`.trim();
			if (!this.autoRetryEnabled || !taskGid) {
				return;
			}

			this.clearAutoRetryTimer(taskGid);
			const ticket = (this.autoRetryPlanTicketMap[taskGid] || 0) + 1;
			this.autoRetryPlanTicketMap[taskGid] = ticket;

			let nextAttempt = 1;
			let delayMs = 1000;

			try {
				const result = await api.planAutoRetry({
					gid: taskGid,
					strategy: this.autoRetryStrategy,
					intervalSeconds: this.autoRetryIntervalSeconds,
					maxDelayMs: AUTO_RETRY_MAX_DELAY_MS,
					attemptMap: this.autoRetryAttemptMap,
				});

				if (this.autoRetryPlanTicketMap[taskGid] !== ticket) {
					return;
				}

				const plannedAttemptMap = this.normalizeAutoRetryAttemptMap(
					result?.attemptMap,
				);
				const plannedAttempt = Number(result?.nextAttempt);
				const plannedDelay = Number(result?.delayMs);
				if (
					!Number.isFinite(plannedAttempt) ||
					plannedAttempt <= 0 ||
					!Number.isFinite(plannedDelay) ||
					plannedDelay <= 0
				) {
					if (this.autoRetryPlanTicketMap[taskGid] === ticket) {
						delete this.autoRetryPlanTicketMap[taskGid];
					}
					logger.warn(
						"[Risuko] planAutoRetry produced invalid plan, skip scheduling",
						result,
					);
					return;
				}

				const safeAttempt = Math.floor(
					normalizePositiveNumber(
						plannedAttemptMap[taskGid],
						plannedAttempt,
						1,
						Number.MAX_SAFE_INTEGER,
					),
				);
				this.autoRetryAttemptMap = {
					...this.autoRetryAttemptMap,
					[taskGid]: safeAttempt,
				};
				nextAttempt = safeAttempt;
				delayMs = Math.min(
					Math.max(Math.floor(plannedDelay), 1000),
					AUTO_RETRY_MAX_DELAY_MS,
				);
			} catch (err) {
				if (this.autoRetryPlanTicketMap[taskGid] !== ticket) {
					return;
				}

				delete this.autoRetryPlanTicketMap[taskGid];
				logger.warn("[Risuko] planAutoRetry failed:", err?.message || err);
				return;
			}

			if (
				!this.autoRetryEnabled ||
				this.autoRetryPlanTicketMap[taskGid] !== ticket
			) {
				return;
			}

			this.autoRetryTimerMap[taskGid] = setTimeout(() => {
				const taskStore = useTaskStore();
				taskStore
					.resumeTask({ gid: taskGid } as DownloadTask)
					.then(() => {
						logger.info(
							`[Risuko] auto retry resume requested for gid: ${taskGid}, attempt: ${nextAttempt}`,
						);
					})
					.catch((err) => {
						logger.warn(
							`[Risuko] auto retry resume failed for gid: ${taskGid}, attempt: ${nextAttempt}, ${err?.message || err}`,
						);
					})
					.finally(() => {
						delete this.autoRetryTimerMap[taskGid];
						if (this.autoRetryPlanTicketMap[taskGid] === ticket) {
							delete this.autoRetryPlanTicketMap[taskGid];
						}
					});
			}, delayMs);
		},
		syncLowSpeedRecoveringMapWithState() {
			const stateGids = new Set([
				...Object.keys(this.lowSpeedStrikeMap),
				...Object.keys(this.lowSpeedRecoverAtMap),
			]);
			for (const gid of Object.keys(this.lowSpeedRecoveringMap)) {
				if (!stateGids.has(gid)) {
					delete this.lowSpeedRecoveringMap[gid];
				}
			}
		},
		async recoverLowSpeedTask(gid: string) {
			if (!gid || this.lowSpeedRecoveringMap[gid]) {
				return;
			}

			this.lowSpeedRecoveringMap[gid] = true;
			try {
				await api.forcePauseTask({ gid });
				await new Promise((resolve) =>
					setTimeout(resolve, LOW_SPEED_RESTART_WAIT_MS),
				);
				await api.resumeTask({ gid });
				useTaskStore().saveSession();
				logger.info(`[Risuko] auto recovered low speed task: ${gid}`);
			} catch (err) {
				logger.warn(
					`[Risuko] auto recover low speed task failed: ${gid}, ${err?.message || err}`,
				);
			} finally {
				this.lowSpeedRecoveringMap[gid] = false;
			}
		},
		async handleLowSpeedTasks(
			tasks: Pick<
				DownloadTask,
				| "gid"
				| "status"
				| "downloadSpeed"
				| "totalLength"
				| "seeder"
				| "bittorrent"
				| "ed2kLink"
				| "kind"
			>[] = [],
		) {
			if (!this.autoDetectLowSpeedTasks) {
				return;
			}

			const eligibleTasks = tasks.filter((task) => {
				if (!taskBenefitsFromLowSpeedRecovery(task)) {
					return false;
				}
				const total = Number(task?.totalLength || 0);
				return total > 0;
			});
			if (!eligibleTasks.length) {
				this.lowSpeedStrikeMap = {};
				this.lowSpeedRecoverAtMap = {};
				this.syncLowSpeedRecoveringMapWithState();
				return;
			}

			try {
				const result = await api.evaluateLowSpeedTasks({
					tasks: eligibleTasks.map((task) => {
						const isSeedingTask =
							task?.seeder === "true" || String(task?.seeder) === "true";
						return {
							gid: `${task?.gid || ""}`,
							status: isSeedingTask ? "seeding" : `${task?.status || ""}`,
							downloadSpeed: task?.downloadSpeed,
						};
					}) as DownloadTask[],
					thresholdBytes: this.lowSpeedThresholdBytes,
					strikeThreshold: this.lowSpeedStrikeThreshold,
					cooldownMs: LOW_SPEED_RECOVERY_COOLDOWN_MS,
					nowMs: Date.now(),
					strikeMap: this.lowSpeedStrikeMap,
					recoverAtMap: this.lowSpeedRecoverAtMap,
				});

				const strikeMap = result?.strikeMap;
				const recoverAtMap = result?.recoverAtMap;
				this.lowSpeedStrikeMap =
					strikeMap && typeof strikeMap === "object" ? strikeMap : {};
				this.lowSpeedRecoverAtMap =
					recoverAtMap && typeof recoverAtMap === "object" ? recoverAtMap : {};
				this.syncLowSpeedRecoveringMapWithState();

				const recoverGids = Array.isArray(result?.recoverGids)
					? result.recoverGids
					: [];
				await Promise.allSettled(
					recoverGids.map((gid) => this.recoverLowSpeedTask(`${gid || ""}`)),
				);
			} catch (err) {
				logger.warn(
					"[Risuko] evaluateLowSpeedTasks failed:",
					err?.message || err,
				);
			}
		},
		getTraySpeedLabelPayload() {
			return {
				appName: this.$t("menu.app"),
				downloadLabel: this.$t("task.task-download-speed"),
				uploadLabel: this.$t("task.task-upload-speed"),
			};
		},
		syncTraySpeedTooltip(showTraySpeed = this.traySpeedometer) {
			const { appName, downloadLabel, uploadLabel } =
				this.getTraySpeedLabelPayload();
			invoke("on_speed_change", {
				uploadSpeed: this.uploadSpeed,
				downloadSpeed: this.downloadSpeed,
				showTraySpeed: !!showTraySpeed,
				appName,
				downloadLabel,
				uploadLabel,
			}).catch(() => {});
		},
		async setNoSleepState(downloading: boolean) {
			try {
				await invoke("on_download_status_change", { downloading });
				return true;
			} catch {
				return false;
			}
		},
		async syncNoSleepState() {
			if (this.noSleepSyncing) {
				this.noSleepResyncNeeded = true;
				return;
			}

			this.noSleepSyncing = true;
			do {
				this.noSleepResyncNeeded = false;
				const target = this.noSleepDesired;
				if (target === this.noSleepApplied) {
					continue;
				}

				const ok = await this.setNoSleepState(target);
				if (ok) {
					this.noSleepApplied = target;
				} else {
					break;
				}
			} while (this.noSleepResyncNeeded);
			this.noSleepSyncing = false;

			if (this.noSleepResyncNeeded) {
				this.syncNoSleepState();
			}
		},
		async fetchTaskItem({ gid }) {
			return api.fetchTaskItem({ gid }).catch((e) => {
				logger.warn(`fetchTaskItem fail: ${e.message}`);
			});
		},
		onDownloadStart(payload: { gid: string }) {
			const taskStore = useTaskStore();
			taskStore.fetchList();
			useAppStore().resetInterval();
			this.ensurePolling();
			taskStore.saveSession();
			const { gid } = payload;
			this.clearAutoRetryState(gid);
			if (taskStore.currentTaskItem?.gid === gid) {
				taskStore.updateCurrentTaskDetail();
			}
			const { seedingList } = this;
			if (seedingList.includes(gid)) {
				return;
			}

			this.fetchTaskItem({ gid }).then((task) => {
				if (!task) {
					return;
				}
				const { dir } = task;
				usePreferenceStore().recordHistoryDirectory(dir);
				const taskName = getTaskName(task);
				const message = this.$t("task.download-start-message", { taskName });
				this.$msg.info(message);
			});
		},
		onDownloadPause(payload: { gid: string }) {
			const { gid } = payload;
			this.clearAutoRetryState(gid);
			const { seedingList } = this;
			if (seedingList.includes(gid)) {
				return;
			}

			this.fetchTaskItem({ gid }).then((task) => {
				if (!task) {
					return;
				}
				const taskName = getTaskName(task);
				const message = this.$t("task.download-pause-message", { taskName });
				this.$msg.info(message);
			});
		},
		onDownloadStop(payload: { gid: string }) {
			const { gid } = payload;
			this.clearAutoRetryState(gid);
			this.fetchTaskItem({ gid }).then((task) => {
				if (!task) {
					return;
				}
				const taskName = getTaskName(task);
				const message = this.$t("task.download-stop-message", { taskName });
				this.$msg.info(message);
			});
		},
		onDownloadError(payload: { gid: string }) {
			const { gid } = payload;
			this.fetchTaskItem({ gid }).then((task) => {
				if (!task) {
					return;
				}
				const taskName = getTaskName(task);
				const { errorCode, errorMessage } = task;
				const normalizedErrorCode = String(errorCode || "");
				const errorReferenceUrl = getErrorCodeReferenceUrl(errorCode);
				const sourceUrl = findHttpSourceUrl(task);
				logger.error(
					`[Risuko] download error gid: ${gid}, #${errorCode}, ${errorMessage}`,
				);
				this.readCompletionScriptOverrides(gid).then((overrides) => {
					invoke("run_completion_script", {
						path: getTaskFullPath(task) || "",
						hash: task?.infoHash || null,
						status: "error",
						overrides,
					}).catch(() => {});
				});

				if (normalizedErrorCode === "315") {
					const errorString = String(errorMessage || "");
					const hostMatch = errorString.match(/host=([^\s]+)/);
					const host = hostMatch?.[1] || "";
					const url = sourceUrl;

					const appStore = useAppStore();
					if (host && appStore.cloudflareSkipHosts.includes(host)) {
						toast.error(this.$t("task.download-error-message", { taskName }), {
							duration: 5000,
							class: "error-reference-toast",
							action: errorReferenceUrl
								? {
										label: this.$t("task.open-error-code-reference"),
										onClick: () => void openExternalUrl(errorReferenceUrl),
									}
								: undefined,
						});
					} else {
						appStore.showCloudflareDialog({ gid, host, url, taskName });
					}
					this.clearAutoRetryState(gid);
					return;
				}

				const isMissingYtDlp =
					normalizedErrorCode === "540" ||
					/yt-dlp\s+is\s+not\s+available/i.test(String(errorMessage || "")) ||
					/yt-dlp[^:]*:\s*command\s+not\s+found/i.test(
						String(errorMessage || ""),
					);
				const usenetRepairFailure = task.usenetRepairFailure;
				const isTerminalUsenetError = [
					"550",
					"551",
					"552",
					"553",
					"554",
				].includes(normalizedErrorCode);

				if (
					!isMissingYtDlp &&
					!isTerminalUsenetError &&
					normalizedErrorCode !== "541" &&
					normalizedErrorCode !== "542"
				) {
					this.scheduleAutoRetry(gid);
				}

				let message: string;
				if (isMissingYtDlp) {
					message = this.$t("task.media-tool-required", { taskName });
				} else if (normalizedErrorCode === "541") {
					message = this.$t("task.media-auth-required", { taskName });
				} else if (normalizedErrorCode === "542") {
					message = this.$t("task.media-format-unavailable", { taskName });
				} else if (usenetRepairFailure) {
					message = formatUsenetRepairFailure(
						usenetRepairFailure,
						(key, params) => this.$t(key, params),
					);
				} else {
					message = this.$t("task.download-error-message", { taskName });
				}
				const errorCodeSuffix = normalizedErrorCode
					? ` (${normalizedErrorCode})`
					: "";
				toast.error(`${message}${errorCodeSuffix}`, {
					duration: isMissingYtDlp || usenetRepairFailure ? 9000 : 5000,
					class: "error-reference-toast",
					action: errorReferenceUrl
						? {
								label: this.$t("task.open-error-code-reference"),
								onClick: () => void openExternalUrl(errorReferenceUrl),
							}
						: undefined,
				});
			});
		},
		onDownloadComplete(payload: { gid: string }) {
			const taskStore = useTaskStore();
			taskStore.fetchList();
			this.ensurePolling();
			const { gid } = payload;
			this.clearAutoRetryState(gid);
			taskStore.removeFromSeedingList(gid);

			this.fetchTaskItem({ gid }).then((task) => {
				if (!task) {
					return;
				}
				this.handleDownloadComplete(task, false);
			});
		},
		onBtDownloadComplete(payload: { gid: string }) {
			const taskStore = useTaskStore();
			taskStore.fetchList();
			this.ensurePolling();
			const { gid } = payload;
			this.clearAutoRetryState(gid);
			const { seedingList } = this;
			if (seedingList.includes(gid)) {
				return;
			}

			taskStore.addToSeedingList(gid);

			this.fetchTaskItem({ gid }).then((task) => {
				if (!task) {
					return;
				}
				this.handleDownloadComplete(task, true);
			});
		},
		async readCompletionScriptOverrides(gid: string) {
			try {
				const opts = (await api.getOption({ gid })) as
					| Record<string, unknown>
					| undefined;
				if (!opts) {
					return null;
				}
				const command = opts.risukoCompletionScriptCommand;
				const args = opts.risukoCompletionScriptArgs;
				const enabled = opts.risukoCompletionScriptEnabled;
				const timeoutMs = opts.risukoCompletionScriptTimeoutMs;
				if (
					command === undefined &&
					args === undefined &&
					enabled === undefined &&
					timeoutMs === undefined
				) {
					return null;
				}
				const overrides: Record<string, unknown> = {};
				if (typeof command === "string") {
					overrides.command = command;
				}
				if (typeof args === "string") {
					overrides.args = args;
				}
				if (enabled !== undefined) {
					overrides.enabled =
						enabled === true || enabled === "true" || enabled === 1;
				}
				if (timeoutMs !== undefined) {
					const n = Number(timeoutMs);
					if (Number.isFinite(n) && n > 0) {
						overrides.timeoutMs = n;
					}
				}
				return overrides;
			} catch {
				return null;
			}
		},
		async handleDownloadComplete(task, isBT) {
			useTaskStore().saveSession();

			const path = await finalizeCompletedDownloadPath(task);
			this.showTaskCompleteNotify(task, isBT, path);
			invoke("on_task_download_complete", { path }).catch(() => {});
			const overrides = await this.readCompletionScriptOverrides(task.gid);
			invoke("run_completion_script", {
				path,
				hash: task?.infoHash || null,
				status: "complete",
				overrides,
			}).catch(() => {});
		},
		showTaskCompleteNotify(task, isBT, path) {
			const taskName = getTaskName(task);
			const message = isBT
				? this.$t("task.bt-download-complete-message", { taskName })
				: this.$t("task.download-complete-message", { taskName });
			const tips = isBT ? `\n${this.$t("task.bt-download-complete-tips")}` : "";

			this.$msg.success(`${message}${tips}`);

			if (!this.taskNotification) {
				return;
			}

			const notifyMessage = isBT
				? this.$t("task.bt-download-complete-notify")
				: this.$t("task.download-complete-notify");

			if (document.hidden && !is.android()) {
				this.sendOsNotification(notifyMessage, `${taskName}${tips}`, path);
			} else {
				const notify = new Notification(notifyMessage, {
					body: `${taskName}${tips}`,
				});
				notify.onclick = () => {
					showItemInFolder(path, {
						errorMsg: this.$t("task.file-not-exist"),
					});
				};
			}
		},
		async sendOsNotification(title: string, body: string, path?: string) {
			try {
				const {
					isPermissionGranted,
					requestPermission,
					sendNotification,
					onAction,
				} = await import("@tauri-apps/plugin-notification");
				let granted = await isPermissionGranted();
				if (!granted) {
					granted = (await requestPermission()) === "granted";
				}
				if (!granted) {
					return;
				}
				if (!this.notifyActionUnlisten) {
					this.notifyActionUnlisten = onAction((n) => {
						const folder = n?.extra?.folderPath as string | undefined;
						if (folder) {
							showItemInFolder(folder, {
								errorMsg: this.$t("task.file-not-exist"),
							});
						}
					});
					this.notifyActionUnlisten.catch(() => {
						this.notifyActionUnlisten = null;
					});
				}
				sendNotification({
					title,
					body,
					extra: path ? { folderPath: path } : undefined,
				});
			} catch (err) {
				logger.warn(
					"[Risuko] OS notification failed:",
					(err as Error)?.message || err,
				);
			}
		},
		maybeShowClipboardNotice() {
			if (is.android()) {
				return;
			}
			const config = usePreferenceStore().config;
			if (config?.clipboardWatch && !config?.clipboardWatchNoticeSeen) {
				this.$msg.info({
					message: this.$t("preferences.clipboard-watch-notice"),
					duration: 8000,
				});
				usePreferenceStore()
					.save({ clipboardWatchNoticeSeen: true })
					.catch(() => {});
			}
		},
		async bindEngineEvents() {
			const handlers: [string, (payload: { gid: string }) => void][] = [
				["engine:download-start", this.onDownloadStart],
				["engine:download-stop", this.onDownloadStop],
				["engine:download-complete", this.onDownloadComplete],
				["engine:download-error", this.onDownloadError],
				["engine:bt-download-complete", this.onBtDownloadComplete],
			];

			for (const [event, handler] of handlers) {
				const unlisten = await listen(event, (e: { payload: unknown }) =>
					handler(e.payload as { gid: string }),
				);
				if (this.isDestroyed) {
					unlisten();
					break;
				}
				this.eventUnlisteners.push(unlisten);
			}
		},
		unbindEngineEvents() {
			for (const unlisten of this.eventUnlisteners) {
				unlisten();
			}
			this.eventUnlisteners = [];
		},
		startPolling() {
			this.stopPolling();

			const loop = async () => {
				try {
					await this.polling();
				} catch (err) {
					logger.warn("[Risuko] polling tick failed:", err?.message || err);
				}
				if (this.isDestroyed || this.timer === null) {
					return;
				}
				this.timer = setTimeout(loop, this.interval);
			};

			this.timer = setTimeout(loop, this.interval);
		},
		ensurePolling() {
			this.cancelShutdown();
			if (this.isDestroyed) {
				return;
			}
			if (this.timer !== null || this.isPolling) {
				return;
			}
			this.startPolling();
		},
		armShutdownCountdown() {
			if (this.shutdownCountdownTimer !== null) {
				return;
			}
			const COUNTDOWN_SECONDS = 60;
			this.shutdownCountdownSecondsLeft = COUNTDOWN_SECONDS;
			const updateToast = () => {
				const title = this.$t("preferences.shutdown-countdown-title", {
					seconds: this.shutdownCountdownSecondsLeft,
				});
				const toastOptions = {
					duration: Infinity,
					action: {
						label: this.$t("preferences.shutdown-countdown-cancel"),
						onClick: () => this.cancelShutdown(),
					},
				};
				if (this.shutdownToastId === null) {
					this.shutdownToastId = toast.warning(title, toastOptions);
				} else {
					toast.warning(title, { ...toastOptions, id: this.shutdownToastId });
				}
			};
			updateToast();
			this.shutdownCountdownTimer = setInterval(() => {
				this.shutdownCountdownSecondsLeft -= 1;
				if (this.shutdownCountdownSecondsLeft <= 0) {
					this.executeShutdown();
				} else {
					updateToast();
				}
			}, 1000);
		},
		cancelShutdown() {
			if (this.shutdownCountdownTimer === null) {
				return;
			}
			clearInterval(this.shutdownCountdownTimer);
			this.shutdownCountdownTimer = null;
			if (this.shutdownToastId !== null) {
				toast.dismiss(this.shutdownToastId);
				this.shutdownToastId = null;
			}
		},
		async executeShutdown() {
			clearInterval(this.shutdownCountdownTimer);
			this.shutdownCountdownTimer = null;
			if (this.shutdownToastId !== null) {
				toast.dismiss(this.shutdownToastId);
				this.shutdownToastId = null;
			}
			try {
				await usePreferenceStore().save({ shutdownWhenComplete: false });
			} catch (err) {
				logger.warn(
					"[Risuko] failed to save shutdownWhenComplete preference:",
					err?.message || err,
				);
			}
			try {
				await invoke("shutdown_system");
			} catch (err) {
				logger.warn("[Risuko] shutdown_system failed:", err?.message || err);
				toast.error(this.$t("preferences.shutdown-failed"));
			}
		},
		async polling() {
			if (this.isPolling) {
				return;
			}
			this.isPolling = true;

			try {
				const jobs: Array<Promise<unknown>> = [
					useAppStore().fetchGlobalStat(),
					useAppStore().fetchProgress(),
					useTaskStore().sampleActiveSpeeds(),
				];
				let activeTasksForLowSpeedCheck: Pick<
					DownloadTask,
					| "gid"
					| "status"
					| "downloadSpeed"
					| "totalLength"
					| "completedLength"
					| "seeder"
					| "bittorrent"
					| "ed2kLink"
					| "kind"
				>[] = [];

				if (!document.hidden || this.taskDetailVisible) {
					jobs.push(useTaskStore().fetchList());
				}

				if (this.autoDetectLowSpeedTasks) {
					jobs.push(
						api
							.fetchActiveTaskList({
								keys: [
									"gid",
									"status",
									"downloadSpeed",
									"totalLength",
									"completedLength",
									"seeder",
									"bittorrent",
									"ed2kLink",
									"kind",
								],
							})
							.then((tasks) => {
								activeTasksForLowSpeedCheck = Array.isArray(tasks) ? tasks : [];
							})
							.catch((err) => {
								logger.warn(
									"[Risuko] low speed detection fetch active tasks failed:",
									err?.message || err,
								);
							}),
					);
				}

				if (this.taskDetailVisible && this.currentTaskGid) {
					jobs.push(useTaskStore().fetchItem(this.currentTaskGid));
				}

				await Promise.allSettled(jobs);
				if (this.autoDetectLowSpeedTasks) {
					await this.handleLowSpeedTasks(activeTasksForLowSpeedCheck);
				}
			} finally {
				this.isPolling = false;
			}

			const stat = useAppStore().stat;
			const derivedPaused = useTaskStore().taskList.filter(
				(task) => task.status === "paused",
			).length;
			const nonPausedWaiting = (stat.numWaiting || 0) - derivedPaused;
			if (stat.numActive === 0 && nonPausedWaiting === 0) {
				this.stopPolling();
				if (this.shutdownWhenComplete) {
					this.armShutdownCountdown();
				}
			}
		},
		stopPolling() {
			clearTimeout(this.timer);
			this.timer = null;
		},
		autoResumeUnfinishedTasksOnLaunch() {
			if (this.startupAutoResumeHandled) {
				return;
			}
			this.startupAutoResumeHandled = true;
			this.checkMissedSchedules();

			if (!this.resumeAllWhenAppLaunched) {
				return;
			}

			useTaskStore()
				.resumeAllTask()
				.catch((err) => {
					logger.warn(
						"[Risuko] auto resume unfinished tasks failed:",
						err?.message || err,
					);
				});
		},
		async checkMissedSchedules() {
			const missed: DownloadTask[] = [];
			const num = 5000;
			let offset = 0;
			try {
				for (;;) {
					const tasks = await api.fetchScheduledTaskList({
						offset,
						num,
						keys: ["gid", "scheduleMissed", "startAt", "files", "bittorrent"],
					});
					const page = Array.isArray(tasks) ? tasks : [];
					missed.push(...page.filter((t) => t.scheduleMissed));
					if (page.length < num) {
						break;
					}
					offset += page.length;
				}
				if (missed.length > 0) {
					useAppStore().showMissedScheduled(missed);
				}
			} catch (err) {
				logger.warn(
					"[Risuko] checkMissedSchedules failed:",
					err?.message || err,
				);
			}
		},
	},
	created() {
		this.bindEngineEvents().catch((err) => {
			logger.warn("[Risuko] bindEngineEvents failed:", err.message);
		});
	},
	mounted() {
		this.initTimer = setTimeout(() => {
			const appStore = useAppStore();
			appStore.fetchEngineInfo();
			usePreferenceStore().applyEngineMode();
			this.autoResumeUnfinishedTasksOnLaunch();
			this.syncTraySpeedTooltip();
			this.maybeShowClipboardNotice();

			this.startPolling();
		}, 100);
	},
	beforeUnmount() {
		this.isDestroyed = true;
		useTaskStore().saveSession();
		clearTimeout(this.initTimer);
		this.initTimer = null;

		this.notifyActionUnlisten?.then((u) => u.unregister()).catch(() => {});
		this.notifyActionUnlisten = null;
		this.unbindEngineEvents();
		this.stopPolling();
		this.clearAllAutoRetryTimers();
		this.cancelShutdown();

		this.noSleepDesired = false;
		this.syncNoSleepState();
	},
};
</script>
