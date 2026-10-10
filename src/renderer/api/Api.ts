import { startupOnlyKeys } from "@shared/configKeys";
import { type AppConfig, redactProxySettings } from "@shared/types/config";
import type { HealthReport, RunHealthChecksParams } from "@shared/types/health";
import type {
	LogFileSummary,
	LogLevel,
	LogReadResult,
} from "@shared/types/log";
import type { RssRule } from "@shared/types/rss";
import type {
	DownloadStatsMinuteInput,
	DownloadStatsQuery,
	DownloadStatsStore,
	DownloadStatsView,
} from "@shared/types/stats";
import type {
	AutoRetryPlanResult,
	DownloadTask,
	GlobalStat,
	LowSpeedEvaluationResult,
	MediaInfo,
	PeerInfo,
} from "@shared/types/task";
import type {
	UploadJob,
	UploadRule,
	UploadSinkRecord,
} from "@shared/types/upload";
import {
	changeKeysToCamelCase,
	changeKeysToKebabCase,
	formatOptionsForEngine,
	separateConfig,
} from "@shared/utils";
import logger from "@shared/utils/logger";
import { invoke } from "@tauri-apps/api/core";
import { isEmpty } from "lodash";

const ENGINE_RESTART_USER_KEYS: string[] = [
	"external-engine-enabled",
	"external-engine-host",
	"external-engine-port",
	"external-engine-secret",
	"engine-overrides",
];
const P2P_RUNTIME_OPTION_KEYS = new Set([
	"p2p-proxy",
	"p2p-no-proxy",
	"p2p-udp-proxy",
	"p2p-udp-no-proxy",
]);
const PROXY_RUNTIME_OPTION_KEYS = new Set([
	"all-proxy",
	"no-proxy",
	"p2p-proxy",
	"p2p-no-proxy",
	"p2p-udp-proxy",
	"p2p-udp-no-proxy",
]);
const TASK_LIST_FETCH_SIZE = 5000;

export default class Api {
	config: AppConfig | null;

	constructor() {
		this.config = null;
		this.init();
	}

	async init() {
		this.config = await this.loadConfig();
	}

	async loadConfig(): Promise<AppConfig> {
		return changeKeysToCamelCase(await invoke("get_app_config")) as AppConfig;
	}

	async fetchPreference() {
		this.config = await this.loadConfig();
		return this.config;
	}

	async savePreference(params: Partial<AppConfig> = {}) {
		let kebabParams = changeKeysToKebabCase(params);
		kebabParams = await invoke("prepare_preference_patch", {
			params: kebabParams,
		});

		const { user, system } = separateConfig(kebabParams);
		const hasStartupOnlySystemChanges = Object.keys(system).some((key) =>
			startupOnlyKeys.includes(key),
		);
		const hasEngineRestartUserChanges = ENGINE_RESTART_USER_KEYS.some((key) =>
			Object.hasOwn(user, key),
		);

		await this.savePreferenceToNativeStore(kebabParams);

		if (hasStartupOnlySystemChanges || hasEngineRestartUserChanges) {
			await invoke("restart_engine");
		}

		this.config = await this.loadConfig();
	}

	async savePreferenceToNativeStore(params: Record<string, unknown> = {}) {
		const { user, system, others } = separateConfig(params);
		const config: Record<string, Record<string, unknown>> = {};
		const runtimeSystem = Object.fromEntries(
			Object.entries(system).filter(
				([key]) =>
					!startupOnlyKeys.includes(key) && !P2P_RUNTIME_OPTION_KEYS.has(key),
			),
		);

		if (!isEmpty(user)) {
			const userRecord = user as Record<string, unknown>;
			const diagnosticUser = redactProxySettings(userRecord);
			logger.info("[Risuko] save user config: ", diagnosticUser);
			config.user = user;
		}

		if (!isEmpty(system)) {
			logger.info(
				"[Risuko] save system config: ",
				redactProxySettings(system as Record<string, unknown>),
			);
			config.system = system;
		}

		if (!isEmpty(others)) {
			logger.info("[Risuko] save config found illegal key: ", others);
		}

		const result = await invoke("save_preference", { config });
		if (!isEmpty(runtimeSystem)) {
			try {
				await this.changeGlobalOption(runtimeSystem);
			} catch (err) {
				logger.warn("[Risuko] changeGlobalOption failed:", err?.message || err);
			}
			const activeTaskOptions = Object.fromEntries(
				Object.entries(runtimeSystem).filter(
					([key]) => !PROXY_RUNTIME_OPTION_KEYS.has(key),
				),
			);
			if (!isEmpty(activeTaskOptions)) {
				this.updateActiveTaskOption(activeTaskOptions);
			}
		}

		return result;
	}

	getVersion() {
		return invoke<string>("get_version");
	}

	changeGlobalOption(options: Record<string, unknown>) {
		const args = formatOptionsForEngine(options);
		return invoke("change_global_option_engine", { options: args });
	}

	saveUsenetCredentials(
		profileId: string,
		username?: string,
		password?: string,
	) {
		return invoke("usenet_save_credentials", { profileId, username, password });
	}

	removeUsenetCredentials(profileId: string) {
		return invoke("usenet_remove_credentials", { profileId });
	}

	hasUsenetCredentials(profileId: string) {
		return invoke<boolean>("usenet_has_credentials", { profileId });
	}

	testUsenetProfile(profile: unknown) {
		return invoke<string>("usenet_test_profile", { profile });
	}

	getOption(params: { gid: string } = { gid: "" }) {
		const { gid } = params;
		return invoke("get_option_engine", { gid }).then((data) =>
			changeKeysToCamelCase(data),
		);
	}

	updateActiveTaskOption(options: Record<string, unknown>) {
		return this.fetchTaskList({ type: "active", keys: ["gid"] })
			.then((data) => {
				if (isEmpty(data)) {
					return;
				}
				const gids = (data as DownloadTask[]).map((task) => task.gid);
				return this.batchChangeOption({ gids, options });
			})
			.catch((err) => {
				logger.warn(
					"[Risuko] updateActiveTaskOption failed:",
					err?.message || err,
				);
			});
	}

	changeOption(
		params: { gid: string; options?: Record<string, unknown> } = { gid: "" },
	) {
		const { gid, options = {} } = params;
		const engineOptions = formatOptionsForEngine(options);
		return invoke("change_option", { gid, options: engineOptions });
	}

	updateTask(params: {
		gid: string;
		patch: {
			uris?: string[];
			dir?: string;
			out?: string;
			trackers?: string[];
			options?: Record<string, unknown>;
		};
	}) {
		const { gid, patch } = params;
		const enginePatch: Record<string, unknown> = {};
		if (patch.uris !== undefined) {
			enginePatch.uris = patch.uris;
		}
		if (patch.dir !== undefined) {
			enginePatch.dir = patch.dir;
		}
		if (patch.out !== undefined) {
			enginePatch.out = patch.out;
		}
		if (patch.trackers !== undefined) {
			enginePatch.trackers = patch.trackers;
		}
		if (patch.options !== undefined) {
			enginePatch.options = formatOptionsForEngine(patch.options, true);
		}
		return invoke<{
			restarted: boolean;
			trackersAdded: number;
			progressPreserved: boolean;
		}>("update_task", { gid, patch: enginePatch });
	}

	getGlobalStat() {
		return invoke<GlobalStat>("get_global_stat");
	}

	runHealthChecks(params: RunHealthChecksParams = {}) {
		const { categories, slowProbes } = params;
		return invoke<HealthReport>("run_health_checks", {
			categories: categories ?? null,
			slowProbes: slowProbes ?? false,
		});
	}

	listLogFiles() {
		return invoke<LogFileSummary[]>("list_log_files");
	}

	readLogFile(params: { name: string; levels?: LogLevel[] }) {
		return invoke<LogReadResult>("read_log_file", {
			name: params.name,
			levels: params.levels ?? null,
		});
	}

	fetchTrackerSources(urls: string[]) {
		return invoke<string[]>("fetch_tracker_sources", { urls });
	}

	updateAndroidDownloadNotification(params: {
		progress: number;
		activeCount: number;
		detail: string;
	}) {
		return invoke("update_android_download_notification", params);
	}

	clearAndroidDownloadNotification() {
		return invoke("clear_android_download_notification");
	}

	evaluateLowSpeedTasks(
		params: {
			tasks?: DownloadTask[];
			thresholdBytes?: number;
			strikeThreshold?: number;
			cooldownMs?: number;
			nowMs?: number;
			strikeMap?: Record<string, number>;
			recoverAtMap?: Record<string, number>;
		} = {},
	) {
		const {
			tasks = [],
			thresholdBytes = 0,
			strikeThreshold = 3,
			cooldownMs = 30000,
			nowMs = Date.now(),
			strikeMap = {},
			recoverAtMap = {},
		} = params;
		return invoke<LowSpeedEvaluationResult>("evaluate_low_speed_tasks", {
			tasks,
			thresholdBytes,
			strikeThreshold,
			cooldownMs,
			nowMs,
			strikeMap,
			recoverAtMap,
		});
	}

	planAutoRetry(
		params: {
			gid?: string;
			strategy?: string;
			intervalSeconds?: number;
			maxDelayMs?: number;
			attemptMap?: Record<string, number>;
		} = {},
	) {
		const {
			gid = "",
			strategy = "static",
			intervalSeconds = 5,
			maxDelayMs = 15 * 60 * 1000,
			attemptMap = {},
		} = params;
		return invoke<AutoRetryPlanResult>("plan_auto_retry", {
			gid,
			strategy,
			intervalSeconds,
			maxDelayMs,
			attemptMap,
		});
	}

	reorderTasks(params: { gids: string[]; targetGid: string; after: boolean }) {
		return invoke("reorder_tasks", params);
	}

	setTaskSchedule(params: { gid: string; startAt: number }) {
		return invoke("set_task_schedule", params);
	}

	startTaskNow(params: { gid: string }) {
		return invoke("start_task_now", params);
	}

	addUri(params: {
		uris: string[];
		outs: string[];
		options?: Record<string, unknown>;
	}) {
		const { uris, outs, options } = params;
		const engineOptions = formatOptionsForEngine(options);
		return invoke("add_uri", {
			uris,
			outs,
			options: engineOptions,
		});
	}

	addMedia(params: { url: string; options?: Record<string, unknown> }) {
		const { url, options } = params;
		const engineOptions = formatOptionsForEngine(options);
		return invoke<string>("add_media", {
			url,
			options: engineOptions,
		});
	}

	getMediaInfo(params: { url: string; options?: Record<string, unknown> }) {
		const engineOptions = formatOptionsForEngine(params.options);
		return invoke<MediaInfo>("get_media_info", {
			url: params.url,
			options: engineOptions,
		});
	}

	addTorrent(params: {
		torrentPath: string;
		options?: Record<string, unknown>;
	}) {
		const { torrentPath, options } = params;
		const engineOptions = formatOptionsForEngine(options);

		if (typeof torrentPath !== "string" || !torrentPath.trim()) {
			throw new Error("task.new-task-torrent-required");
		}

		return invoke("add_torrent_by_path", {
			path: torrentPath,
			options: engineOptions,
		});
	}

	addTorrentsByPaths(params: {
		paths: string[];
		options?: Record<string, unknown>;
	}) {
		const { paths, options } = params;
		const engineOptions = formatOptionsForEngine(options);
		return invoke<{ path: string; gid: string | null; error: string | null }[]>(
			"add_torrents_by_paths",
			{
				paths,
				options: engineOptions,
			},
		);
	}

	addMetalinksByPaths(params: {
		paths: string[];
		options?: Record<string, unknown>;
	}) {
		const { paths, options } = params;
		const engineOptions = formatOptionsForEngine(options);
		return invoke<{ path: string; gid: string | null; error: string | null }[]>(
			"add_metalinks_by_paths",
			{
				paths,
				options: engineOptions,
			},
		);
	}

	addNzbsByPaths(params: {
		paths: string[];
		options?: Record<string, unknown>;
	}) {
		const { paths, options } = params;
		const { usenetArchiveLimits, ...scalarOptions } = options || {};
		const engineOptions: Record<string, unknown> =
			formatOptionsForEngine(scalarOptions);
		if (usenetArchiveLimits !== undefined) {
			engineOptions["usenet-archive-limits"] = usenetArchiveLimits;
		}
		return invoke<{ path: string; gid: string | null; error: string | null }[]>(
			"add_nzbs_by_paths",
			{ paths, options: engineOptions },
		);
	}

	fetchWaitingTaskList(
		params: { offset?: number; num?: number; keys?: string[] } = {},
	) {
		const { offset = 0, num = TASK_LIST_FETCH_SIZE, keys } = params;
		return invoke<DownloadTask[]>("tell_waiting", {
			offset,
			num,
			keys,
		});
	}

	fetchStoppedTaskList(
		params: { offset?: number; num?: number; keys?: string[] } = {},
	) {
		const { offset = 0, num = TASK_LIST_FETCH_SIZE, keys } = params;
		return invoke<DownloadTask[]>("tell_stopped", {
			offset,
			num,
			keys,
		});
	}

	fetchScheduledTaskList(
		params: { offset?: number; num?: number; keys?: string[] } = {},
	) {
		const { offset = 0, num = TASK_LIST_FETCH_SIZE, keys } = params;
		return invoke<DownloadTask[]>("tell_scheduled", {
			offset,
			num,
			keys,
		});
	}

	async fetchActiveTaskList(
		params: { offset?: number; num?: number; keys?: string[] } = {},
	) {
		const { keys } = params;
		const active = await invoke<DownloadTask[]>("tell_active", { keys });
		return Array.isArray(active) ? active : [];
	}

	fetchTaskList(
		params: {
			type?: string;
			offset?: number;
			num?: number;
			keys?: string[];
		} = {},
	) {
		const { type } = params;
		switch (type) {
			case "all":
				return this.fetchAllTaskList(params);
			case "waiting":
				return this.fetchWaitingTaskList(params);
			case "stopped":
				return this.fetchStoppedTaskList(params);
			case "scheduled":
				return this.fetchScheduledTaskList(params);
			default:
				return this.fetchActiveTaskList(params);
		}
	}

	async fetchAllTaskList(
		params: { offset?: number; num?: number; keys?: string[] } = {},
	) {
		const { offset = 0, num = TASK_LIST_FETCH_SIZE, keys } = params;
		const [active, waiting, scheduled, stopped] = await Promise.all([
			invoke<DownloadTask[]>("tell_active", { keys }),
			invoke<DownloadTask[]>("tell_waiting", { offset, num, keys }),
			invoke<DownloadTask[]>("tell_scheduled", { offset, num, keys }),
			invoke<DownloadTask[]>("tell_stopped", { offset, num, keys }),
		]);
		const activeArr = Array.isArray(active) ? active : [];
		const waitingArr = Array.isArray(waiting) ? waiting : [];
		const scheduledArr = Array.isArray(scheduled) ? scheduled : [];
		const stoppedArr = Array.isArray(stopped) ? stopped : [];
		return [...activeArr, ...waitingArr, ...scheduledArr, ...stoppedArr];
	}

	fetchTaskItem(params: { gid: string }) {
		const { gid } = params;
		return invoke<DownloadTask>("tell_status", { gid });
	}

	async fetchTaskItemWithPeers(params: {
		gid: string;
	}): Promise<(DownloadTask & { peers: PeerInfo[] }) | null> {
		const { gid } = params;
		const [status, peers] = await Promise.all([
			invoke<DownloadTask>("tell_status", { gid }),
			invoke<PeerInfo[]>("get_peers", { gid }),
		]);
		if (!status) {
			return null;
		}
		return {
			...status,
			peers: Array.isArray(peers) ? peers : [],
		};
	}

	pauseTask(params: { gid: string }) {
		const { gid } = params;
		return invoke("pause_task", { gid });
	}

	pauseAllTask() {
		return invoke("pause_all_tasks");
	}

	forcePauseTask(params: { gid: string }) {
		return this.pauseTask(params);
	}

	forcePauseAllTask() {
		return this.pauseAllTask();
	}

	resumeTask(params: { gid: string }) {
		const { gid } = params;
		return invoke("unpause_task", { gid });
	}

	resumeAllTask() {
		return invoke("unpause_all_tasks");
	}

	removeTask(params: { gid: string }) {
		const { gid } = params;
		return invoke("remove_task", { gid });
	}

	saveSession() {
		return invoke("save_session");
	}

	purgeTaskRecord() {
		return invoke("purge_download_result");
	}

	removeTaskRecord(params: { gid: string }) {
		const { gid } = params;
		return invoke("remove_download_result", { gid });
	}

	recordDownloadStatsMinute(input: DownloadStatsMinuteInput) {
		return invoke("record_download_stats_minute", { input });
	}

	getDownloadStats(query: DownloadStatsQuery) {
		return invoke<DownloadStatsView>("get_download_stats", { query });
	}

	exportDownloadStats() {
		return invoke<DownloadStatsStore>("export_download_stats");
	}

	mergeDownloadStats(data: DownloadStatsStore | Record<string, unknown>) {
		return invoke("merge_download_stats", { data });
	}

	multicall(
		method: string,
		params: { gids?: string[]; options?: Record<string, unknown> } = {},
	) {
		const { gids, options = {} } = params;
		const engineOptions = formatOptionsForEngine(options);
		return invoke("multicall_engine", { method, gids, options: engineOptions });
	}

	batchChangeOption(
		params: { gids?: string[]; options?: Record<string, unknown> } = {},
	) {
		return this.multicall("risuko.changeOption", params);
	}

	batchRemoveTask(params: { gids?: string[] } = {}) {
		return this.multicall("risuko.remove", params);
	}

	batchResumeTask(params: { gids?: string[] } = {}) {
		return this.multicall("risuko.unpause", params);
	}

	batchPauseTask(params: { gids?: string[] } = {}) {
		return this.multicall("risuko.pause", params);
	}

	addRssFeed(url: string) {
		return invoke("add_rss_feed", { url });
	}

	removeRssFeed(feedId: string) {
		return invoke("remove_rss_feed", { feedId });
	}

	refreshRssFeed(feedId: string) {
		return invoke("refresh_rss_feed", { feedId });
	}

	refreshAllRssFeeds() {
		return invoke("refresh_all_rss_feeds");
	}

	getRssFeeds() {
		return invoke("get_rss_feeds");
	}

	getRssItems(feedId: string) {
		return invoke("get_rss_items", { feedId });
	}

	updateRssFeedSettings(feedId: string, interval?: number, isActive?: boolean) {
		return invoke("update_rss_feed_settings", { feedId, interval, isActive });
	}

	addRssRule(rule: Omit<RssRule, "id">) {
		return invoke("add_rss_rule", { rule });
	}

	updateRssRule(rule: RssRule) {
		return invoke("update_rss_rule", { rule });
	}

	dryRunRssRule(rule: RssRule, sampleSize?: number) {
		return invoke("dry_run_rss_rule", { rule, sampleSize });
	}

	removeRssRule(ruleId: string) {
		return invoke("remove_rss_rule", { ruleId });
	}

	getRssRules() {
		return invoke("get_rss_rules");
	}

	deleteRssItems(itemsByFeed: [string, string[]][]) {
		return invoke("delete_rss_items", { itemsByFeed });
	}

	clearRssDownload(feedId: string, itemId: string) {
		return invoke("clear_rss_download", { feedId, itemId });
	}

	readRssDownload(feedId: string, itemId: string) {
		return invoke("read_rss_download", { feedId, itemId }) as Promise<string>;
	}

	downloadRssItemTracked(
		feedId: string,
		itemId: string,
		options?: Record<string, unknown>,
	) {
		return invoke("download_rss_item_tracked", { feedId, itemId, options });
	}

	markRssItemRead(feedId: string, itemId: string) {
		return invoke("mark_rss_item_read", { feedId, itemId });
	}

	markRssItemsRead(entries: [string, string][]) {
		return invoke("mark_rss_items_read", { entries });
	}

	listUploadSinks() {
		return invoke<UploadSinkRecord[]>("list_upload_sinks");
	}

	addUploadSink(
		record: Omit<UploadSinkRecord, "id" | "createdAt"> & {
			id?: string;
			createdAt?: number;
		},
	) {
		const payload: UploadSinkRecord = {
			...record,
			id: record.id ?? "",
			createdAt: record.createdAt ?? 0,
		};
		return invoke<UploadSinkRecord>("add_upload_sink", { record: payload });
	}

	updateUploadSink(record: UploadSinkRecord) {
		return invoke("update_upload_sink", { record });
	}

	removeUploadSink(id: string) {
		return invoke("remove_upload_sink", { id });
	}

	testUploadSink(id: string) {
		return invoke("test_upload_sink", { id });
	}

	getDefaultUploadSink() {
		return invoke<string | null>("get_default_upload_sink");
	}

	setDefaultUploadSink(id: string | null) {
		return invoke("set_default_upload_sink", { id });
	}

	listUploadRules() {
		return invoke<UploadRule[]>("list_upload_rules");
	}

	addUploadRule(rule: Omit<UploadRule, "id"> & { id?: string }) {
		const payload: UploadRule = { ...rule, id: rule.id ?? "" };
		return invoke<UploadRule>("add_upload_rule", { rule: payload });
	}

	updateUploadRule(rule: UploadRule) {
		return invoke("update_upload_rule", { rule });
	}

	removeUploadRule(id: string) {
		return invoke("remove_upload_rule", { id });
	}

	listUploadJobs() {
		return invoke<UploadJob[]>("list_upload_jobs");
	}

	cancelUploadJob(id: string) {
		return invoke("cancel_upload_job", { id });
	}

	clearUploadHistory() {
		return invoke("clear_upload_history");
	}

	vaultStatus() {
		return invoke<{ enabled: boolean; backend: string }>("vault_status");
	}

	vaultPutCredential(id: string, secrets: Record<string, string | undefined>) {
		return invoke("vault_put_credential", { id, secrets });
	}

	vaultGetCredential(id: string) {
		return invoke<Record<string, string> | null>("vault_get_credential", {
			id,
		});
	}

	vaultRemoveCredential(id: string) {
		return invoke("vault_remove_credential", { id });
	}

	listBrowsers() {
		return invoke<BrowserInfo[]>("list_browsers_cmd");
	}

	importBrowserCookies(params: {
		browser: string;
		url: string;
		persist?: boolean;
		userAgent?: string;
	}) {
		return invoke<ImportedCookies>("import_browser_cookies", params);
	}

	listCookieEntries() {
		return invoke<CookieEntryView[]>("list_cookie_entries");
	}

	deleteCookieEntry(host: string) {
		return invoke<boolean>("delete_cookie_entry", { host });
	}

	clearCookieEntries() {
		return invoke<void>("clear_cookie_entries");
	}

	retryWithCookies(params: {
		gid: string;
		cookie?: string;
		userAgent?: string;
	}) {
		const { gid, cookie, userAgent } = params;
		return invoke<void>("retry_with_cookies", {
			gid,
			payload: { cookie, userAgent },
		});
	}

	captureUserAgent() {
		return invoke<{ userAgent: string }>("capture_user_agent");
	}
}

export interface BrowserInfo {
	id: string;
	name: string;
	available: boolean;
	userAgent: string;
}

interface ImportedCookieView {
	name: string;
	value: string;
	domain: string;
	path: string;
	secure: boolean;
	httpOnly: boolean;
	expires: number | null;
}

export interface ImportedCookies {
	host: string;
	userAgent: string;
	cookieHeader: string;
	count: number;
	hasCfClearance: boolean;
	cookieNames: string[];
	cookies: ImportedCookieView[];
}

export interface CookieEntryView {
	host: string;
	browserId: string;
	userAgent: string;
	cookieCount: number;
	importedAt: number;
	lastValidatedAt: number;
}
