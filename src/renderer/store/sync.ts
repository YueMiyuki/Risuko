import {
	getCategoriesForKey,
	syncCategories,
	syncCategoryIds,
} from "@shared/syncCategories";
import {
	normalizeNetworkProxyConfig,
	normalizeProxyConfig,
} from "@shared/types/config";
import {
	ANDROID_USENET_ARCHIVE_LIMITS,
	DEFAULT_USENET_ARCHIVE_LIMITS,
} from "@shared/types/usenet";
import { changeKeysToKebabCase, getApiErrorMessage } from "@shared/utils";
import logger from "@shared/utils/logger";
import {
	conflictKeys,
	dirtyProfileIds,
	isAuthFailure,
	mergeProfiles,
	mergeSyncConflict,
	nextPushDelayMs,
	PUSH_DELAY_MS,
	planCategorySync,
	remainingDirtyKeys,
	sameSyncData,
	USENET_PROFILES_KEY,
} from "@shared/utils/syncConflict";
import { pickDeclaredKeys } from "@shared/utils/syncFilter";
import {
	keepLocalSyncSecrets,
	RETIRED_SYNC_CATEGORIES,
	remoteNeedsScrub,
	withoutSyncSecrets,
} from "@shared/utils/syncSecrets";
import axios from "axios";
import { defineStore } from "pinia";
import api from "@/api";
import is from "@/shims/platform";
import { useAuthStore } from "./auth";
import { usePreferenceStore } from "./preference";

const getAxiosErrorMessage = (err: unknown): string =>
	getApiErrorMessage(err, "Sync failed");

interface SyncState {
	syncing: boolean;
	lastSyncAt: number | null;
}

let pushTimer: ReturnType<typeof setTimeout> | null = null;
let pushRetryAttempt = 0;
let syncGeneration = 0;
const pendingCategories = new Set<string>();
const STATS_CATEGORY = "stats";
const MAX_PUSH_ATTEMPTS = 3;
const scrubbedRevs = new Map<string, number>();
const purgedRetired = new Set<string>();

interface SyncedMark {
	timestamp: number;
	rev?: number;
	pushedKeys?: readonly string[] | null;
	pulledKeys?: readonly string[] | null;
}

interface PushConflict {
	rev: number;
	data: Record<string, unknown> | null;
}

function pushConflict(err: unknown): PushConflict | null {
	const response = (
		err as { response?: { status?: number; data?: Partial<PushConflict> } }
	).response;
	if (response?.status !== 409 || typeof response.data?.rev !== "number") {
		return null;
	}
	return { rev: response.data.rev, data: response.data.data ?? null };
}

function responseStatus(err: unknown): unknown {
	return (err as { response?: { status?: unknown } } | null)?.response?.status;
}

function assertSyncGeneration(generation: number): void {
	if (generation !== syncGeneration) {
		throw new Error("Sync was reset");
	}
}

const shouldApplyCategoryData = (categoryId: string): boolean =>
	categoryId === STATS_CATEGORY ||
	categoryId === "usenet" ||
	categoryId === "network" ||
	categoryId === "engine" ||
	categoryId === "tracker";

function declaredCategoryData(
	categoryId: string,
	data: Record<string, unknown>,
): Record<string, unknown> {
	if (categoryId === STATS_CATEGORY) {
		return data;
	}
	const category = syncCategories.find((c) => c.id === categoryId);
	return pickDeclaredKeys(category?.keys ?? [], data);
}

function mergeUsenetCategory(
	localData: Record<string, unknown>,
	remoteData: Record<string, unknown>,
	dirtyIds: ReadonlySet<string> | null,
): Record<string, unknown> {
	const defaults: Record<string, number> = is.android()
		? { ...ANDROID_USENET_ARCHIVE_LIMITS }
		: { ...DEFAULT_USENET_ARCHIVE_LIMITS };
	const limits = remoteData["usenet-archive-limits"];
	let adjusted = false;
	if (limits && typeof limits === "object" && !Array.isArray(limits)) {
		const clamped = { ...(limits as Record<string, unknown>) };
		for (const [key, fallback] of Object.entries(defaults)) {
			const value = Number(clamped[key]);
			if (
				!Number.isFinite(value) ||
				!Number.isSafeInteger(value) ||
				value <= 0
			) {
				clamped[key] = fallback;
				adjusted = true;
			} else if (value > fallback * 4) {
				clamped[key] = fallback * 4;
				adjusted = true;
			} else {
				clamped[key] = value;
			}
		}
		remoteData = { ...remoteData, "usenet-archive-limits": clamped };
	}
	return {
		...remoteData,
		[USENET_PROFILES_KEY]: mergeProfiles(
			localData[USENET_PROFILES_KEY],
			remoteData[USENET_PROFILES_KEY],
			dirtyIds,
		),
		"usenet-limits-adjusted":
			adjusted || remoteData["usenet-limits-adjusted"] === true,
	};
}

function normalizeNetworkCategory(
	data: Record<string, unknown>,
): Record<string, unknown> {
	return normalizeNetworkProxyConfig(data);
}

export const useSyncStore = defineStore("sync", {
	state: (): SyncState => ({
		syncing: false,
		lastSyncAt: null,
	}),
	actions: {
		getSelectedCategories(): string[] {
			const preferenceStore = usePreferenceStore();
			const selected =
				(preferenceStore.config.cloudSyncCategories as string[]) || [];
			return selected.filter((id) => syncCategoryIds.includes(id));
		},

		isAutoSyncEnabled(): boolean {
			const preferenceStore = usePreferenceStore();
			return (
				!!preferenceStore.config.cloudSyncAuto && !!useAuthStore().isLoggedIn
			);
		},

		localCategoryData(categoryId: string): Record<string, unknown> {
			if (categoryId === STATS_CATEGORY) {
				return {};
			}

			const cat = syncCategories.find((c) => c.id === categoryId);
			if (!cat) {
				return {};
			}

			const preferenceStore = usePreferenceStore();
			const config = changeKeysToKebabCase(
				preferenceStore.config || {},
			) as Record<string, unknown>;
			const data: Record<string, unknown> = {};
			for (const key of cat.keys) {
				if (key in config) {
					data[key] =
						key === "proxy" ? normalizeProxyConfig(config[key]) : config[key];
				}
			}
			return data;
		},

		async extractCategoryData(
			categoryId: string,
		): Promise<Record<string, unknown>> {
			if (categoryId === STATS_CATEGORY) {
				return (await api.exportDownloadStats()) as unknown as Record<
					string,
					unknown
				>;
			}
			return withoutSyncSecrets(categoryId, this.localCategoryData(categoryId));
		},

		async applyCategoryData(
			categoryId: string,
			data: Record<string, unknown>,
		): Promise<void> {
			if (categoryId === STATS_CATEGORY) {
				await api.mergeDownloadStats(data);
				return;
			}
			if (
				categoryId === "network" ||
				categoryId === "engine" ||
				categoryId === "tracker"
			) {
				data = keepLocalSyncSecrets(
					categoryId,
					this.localCategoryData(categoryId),
					data,
				);
			}
			if (categoryId === "usenet") {
				const localData = await this.extractCategoryData(categoryId);
				data = mergeUsenetCategory(
					localData,
					data,
					dirtyProfileIds(this.getDirtyKeys(categoryId)),
				);
			}
			if (categoryId === "network") {
				data = normalizeNetworkCategory(data);
			}
			await usePreferenceStore().save(data, { skipSync: true });
		},

		getCategoryTimestamp(categoryId: string): number | undefined {
			const preferenceStore = usePreferenceStore();
			const timestamps = preferenceStore.config.cloudSyncCategoryTimestamps as
				| Record<string, number>
				| undefined;
			return timestamps?.[categoryId];
		},

		getCategoryRev(categoryId: string): number {
			return (
				usePreferenceStore().config.cloudSyncCategoryRevs?.[categoryId] ?? 0
			);
		},

		getDirtyKeys(categoryId: string): readonly string[] | null {
			if (categoryId === STATS_CATEGORY) {
				return null;
			}
			return (
				usePreferenceStore().config.cloudSyncDirtyKeys?.[categoryId] ?? null
			);
		},

		async setCategoryTimestamp(
			categoryId: string,
			timestamp: number,
		): Promise<void> {
			const preferenceStore = usePreferenceStore();
			const timestamps = {
				...preferenceStore.config.cloudSyncCategoryTimestamps,
			};
			timestamps[categoryId] = timestamp;
			await preferenceStore.save(
				{ cloudSyncCategoryTimestamps: timestamps },
				{ skipSync: true },
			);
		},

		async markSynced(marks: Record<string, SyncedMark>): Promise<void> {
			if (Object.keys(marks).length === 0) {
				return;
			}
			const preferenceStore = usePreferenceStore();
			const timestamps = {
				...preferenceStore.config.cloudSyncCategoryTimestamps,
			};
			const revs = { ...preferenceStore.config.cloudSyncCategoryRevs };
			const dirty = { ...preferenceStore.config.cloudSyncDirtyKeys };
			for (const [categoryId, mark] of Object.entries(marks)) {
				timestamps[categoryId] = mark.timestamp;
				if (typeof mark.rev === "number") {
					revs[categoryId] = mark.rev;
				}
				dirty[categoryId] = remainingDirtyKeys(dirty[categoryId] ?? [], mark);
			}
			await preferenceStore.save(
				{
					cloudSyncCategoryTimestamps: timestamps,
					cloudSyncCategoryRevs: revs,
					cloudSyncDirtyKeys: dirty,
				},
				{ skipSync: true },
			);
		},

		async pushCategory(
			categoryId: string,
			baseRev = this.getCategoryRev(categoryId),
		): Promise<void> {
			const authStore = useAuthStore();
			if (!authStore.isLoggedIn || !authStore.token) {
				return;
			}

			const generation = syncGeneration;
			const dirtyKeys = this.getDirtyKeys(categoryId);
			for (let attempt = 0; attempt < MAX_PUSH_ATTEMPTS; attempt += 1) {
				const data = await this.extractCategoryData(categoryId);
				if (Object.keys(data).length === 0 || generation !== syncGeneration) {
					return;
				}
				try {
					const { data: resp } = await axios.put(
						`${authStore.serverUrl}/settings`,
						{ category: categoryId, data, baseRev },
						{ headers: authStore.getAuthHeaders() },
					);
					if (generation !== syncGeneration) {
						return;
					}
					await this.markSynced({
						[categoryId]: {
							timestamp: resp.updatedAt as number,
							rev: resp.rev as number | undefined,
							pushedKeys: dirtyKeys,
						},
					});
					return;
				} catch (err) {
					const conflict = pushConflict(err);
					if (!conflict) {
						throw err;
					}
					if (generation !== syncGeneration) {
						return;
					}
					if (conflict.data) {
						const remoteData = declaredCategoryData(categoryId, conflict.data);
						await this.applyCategoryData(
							categoryId,
							categoryId === STATS_CATEGORY
								? remoteData
								: mergeSyncConflict(
										data,
										remoteData,
										conflictKeys(categoryId, data, dirtyKeys),
									),
						);
					}
					baseRev = conflict.rev;
				}
			}
			throw new Error("Settings kept changing on another device");
		},

		async pullAll(): Promise<void> {
			const authStore = useAuthStore();
			if (!authStore.isLoggedIn || !authStore.token) {
				throw new Error("Not logged in");
			}

			const generation = syncGeneration;
			this.syncing = true;
			try {
				const { data } = await axios.get(`${authStore.serverUrl}/settings`, {
					headers: authStore.getAuthHeaders(),
				});

				const preferenceStore = usePreferenceStore();
				const selectedCategories = this.getSelectedCategories();
				const settings = data.settings || {};
				const timestamps = data.timestamps || {};
				const revs = data.revs || {};
				const merged: Record<string, unknown> = {};
				const pulled: Record<string, SyncedMark> = {};

				for (const categoryId of selectedCategories) {
					assertSyncGeneration(generation);
					const rawRemoteData = settings[categoryId] as
						| Record<string, unknown>
						| undefined;
					const remoteData = rawRemoteData
						? declaredCategoryData(categoryId, rawRemoteData)
						: undefined;
					if (remoteData) {
						if (shouldApplyCategoryData(categoryId)) {
							await this.applyCategoryData(categoryId, remoteData);
						} else {
							Object.assign(merged, remoteData);
						}
						if (timestamps[categoryId]) {
							pulled[categoryId] = {
								timestamp: timestamps[categoryId] as number,
								rev: revs[categoryId] as number | undefined,
								pulledKeys: Object.keys(remoteData),
							};
						}
					}
				}

				assertSyncGeneration(generation);
				if (Object.keys(merged).length > 0) {
					await preferenceStore.save(merged, { skipSync: true });
				}

				assertSyncGeneration(generation);
				await this.markSynced(pulled);

				assertSyncGeneration(generation);
				this.lastSyncAt = Date.now();
				await preferenceStore.save({ cloudSyncLastAt: this.lastSyncAt });
				logger.info("[Risuko] sync pullAll done");
			} catch (err) {
				const message = getAxiosErrorMessage(err);
				logger.warn("[Risuko] sync pullAll failed:", message);
				throw new Error(message);
			} finally {
				this.syncing = false;
			}
		},

		async syncBidirectional(): Promise<void> {
			const authStore = useAuthStore();
			if (!authStore.isLoggedIn || !authStore.token) {
				throw new Error("Not logged in");
			}

			const generation = syncGeneration;
			this.syncing = true;
			try {
				const { data } = await axios.get(`${authStore.serverUrl}/settings`, {
					headers: authStore.getAuthHeaders(),
				});

				const preferenceStore = usePreferenceStore();
				const selectedCategories = this.getSelectedCategories();
				const remoteSettings = data.settings || {};
				const remoteTimestamps = data.timestamps || {};
				const remoteRevs = data.revs || {};

				const toPull: Record<string, unknown> = {};
				const pulled: Record<string, SyncedMark> = {};
				const toScrub: string[] = [];

				for (const categoryId of selectedCategories) {
					const remoteTimestamp = remoteTimestamps[categoryId] as
						| number
						| undefined;
					const remoteRev = remoteRevs[categoryId] as number | undefined;
					const localData = await this.extractCategoryData(categoryId);
					assertSyncGeneration(generation);
					const rawRemoteData = remoteSettings[categoryId] as
						| Record<string, unknown>
						| undefined;
					const remoteData = rawRemoteData
						? declaredCategoryData(categoryId, rawRemoteData)
						: undefined;
					const scrubbedAt = scrubbedRevs.get(categoryId);
					const alreadyScrubbed =
						scrubbedAt !== undefined && scrubbedAt === (remoteRev ?? 0);
					const needsScrub =
						categoryId !== STATS_CATEGORY &&
						!!rawRemoteData &&
						!!remoteData &&
						!alreadyScrubbed &&
						remoteNeedsScrub(categoryId, remoteData, rawRemoteData);
					if (
						categoryId === STATS_CATEGORY &&
						remoteData &&
						remoteTimestamp !== undefined &&
						Object.keys(localData).length > 0
					) {
						await this.applyCategoryData(categoryId, remoteData);
						const merged = await this.extractCategoryData(categoryId);
						if (sameSyncData(merged, remoteData)) {
							pulled[categoryId] = {
								timestamp: remoteTimestamp,
								rev: remoteRev,
							};
						} else {
							assertSyncGeneration(generation);
							await this.pushCategory(categoryId, remoteRev ?? 0);
						}
						continue;
					}
					const dirtyKeys = this.getDirtyKeys(categoryId);
					const step = planCategorySync(
						{
							hasData: Object.keys(localData).length > 0,
							timestamp: this.getCategoryTimestamp(categoryId),
							rev: this.getCategoryRev(categoryId),
						},
						{
							hasData: !!remoteData,
							timestamp: remoteTimestamp,
							rev: remoteRev,
						},
						dirtyKeys,
					);
					if (needsScrub && (step === "skip" || step === "pull")) {
						toScrub.push(categoryId);
					}

					if (step === "pull" && remoteData && remoteTimestamp !== undefined) {
						if (shouldApplyCategoryData(categoryId)) {
							await this.applyCategoryData(categoryId, remoteData);
						} else {
							Object.assign(toPull, remoteData);
						}
						pulled[categoryId] = {
							timestamp: remoteTimestamp,
							rev: remoteRev,
							pulledKeys: Object.keys(remoteData),
						};
					} else if (step === "push" || step === "merge") {
						if (step === "merge" && remoteData) {
							await this.applyCategoryData(
								categoryId,
								categoryId === STATS_CATEGORY
									? remoteData
									: mergeSyncConflict(
											localData,
											remoteData,
											conflictKeys(categoryId, localData, dirtyKeys),
										),
							);
						}
						assertSyncGeneration(generation);
						await this.pushCategory(categoryId, remoteRev ?? 0);
					}
				}

				assertSyncGeneration(generation);
				if (Object.keys(toPull).length > 0) {
					await preferenceStore.save(toPull, { skipSync: true });
				}

				assertSyncGeneration(generation);
				await this.markSynced(pulled);

				for (const categoryId of toScrub) {
					assertSyncGeneration(generation);
					try {
						await this.pushCategory(categoryId);
						scrubbedRevs.set(categoryId, this.getCategoryRev(categoryId));
					} catch (err) {
						logger.warn(
							"[Risuko] sync scrub failed:",
							categoryId,
							(err as Error).message,
						);
					}
				}

				for (const categoryId of RETIRED_SYNC_CATEGORIES) {
					const stale = remoteSettings[categoryId];
					if (
						purgedRetired.has(categoryId) ||
						!stale ||
						typeof stale !== "object" ||
						Object.keys(stale).length === 0
					) {
						continue;
					}
					assertSyncGeneration(generation);
					purgedRetired.add(categoryId);
					try {
						await axios.put(
							`${authStore.serverUrl}/settings`,
							{
								category: categoryId,
								data: {},
								baseRev: (remoteRevs[categoryId] as number | undefined) ?? 0,
							},
							{ headers: authStore.getAuthHeaders() },
						);
					} catch (err) {
						logger.warn(
							"[Risuko] sync purge failed:",
							categoryId,
							(err as Error).message,
						);
					}
				}

				assertSyncGeneration(generation);
				this.lastSyncAt = Date.now();
				await preferenceStore.save({ cloudSyncLastAt: this.lastSyncAt });
				logger.info("[Risuko] sync bidirectional done");
			} catch (err) {
				const message = getAxiosErrorMessage(err);
				logger.warn("[Risuko] sync bidirectional failed:", message);
				throw new Error(message);
			} finally {
				this.syncing = false;
			}
		},

		async syncOnStartup(): Promise<void> {
			if (!this.isAutoSyncEnabled()) {
				return;
			}
			try {
				await this.syncBidirectional();
			} catch (err) {
				logger.warn("[Risuko] syncOnStartup failed:", (err as Error).message);
			}
		},

		syncCategoryOnChange(categoryId: string): void {
			if (!this.isAutoSyncEnabled()) {
				return;
			}
			if (!this.getSelectedCategories().includes(categoryId)) {
				return;
			}

			pendingCategories.add(categoryId);
			this.schedulePendingPush();
		},

		async markCategoryChanged(categoryId: string): Promise<void> {
			if (
				!this.isAutoSyncEnabled() ||
				!this.getSelectedCategories().includes(categoryId)
			) {
				return;
			}
			await this.setCategoryTimestamp(
				categoryId,
				Math.floor(Date.now() / 1000),
			);
			this.syncCategoryOnChange(categoryId);
		},

		syncOnChange(changedConfig: Record<string, unknown>): void {
			if (!this.isAutoSyncEnabled()) {
				return;
			}

			const kebabConfig = changeKeysToKebabCase(changedConfig);

			for (const key of Object.keys(kebabConfig)) {
				const cats = getCategoriesForKey(key);
				for (const cat of cats) {
					this.syncCategoryOnChange(cat);
				}
			}
		},

		schedulePendingPush(delayMs = PUSH_DELAY_MS): void {
			if (pendingCategories.size === 0) {
				return;
			}

			if (pushTimer) {
				clearTimeout(pushTimer);
			}

			pushTimer = setTimeout(async () => {
				pushTimer = null;
				if (!this.isAutoSyncEnabled()) {
					pendingCategories.clear();
					pushRetryAttempt = 0;
					return;
				}
				const generation = syncGeneration;
				const categories = [...pendingCategories];
				pendingCategories.clear();
				const failed: string[] = [];
				const retry: string[] = [];
				let firstError: Error | null = null;
				for (const categoryId of categories) {
					if (generation !== syncGeneration) {
						break;
					}
					try {
						await this.pushCategory(categoryId);
					} catch (err) {
						failed.push(categoryId);
						firstError ??= err as Error;
						if (!isAuthFailure(responseStatus(err))) {
							retry.push(categoryId);
						}
					}
				}
				if (generation !== syncGeneration) {
					return;
				}
				if (failed.length > 0) {
					logger.warn("[Risuko] syncOnChange failed:", firstError?.message);
					if (retry.length === 0) {
						return;
					}
					const queued = [...pendingCategories];
					for (const categoryId of retry) {
						pendingCategories.add(categoryId);
					}
					pushRetryAttempt += 1;
					this.schedulePendingPush(
						nextPushDelayMs(failed, queued, pushRetryAttempt),
					);
					return;
				}
				pushRetryAttempt = 0;
				try {
					this.lastSyncAt = Date.now();
					await api.savePreference({ cloudSyncLastAt: this.lastSyncAt });
				} catch (err) {
					logger.warn("[Risuko] syncOnChange failed:", (err as Error).message);
				}
			}, delayMs);
		},

		resetLocalState(): void {
			syncGeneration += 1;
			pendingCategories.clear();
			if (pushTimer) {
				clearTimeout(pushTimer);
				pushTimer = null;
			}
			pushRetryAttempt = 0;
			this.lastSyncAt = null;
			scrubbedRevs.clear();
			purgedRetired.clear();
		},
	},
});
