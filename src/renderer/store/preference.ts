import {
	AUTO_SYNC_TRACKER_INTERVAL,
	MAX_NUM_OF_DIRECTORIES,
	MAX_NUM_OF_SAVED_CREDENTIALS,
} from "@shared/constants";
import { getLanguage } from "@shared/locales";
import { getCategoriesForKey } from "@shared/syncCategories";
import {
	type AppConfig,
	normalizeProxyConfig,
	redactProxySettings,
} from "@shared/types/config";
import {
	CREDENTIAL_SECRET_FIELDS,
	type SavedCredential,
} from "@shared/types/credential";
import {
	changeKeysToCamelCase,
	changeKeysToKebabCase,
	getLangDirection,
	pushItemToFixedLengthArray,
	removeArrayItem,
} from "@shared/utils";
import logger from "@shared/utils/logger";
import {
	engineSpeedLimitOptions,
	touchesEngineSpeedLimits,
} from "@shared/utils/speedLimit";
import {
	changedProfileIds,
	profileDirtyKey,
	trackDirtyKey,
	USENET_PROFILES_KEY,
} from "@shared/utils/syncConflict";
import {
	fetchBtTrackerFromSource,
	reduceTrackerString,
} from "@shared/utils/tracker";
import { isEmpty } from "lodash";
import { defineStore } from "pinia";
import api from "@/api";
import { useSyncStore } from "@/store/sync";
import { useTaskStore } from "@/store/task";

const LEGACY_LOCAL_KEYS = {
	engineMode: "risuko.engine-mode",
	taskListStyle: "risuko.task-list-style",
	sidebarCollapsed: "risuko.sidebar-collapsed",
};

export const usePreferenceStore = defineStore("preference", {
	state: () => ({
		vaultEnabled: false,
		config: {
			locale: "auto",
		} as AppConfig,
	}),
	getters: {
		theme: (state) => state.config.theme,
		locale: (state) => state.config.locale,
		direction: (state) => getLangDirection(getLanguage(state.config.locale)),
		engineMode: (state) =>
			state.config.engineMode === "LIMIT" ? "LIMIT" : "MAX",
		taskListStyle: (state) =>
			state.config.taskListStyle === "card" ? "card" : "compact",
		sidebarCollapsed: (state) => `${state.config.sidebarCollapsed}` === "true",
	},
	actions: {
		async fetchPreference(): Promise<AppConfig> {
			try {
				const config = await api.fetchPreference();
				this.updatePreference(config);
				this.migrateLocalUiPreferences();
				await this.fetchVaultStatus();
				return config;
			} catch (err: unknown) {
				logger.warn("[Risuko] fetchPreference failed:", (err as Error).message);
				return {} as AppConfig;
			}
		},
		async save(config: Partial<AppConfig>, options?: { skipSync?: boolean }) {
			const taskStore = useTaskStore();
			taskStore.saveSession();

			if (isEmpty(config)) {
				return;
			}

			if (!options?.skipSync) {
				const timestamps = {
					...this.config.cloudSyncCategoryTimestamps,
				};
				const dirty = { ...this.config.cloudSyncDirtyKeys };
				const kebabConfig = changeKeysToKebabCase(config);
				let touched = false;
				for (const key of Object.keys(kebabConfig)) {
					for (const cat of getCategoriesForKey(key)) {
						timestamps[cat] = Math.floor(Date.now() / 1000);
						trackDirtyKey(dirty, cat, key);
						if (key === USENET_PROFILES_KEY) {
							for (const id of changedProfileIds(
								this.config.usenetProfiles,
								kebabConfig[key],
							)) {
								trackDirtyKey(dirty, cat, profileDirtyKey(id));
							}
						}
						touched = true;
					}
				}
				if (touched) {
					config = {
						...config,
						cloudSyncCategoryTimestamps: timestamps,
						cloudSyncDirtyKeys: dirty,
					};
				}
			}

			const normalized = changeKeysToCamelCase(changeKeysToKebabCase(config));
			const previousConfig = this.config;
			this.updatePreference(normalized);

			try {
				await api.savePreference(config);
				this.updatePreference(api.config || normalized);
			} catch (error) {
				try {
					this.updatePreference(await api.fetchPreference());
				} catch (refreshError) {
					this.config = previousConfig;
					logger.warn(
						"[Risuko] failed to refresh preferences after save failure:",
						(refreshError as Error).message,
					);
				}
				throw error;
			}

			if (!options?.skipSync) {
				try {
					useSyncStore().syncOnChange(config);
				} catch (err) {
					logger.warn("[Risuko] syncOnChange failed:", (err as Error).message);
				}
			}

			if (touchesEngineSpeedLimits(normalized)) {
				this.applyEngineMode().catch(() => undefined);
			}
		},
		recordHistoryDirectory(directory: string) {
			const { historyDirectories = [], favoriteDirectories = [] } = this.config;
			const all = new Set([...historyDirectories, ...favoriteDirectories]);
			if (all.has(directory)) {
				return;
			}

			this.addHistoryDirectory(directory);
		},
		addHistoryDirectory(directory: string) {
			const { historyDirectories = [] } = this.config;
			const history = pushItemToFixedLengthArray(
				historyDirectories,
				MAX_NUM_OF_DIRECTORIES,
				directory,
			);

			this.save({ historyDirectories: history });
		},
		favoriteDirectory(directory: string) {
			const { historyDirectories = [], favoriteDirectories = [] } = this.config;
			if (
				favoriteDirectories.includes(directory) ||
				favoriteDirectories.length >= MAX_NUM_OF_DIRECTORIES
			) {
				return;
			}

			const favorite = pushItemToFixedLengthArray(
				favoriteDirectories,
				MAX_NUM_OF_DIRECTORIES,
				directory,
			);
			const history = removeArrayItem(historyDirectories, directory);

			this.save({
				historyDirectories: history,
				favoriteDirectories: favorite,
			});
		},
		cancelFavoriteDirectory(directory: string) {
			const { historyDirectories = [], favoriteDirectories = [] } = this.config;
			if (historyDirectories.includes(directory)) {
				return;
			}

			const favorite = removeArrayItem(favoriteDirectories, directory);
			const history = pushItemToFixedLengthArray(
				historyDirectories,
				MAX_NUM_OF_DIRECTORIES,
				directory,
			);

			this.save({
				historyDirectories: history,
				favoriteDirectories: favorite,
			});
		},
		removeDirectory(directory: string) {
			const { historyDirectories = [], favoriteDirectories = [] } = this.config;

			const favorite = removeArrayItem(favoriteDirectories, directory);
			const history = removeArrayItem(historyDirectories, directory);

			this.save({
				historyDirectories: history,
				favoriteDirectories: favorite,
			});
		},
		getSavedCredentials(): SavedCredential[] {
			const { savedCredentials = [] } = this.config;
			return [...savedCredentials].sort(
				(a: SavedCredential, b: SavedCredential) =>
					(b.lastUsedAt || 0) - (a.lastUsedAt || 0),
			);
		},
		async fetchVaultStatus() {
			try {
				const { enabled } = await api.vaultStatus();
				this.vaultEnabled = enabled;
			} catch (err) {
				logger.warn("[Risuko] vaultStatus failed:", (err as Error).message);
				this.vaultEnabled = false;
			}
		},
		_splitCredentialSecrets(credential: SavedCredential): {
			meta: SavedCredential;
			secrets: Record<string, string>;
		} {
			const meta: SavedCredential = { ...credential };
			const secrets: Record<string, string> = {};
			for (const key of CREDENTIAL_SECRET_FIELDS) {
				const val = meta[key];
				if (typeof val === "string" && val.length > 0) {
					secrets[key] = val;
				}
				meta[key] = undefined;
			}
			return { meta, secrets };
		},
		async saveCredential(credential: SavedCredential) {
			const { savedCredentials = [] } = this.config;
			let toStore: SavedCredential = credential;

			if (this.vaultEnabled) {
				const persisted = savedCredentials.find(
					(c: SavedCredential) => c.id === credential.id,
				);
				const wasVaulted = !!persisted?.vaulted;
				const { meta, secrets } = this._splitCredentialSecrets(credential);
				const explicitClear = !!meta.clearVault;
				delete meta.clearVault;
				try {
					if (Object.keys(secrets).length > 0) {
						await api.vaultPutCredential(credential.id, secrets);
						meta.vaulted = true;
					} else if (explicitClear) {
						await api.vaultRemoveCredential(credential.id);
						meta.vaulted = false;
					} else {
						meta.vaulted = wasVaulted;
					}
					toStore = meta;
				} catch (err) {
					logger.warn(
						"[Risuko] vaultPutCredential failed, falling back to inline:",
						(err as Error).message,
					);
					try {
						await api.vaultRemoveCredential(credential.id);
					} catch {}
					toStore = { ...credential, vaulted: false };
					delete (toStore as SavedCredential).clearVault;
				}
			} else if (credential.clearVault) {
				toStore = { ...credential };
				delete (toStore as SavedCredential).clearVault;
			}

			const idx = savedCredentials.findIndex(
				(c: SavedCredential) => c.id === toStore.id,
			);
			let updated: SavedCredential[];
			if (idx >= 0) {
				updated = [...savedCredentials];
				updated[idx] = toStore;
			} else {
				updated = [toStore, ...savedCredentials];
				if (updated.length > MAX_NUM_OF_SAVED_CREDENTIALS) {
					updated = updated.slice(0, MAX_NUM_OF_SAVED_CREDENTIALS);
				}
			}
			await this.save({ savedCredentials: updated });
		},
		async removeCredential(id: string) {
			const { savedCredentials = [] } = this.config;
			const target = savedCredentials.find((c: SavedCredential) => c.id === id);
			if (target?.vaulted) {
				try {
					await api.vaultRemoveCredential(id);
				} catch (err) {
					logger.warn(
						"[Risuko] vaultRemoveCredential failed:",
						(err as Error).message,
					);
				}
			}
			const updated = savedCredentials.filter(
				(c: SavedCredential) => c.id !== id,
			);
			await this.save({ savedCredentials: updated });
		},
		updateAppTheme(theme: string) {
			this.updatePreference({ theme });
		},
		updateAppLocale(locale: string) {
			this.updatePreference({ locale: locale || "auto" });
		},
		updatePreference(config: Partial<AppConfig>) {
			this.config = {
				...this.config,
				...config,
				...(Object.hasOwn(config, "proxy")
					? { proxy: normalizeProxyConfig(config.proxy) }
					: {}),
			};
		},
		fetchBtTracker(trackerSource: string[] = []) {
			const proxy = normalizeProxyConfig(this.config.proxy);
			logger.log(
				"fetchBtTracker",
				trackerSource,
				redactProxySettings({ proxy }),
			);
			return fetchBtTrackerFromSource(trackerSource, proxy, (urls) =>
				api.fetchTrackerSources(urls),
			);
		},
		async autoSyncTracker() {
			const config = this.config;
			if (!config.autoSyncTracker) {
				return;
			}

			const lastSync = config.lastSyncTrackerTime || 0;
			if (Date.now() - lastSync < AUTO_SYNC_TRACKER_INTERVAL) {
				return;
			}

			const trackerSource = config.trackerSource || [];
			if (!trackerSource.length) {
				return;
			}

			try {
				const data = await this.fetchBtTracker(trackerSource);
				const tracker = data.join(",").replace(/^\s*,|,\s*$/g, "");
				if (!tracker) {
					return;
				}
				await this.save({
					btTracker: reduceTrackerString(tracker),
					lastSyncTrackerTime: Date.now(),
				});
				logger.info("[Risuko] auto-sync tracker done");
			} catch (err) {
				logger.warn("[Risuko] auto-sync tracker failed:", err);
			}
		},
		applyEngineMode() {
			return api
				.changeGlobalOption(
					engineSpeedLimitOptions(this.config, this.engineMode === "MAX"),
				)
				.catch((err) => {
					logger.warn(
						"[Risuko] apply engine mode failed:",
						(err as Error)?.message || err,
					);
				});
		},
		setEngineMode(mode: "MAX" | "LIMIT") {
			this.save({ engineMode: mode === "LIMIT" ? "LIMIT" : "MAX" }).catch(
				() => undefined,
			);
			return this.applyEngineMode();
		},
		toggleEngineMode() {
			return this.setEngineMode(this.engineMode === "MAX" ? "LIMIT" : "MAX");
		},
		setTaskListStyle(style: "compact" | "card") {
			this.save({ taskListStyle: style === "card" ? "card" : "compact" }).catch(
				() => undefined,
			);
		},
		setSidebarCollapsed(collapsed: boolean) {
			this.save({ sidebarCollapsed: !!collapsed }).catch(() => undefined);
		},
		migrateLocalUiPreferences() {
			const patch: Partial<AppConfig> = {};
			if (this.config.engineMode === undefined) {
				const legacy = localStorage.getItem(LEGACY_LOCAL_KEYS.engineMode);
				if (legacy === "LIMIT" || legacy === "MAX") {
					patch.engineMode = legacy;
				}
			}
			if (this.config.taskListStyle === undefined) {
				const legacy = localStorage.getItem(LEGACY_LOCAL_KEYS.taskListStyle);
				if (legacy === "card" || legacy === "compact") {
					patch.taskListStyle = legacy;
				}
			}
			if (this.config.sidebarCollapsed === undefined) {
				const legacy = localStorage.getItem(LEGACY_LOCAL_KEYS.sidebarCollapsed);
				if (legacy === "true") {
					patch.sidebarCollapsed = true;
				}
			}
			if (Object.keys(patch).length === 0) {
				return;
			}
			this.save(patch, { skipSync: true })
				.then(() => {
					for (const key of Object.values(LEGACY_LOCAL_KEYS)) {
						localStorage.removeItem(key);
					}
				})
				.catch(() => undefined);
		},
	},
});
