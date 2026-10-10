import type { AppConfig } from "@shared/types/config";
import logger from "@shared/utils/logger";
import { invoke } from "@tauri-apps/api/core";
import { reactive } from "vue";
import { toast } from "vue-sonner";
import { getLocaleManager } from "@/components/Locale";
import { confirm } from "@/components/ui/confirm-dialog";
import is from "@/shims/platform";
import { usePreferenceStore } from "@/store/preference";

const CHECK_INTERVAL_MS = 24 * 60 * 60 * 1000;
const UPDATE_ENDPOINT = "https://risuko.app/api/update";

type UpdateStatus =
	| "idle"
	| "checking"
	| "available"
	| "downloading"
	| "ready-to-install"
	| "installing"
	| "up-to-date"
	| "cancelled"
	| "error";

export interface UpdaterState {
	status: UpdateStatus;
	version: string | null;
	progress: number | null;
	lastCheckedAt: number;
	error: string | null;
}

export const updaterState = reactive<UpdaterState>({
	status: "idle",
	version: null,
	progress: null,
	lastCheckedAt: 0,
	error: null,
});

type UpdateMode = "manual" | "automatic";
type UpdateResource = import("@tauri-apps/plugin-updater").Update;

type ActiveCheck = {
	promise: Promise<UpdateResource | null>;
	manualRequested: boolean;
};

type ToastId = string | number;

let activeCheck: ActiveCheck | null = null;
let activeOperation: Promise<void> | null = null;
let automaticCheckTimer: ReturnType<typeof setTimeout> | null = null;

function isTauriRuntime(): boolean {
	return Boolean(
		(globalThis as typeof globalThis & { __TAURI_INTERNALS__?: unknown })
			.__TAURI_INTERNALS__,
	);
}

export function isDesktopUpdaterAvailable(): boolean {
	return (
		isTauriRuntime() &&
		!is.android() &&
		(is.macOS() || is.windows() || is.linux())
	);
}

function translate(key: string, options?: Record<string, unknown>): string {
	return getLocaleManager().getI18n().t(key, options);
}

function errorMessage(error: unknown): string {
	if (error instanceof Error) {
		return error.message;
	}
	return `${error || "Unknown updater error"}`;
}

function isUnsignedUpdaterError(error: unknown): boolean {
	return /(?:signature|public\s*key|pubkey|unsigned|signing\s*key|base64|decode)/i.test(
		errorMessage(error),
	);
}

function showManualError(
	message: string,
	unsigned = false,
	toastId?: ToastId,
): void {
	const options =
		toastId === undefined ? undefined : { id: toastId, duration: 6_000 };
	if (unsigned) {
		toast.error(translate("app.update-unsigned"), options);
		return;
	}
	toast.error(translate("app.update-error", { error: message }), options);
}

function showManualChecking(): void {
	toast.info(translate("app.update-checking"));
}

function showManualCancelled(toastId?: ToastId): void {
	toast.info(
		translate("app.update-cancelled"),
		toastId === undefined ? undefined : { id: toastId, duration: 4_000 },
	);
}

async function resolveUpdateProxy(): Promise<string | null> {
	return invoke<string | null>("resolve_configured_proxy", {
		scope: "update-app",
		url: UPDATE_ENDPOINT,
	});
}

async function isSignedUpdaterAvailable(): Promise<boolean> {
	return invoke<boolean>("is_signed_updater_available");
}

export function shouldRunAutomaticCheck(config: AppConfig): boolean {
	const enabled =
		config.autoCheckUpdate === true || `${config.autoCheckUpdate}` === "true";
	if (!enabled) {
		return false;
	}
	const last = Number(config.lastCheckUpdateTime || 0);
	return !Number.isFinite(last) || Date.now() - last >= CHECK_INTERVAL_MS;
}

export function updateDownloadProgress(
	event: {
		event: string;
		data?: { contentLength?: number; chunkLength?: number };
	},
	bytes: {
		downloaded: number;
		total: number | null;
		lastReportedProgress?: number | null;
	},
	toastId?: ToastId,
): void {
	if (event.event === "Started") {
		bytes.downloaded = 0;
		bytes.total = Number.isFinite(event.data?.contentLength)
			? Number(event.data?.contentLength)
			: null;
		updaterState.progress = bytes.total ? 0 : null;
		bytes.lastReportedProgress = null;
		if (toastId !== undefined) {
			toast.loading(translate("app.update-download"), {
				id: toastId,
				duration: Number.POSITIVE_INFINITY,
			});
		}
		return;
	}
	if (event.event === "Progress") {
		bytes.downloaded += Number(event.data?.chunkLength || 0);
		updaterState.progress = bytes.total
			? Math.min(100, (bytes.downloaded / bytes.total) * 100)
			: null;
		if (
			toastId !== undefined &&
			updaterState.progress !== null &&
			bytes.lastReportedProgress !== Math.round(updaterState.progress)
		) {
			bytes.lastReportedProgress = Math.round(updaterState.progress);
			toast.loading(
				translate("app.update-progress", {
					progress: bytes.lastReportedProgress,
				}),
				{ id: toastId, duration: Number.POSITIVE_INFINITY },
			);
		}
		return;
	}
	if (event.event === "Finished") {
		updaterState.progress = 100;
		if (toastId !== undefined) {
			bytes.lastReportedProgress = 100;
			toast.loading(translate("app.update-progress", { progress: 100 }), {
				id: toastId,
				duration: Number.POSITIVE_INFINITY,
			});
		}
	}
}

async function askToInstall(
	update: UpdateResource,
	showFeedback: () => boolean,
	feedbackToastId?: ToastId,
): Promise<void> {
	const installPrompt = await confirm({
		title: translate("app.check-for-updates-title"),
		message: translate("app.update-install"),
		kind: "info",
		confirmText: translate("app.yes"),
		cancelText: translate("app.no"),
	});
	if (!installPrompt.confirmed) {
		updaterState.status = "cancelled";
		if (showFeedback()) {
			showManualCancelled(feedbackToastId);
		}
		return;
	}

	updaterState.status = "installing";
	if (showFeedback()) {
		toast.info(
			translate("app.update-install"),
			feedbackToastId === undefined
				? undefined
				: { id: feedbackToastId, duration: Number.POSITIVE_INFINITY },
		);
	}
	try {
		await update.install();

		if (showFeedback()) {
			toast.info(
				translate("app.update-relaunch"),
				feedbackToastId === undefined
					? undefined
					: { id: feedbackToastId, duration: Number.POSITIVE_INFINITY },
			);
		}
		const relaunchPrompt = await confirm({
			title: translate("app.check-for-updates-title"),
			message: translate("app.update-relaunch"),
			kind: "info",
			confirmText: translate("app.yes"),
			cancelText: translate("app.no"),
		});
		if (!relaunchPrompt.confirmed) {
			updaterState.status = "idle";
			if (showFeedback()) {
				showManualCancelled(feedbackToastId);
			}
			return;
		}

		await invoke("relaunch_app");
	} catch (error) {
		updaterState.status = "error";
		updaterState.error = errorMessage(error);
		if (showFeedback()) {
			showManualError(
				updaterState.error,
				isUnsignedUpdaterError(error),
				feedbackToastId,
			);
		}
	}
}

async function closeUpdate(update: UpdateResource): Promise<void> {
	try {
		await update.close();
	} catch {}
}

async function offerUpdate(
	update: UpdateResource,
	isManual: () => boolean,
): Promise<void> {
	let prompt: Awaited<ReturnType<typeof confirm>>;
	try {
		prompt = await confirm({
			title: translate("app.check-for-updates-title"),
			message: `${translate("app.update-available")} (${update.version})`,
			kind: "info",
			confirmText: translate("app.yes"),
			cancelText: translate("app.no"),
		});
	} catch (error) {
		await closeUpdate(update);
		throw error;
	}
	if (!prompt.confirmed) {
		updaterState.status = "cancelled";
		if (isManual()) {
			showManualCancelled();
		}
		await closeUpdate(update);
		return;
	}

	if (activeOperation) {
		await closeUpdate(update);
		return activeOperation;
	}
	activeOperation = (async () => {
		const bytes = { downloaded: 0, total: null as number | null };
		const feedbackToastId = toast.loading(translate("app.update-download"), {
			duration: Number.POSITIVE_INFINITY,
		});
		updaterState.status = "downloading";
		updaterState.progress = 0;
		try {
			await update.download((event) =>
				updateDownloadProgress(event, bytes, feedbackToastId),
			);
			updaterState.status = "ready-to-install";
			await askToInstall(update, () => true, feedbackToastId);
			if (updaterState.status === "ready-to-install") {
				updaterState.status = "idle";
			}
		} catch (error) {
			updaterState.status = "error";
			updaterState.error = errorMessage(error);
			showManualError(
				updaterState.error,
				isUnsignedUpdaterError(error),
				feedbackToastId,
			);
		} finally {
			updaterState.progress = null;
			await closeUpdate(update);
		}
	})();
	try {
		await activeOperation;
	} finally {
		activeOperation = null;
	}
}

export async function checkForUpdates(
	mode: UpdateMode = "manual",
): Promise<UpdateResource | null> {
	if (!isDesktopUpdaterAvailable()) {
		return null;
	}

	const preferenceStore = usePreferenceStore();
	if (
		mode === "automatic" &&
		!shouldRunAutomaticCheck(preferenceStore.config)
	) {
		return null;
	}

	if (activeCheck) {
		if (mode === "manual") {
			activeCheck.manualRequested = true;
			showManualChecking();
		}
		return activeCheck.promise;
	}

	const check = {
		manualRequested: mode === "manual",
		promise: Promise.resolve(null) as Promise<UpdateResource | null>,
	};
	activeCheck = check;

	const isManual = () => mode === "manual" || check.manualRequested;
	updaterState.status = "checking";
	updaterState.error = null;
	if (isManual()) {
		showManualChecking();
	}

	const checkPromise = (async () => {
		try {
			const signedUpdaterAvailable = await isSignedUpdaterAvailable();
			if (!signedUpdaterAvailable) {
				updaterState.status = "idle";
				updaterState.error = null;
				if (isManual()) {
					showManualError("", true);
				}
				return null;
			}

			const proxy = await resolveUpdateProxy();
			const { check } = await import("@tauri-apps/plugin-updater");
			const update = await check({
				proxy: proxy || undefined,
				timeout: 15_000,
			});

			const checkedAt = Date.now();
			updaterState.lastCheckedAt = checkedAt;
			try {
				await preferenceStore.save({ lastCheckUpdateTime: checkedAt });
			} catch (error) {
				logger.warn("[Risuko] failed to persist update check time:", error);
			}

			if (!update) {
				updaterState.version = null;
				updaterState.status = "up-to-date";
				if (isManual()) {
					toast.success(translate("app.update-unavailable"));
				}
				return null;
			}

			updaterState.version = update.version;
			updaterState.status = "available";
			await offerUpdate(update, isManual);
			return update;
		} catch (error) {
			updaterState.status = "error";
			updaterState.error = errorMessage(error);
			if (isManual()) {
				showManualError(updaterState.error, isUnsignedUpdaterError(error));
			} else {
				logger.warn("[Risuko] automatic update check failed:", error);
			}
			return null;
		} finally {
			if (activeCheck === check) {
				activeCheck = null;
			}
		}
	})();

	check.promise = checkPromise;
	return checkPromise;
}

export function maybeCheckForUpdates(config: AppConfig): void {
	if (!isDesktopUpdaterAvailable() || !shouldRunAutomaticCheck(config)) {
		return;
	}
	if (automaticCheckTimer !== null) {
		return;
	}
	automaticCheckTimer = setTimeout(() => {
		automaticCheckTimer = null;
		const currentConfig = usePreferenceStore().config;
		if (!shouldRunAutomaticCheck(currentConfig)) {
			return;
		}
		void checkForUpdates("automatic");
	}, 3000);
}
