import { ADD_TASK_TYPE } from "@shared/constants";
import logger from "@shared/utils/logger";
import { invoke } from "@tauri-apps/api/core";
import { toast } from "vue-sonner";
import { commands } from "@/components/CommandManager/instance";
import { getLocaleManager } from "@/components/Locale";
import { confirm } from "@/components/ui/confirm-dialog";
import router from "@/router";
import { useAppStore, usePreferenceStore, useTaskStore } from "@/store";
import { batchItemForFilePath } from "@/store/batchQueue";
import {
	buildTorrentPayload,
	buildUriPayload,
	initTaskForm,
} from "@/utils/task";
import { decodeThunderLink } from "@/utils/thunder";
import { checkForUpdates } from "@/utils/updater";

const i18n = getLocaleManager().getI18n();
const getAppStore = () => useAppStore();
const getPreferenceStore = () => usePreferenceStore();
const getTaskStore = () => useTaskStore();

const updateSystemTheme = (payload: { theme?: string } = {}) => {
	const { theme } = payload;
	getAppStore().updateSystemTheme(theme);
};

const updateTheme = (payload: { theme?: string } = {}) => {
	const { theme } = payload;
	getPreferenceStore().updateAppTheme(theme);
};

const updateLocale = (payload: { locale?: string } = {}) => {
	const { locale } = payload;
	getPreferenceStore().updateAppLocale(locale);
};

const updateTrayFocused = (payload: { focused?: boolean } = {}) => {
	const { focused } = payload;
	getAppStore().updateTrayFocused(focused);
};

const showAboutPanel = () => {
	getAppStore().showAboutPanel();
};

const addTask = async (
	payload: {
		type?: string;
		uri?: string;
		silent?: boolean;
		[key: string]: unknown;
	} = {},
) => {
	const { type = ADD_TASK_TYPE.URI, uri, silent, ...rest } = payload;

	const options = {
		...rest,
	};

	if (type === ADD_TASK_TYPE.URI && uri) {
		getAppStore().updateAddTaskUrl(await decodeThunderLink(uri));
	}
	getAppStore().updateAddTaskOptions(options);

	if (silent) {
		await addTaskSilent(type);
		return;
	}

	getAppStore().showAddTaskDialog(type);
};

const addTaskSilent = async (type: string) => {
	try {
		await addTaskByType(type);
	} catch (err) {
		const message = err instanceof Error ? err.message : String(err);
		toast.error(i18n.t(message));
	}
};

const addTaskByType = async (type: string) => {
	const form = initTaskForm({
		app: getAppStore().$state,
		preference: getPreferenceStore().$state,
	});

	let payload = null;
	if (type === ADD_TASK_TYPE.URI) {
		payload = await buildUriPayload(form);
		return getTaskStore().addUri(payload);
	} else if (type === ADD_TASK_TYPE.TORRENT) {
		payload = buildTorrentPayload(form);
		return getTaskStore().addTorrent(payload);
	} else {
		logger.error("addTask fail", type);
		throw new Error("task.unknown-task-type");
	}
};

const showAddBtTask = () => {
	getAppStore().showAddTaskDialog(ADD_TASK_TYPE.TORRENT);
};

const showAddBtTaskWithFile = (payload: { path?: string } = {}) => {
	const { path = "" } = payload;
	if (!path) {
		return;
	}

	const segs = `${path}`.split(/[/\\]/);
	const name = segs[segs.length - 1] || "task.torrent";
	const item = batchItemForFilePath(path, name);
	if (!item) {
		return;
	}

	getAppStore().showAddTaskDialog(ADD_TASK_TYPE.TORRENT);
	getAppStore().enqueueBatchItems([item]);
};

const navigateTaskList = (payload: { status?: string } = {}) => {
	const { status = "active" } = payload;

	router.push({ path: `/task/${status}` }).catch((err) => {
		logger.log(err);
	});
};

const navigatePreferences = () => {
	router.push({ path: "/preference" }).catch((err) => {
		logger.log(err);
	});
};

const pauseTask = () => {
	getTaskStore().batchPauseSelectedTasks();
};

const resumeTask = () => {
	getTaskStore().batchResumeSelectedTasks();
};

const deleteTask = () => {
	commands.emit("batch-delete-task", {
		deleteWithFiles: false,
	});
};

const pauseAllTask = () => {
	getTaskStore().pauseAllTask();
};

const resumeAllTask = () => {
	getTaskStore().resumeAllTask();
};

const selectAllTask = () => {
	getTaskStore().selectAllTask();
};

const showTaskDetail = (payload: { gid?: string } = {}) => {
	const { gid } = payload;
	navigateTaskList();
	if (gid) {
		getTaskStore().showTaskDetailByGid(gid);
	}
};

const fetchPreference = () => {
	getPreferenceStore().fetchPreference();
};

const checkForUpdatesNow = () => {
	void checkForUpdates("manual");
};

commands.register("application:task-list", navigateTaskList);
commands.register("application:preferences", navigatePreferences);
commands.register("application:about", showAboutPanel);

commands.register("application:new-task", addTask);
commands.register("application:new-bt-task", showAddBtTask);
commands.register("application:new-bt-task-with-file", showAddBtTaskWithFile);
commands.register("application:create-torrent", () =>
	getAppStore().showCreateTorrentDialog(),
);
commands.register("application:pause-task", pauseTask);
commands.register("application:resume-task", resumeTask);
commands.register("application:delete-task", deleteTask);
commands.register("application:pause-all-task", pauseAllTask);
commands.register("application:resume-all-task", resumeAllTask);
commands.register("application:select-all-task", selectAllTask);
commands.register("application:show-task-detail", showTaskDetail);

commands.register("application:update-preference-config", fetchPreference);
commands.register("application:check-for-updates", checkForUpdatesNow);
commands.register("application:update-system-theme", updateSystemTheme);
commands.register("application:update-theme", updateTheme);
commands.register("application:update-locale", updateLocale);
commands.register("application:update-tray-focused", updateTrayFocused);

import("@tauri-apps/api/event").then(({ listen }) => {
	listen("confirm-quit", async () => {
		const numActive = getAppStore().stat?.numActive ?? 0;
		const message =
			numActive > 0
				? i18n.t("app.quit-confirm-active", { count: numActive })
				: i18n.t("app.quit-confirm");

		const { confirmed } = await confirm({
			title: i18n.t("app.quit"),
			message,
			kind: numActive > 0 ? "warning" : "info",
			confirmText: i18n.t("app.yes"),
			cancelText: i18n.t("app.no"),
		});

		if (confirmed) {
			invoke("quit_app").catch(() => {});
		}
	});
});
