<template>
  <div class="main panel panel-layout panel-layout--h">
    <div class="content panel panel-layout panel-layout--v relative">
      <motion-enter tag="header" preset="fadeInDown" class="panel-header task-page-header">
        <h4 class="task-title hidden-xs-only">{{ title }}</h4>
        <div class="task-mobile-subnav-switcher">
          <subnav-switcher :title="title" :subnavs="subnavs" />
        </div>
        <task-actions />
      </motion-enter>
      <div class="task-toolbar">
        <div class="task-toolbar-left">
          <div class="task-filter-input">
            <Search :size="12" class="task-filter-icon" />
            <input
              type="text"
              class="task-filter-field"
              :placeholder="$t('task.filter-placeholder')"
              :title="$t('task.filter-placeholder')"
              :aria-label="$t('task.filter-placeholder')"
              :value="filterText"
              @input="onFilterInput"
            />
            <button
              type="button"
              v-if="filterText"
              class="task-filter-clear"
              :title="$t('task.clear-filter')"
              :aria-label="$t('task.clear-filter')"
              @click="onFilterClear"
            >
              <X :size="12" />
            </button>
          </div>
        </div>
        <div class="task-toolbar-right">
          <div class="task-total-progress-row" v-if="showTotalProgress && isMobile">
            <span class="task-total-progress-size">
              {{ formatBytes(totalCompletedLength, 1) }} / {{ formatBytes(totalLength, 1) }}
            </span>
            <span class="task-total-progress-sep">·</span>
            <span class="task-total-progress-percent">{{ totalProgressPercent }}%</span>
          </div>
          <Select
            v-if="availableTags.length > 0 || filterTag"
            :model-value="filterTag || '__all__'"
            @update:model-value="onFilterTagChange"
          >
            <SelectTrigger size="sm" class="task-toolbar-select" style="min-width: 100px">
              <SelectValue :placeholder="$t('task.filter-tag-placeholder')" />
            </SelectTrigger>
            <SelectContent align="end">
              <SelectItem value="__all__">{{ $t('task.filter-tag-all') }}</SelectItem>
              <SelectItem v-for="tag in availableTags" :key="tag" :value="tag">
                {{ tag }}
              </SelectItem>
            </SelectContent>
          </Select>
          <Select :model-value="sortBy" @update:model-value="onSortByChange">
            <SelectTrigger size="sm" class="task-toolbar-select">
              <SelectValue />
            </SelectTrigger>
            <SelectContent align="end">
              <SelectItem v-for="item in sortOptions" :key="item.value" :value="item.value">
                {{ item.label }}
              </SelectItem>
            </SelectContent>
          </Select>
          <button
            type="button"
            v-if="sortBy !== 'default'"
            class="task-action task-sort-order"
            @click="onToggleSortOrder"
            :title="sortOrder === 'asc' ? $t('task.sort-ascending') : $t('task.sort-descending')"
            :aria-label="sortOrder === 'asc' ? $t('task.sort-ascending') : $t('task.sort-descending')"
          >
            <ArrowUpNarrowWide v-if="sortOrder === 'asc'" :size="14" />
            <ArrowDownNarrowWide v-else :size="14" />
          </button>
          <Select :model-value="`${tasksPerPage}`" @update:model-value="onTasksPerPageChange">
            <SelectTrigger size="sm" class="task-toolbar-select">
              <SelectValue />
            </SelectTrigger>
            <SelectContent align="end">
              <SelectItem v-for="size in tasksPerPageOptions" :key="size" :value="`${size}`">
                {{ $t('task.tasks-per-page', { count: size }) }}
              </SelectItem>
            </SelectContent>
          </Select>
        </div>
      </div>
      <div class="task-body">
        <main class="panel-content">
          <task-list />
        </main>
        <task-detail
          :visible="taskDetailVisible"
          :gid="currentTaskGid"
          :task="currentTaskItem"
          :files="currentTaskFiles"
          :peers="currentTaskPeers"
        />
        <schedule-dialog
          v-model:visible="scheduleDialogVisible"
          :task="scheduleTask"
        />
        <edit-task-dialog
          v-model:visible="editDialogVisible"
          :task="editTask"
        />
      </div>
      <loading-overlay :show="taskActionLoading" :text="taskActionLoadingText" />
    </div>
  </div>
</template>

<script lang="ts">
import { ArrowDownNarrowWide, ArrowUpNarrowWide, Search, X } from "@lucide/vue";
import { ADD_TASK_TYPE } from "@shared/constants";
import { redactProxySettings } from "@shared/types/config";
import { bytesToSize, getTaskUri, parseHeader } from "@shared/utils";
import logger from "@shared/utils/logger";
import api from "@/api";
import { commands } from "@/components/CommandManager/instance";
import SubnavSwitcher from "@/components/Subnav/SubnavSwitcher.vue";
import EditTaskDialog from "@/components/Task/EditTaskDialog.vue";
import ScheduleDialog from "@/components/Task/ScheduleDialog.vue";
import TaskActions from "@/components/Task/TaskActions.vue";
import TaskList from "@/components/Task/TaskList.vue";
import TaskDetail from "@/components/TaskDetail/Index.vue";
import { confirm } from "@/components/ui/confirm-dialog";
import LoadingOverlay from "@/components/ui/LoadingOverlay.vue";
import {
	Select,
	SelectContent,
	SelectItem,
	SelectTrigger,
	SelectValue,
} from "@/components/ui/select";
import is from "@/shims/platform";
import { useAppStore } from "@/store/app";
import { usePreferenceStore } from "@/store/preference";
import { useTaskStore } from "@/store/task";
import { copyText } from "@/utils/clipboard";
import { moveTaskFilesToTrash, showItemInFolder } from "@/utils/native";

const DELETE_FILES_CONCURRENCY = 8;

export default {
	name: "task-page",
	components: {
		[TaskActions.name]: TaskActions,
		[TaskList.name]: TaskList,
		[TaskDetail.name]: TaskDetail,
		[ScheduleDialog.name]: ScheduleDialog,
		[EditTaskDialog.name]: EditTaskDialog,
		[SubnavSwitcher.name]: SubnavSwitcher,
		[LoadingOverlay.name]: LoadingOverlay,
		Select,
		SelectContent,
		SelectItem,
		SelectTrigger,
		SelectValue,
		Search,
		X,
		ArrowUpNarrowWide,
		ArrowDownNarrowWide,
	},
	props: {
		status: {
			type: String,
			default: "all",
		},
	},
	computed: {
		selectedGidListCount() {
			return useTaskStore().selectedGidList.length;
		},
		selectedTaskRows() {
			return useTaskStore().selectedTaskRows;
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
		currentTaskFiles() {
			return useTaskStore().currentTaskFiles;
		},
		currentTaskPeers() {
			return useTaskStore().currentTaskPeers;
		},
		noConfirmBeforeDelete() {
			return usePreferenceStore().config.noConfirmBeforeDeleteTask;
		},
		subnavs() {
			return [
				{
					key: "all",
					title: this.$t("task.all"),
					route: "/task/all",
				},
				{
					key: "active",
					title: this.$t("task.active"),
					route: "/task/active",
				},
				{
					key: "waiting",
					title: this.$t("task.waiting"),
					route: "/task/waiting",
				},
				{
					key: "completed",
					title: this.$t("task.completed"),
					route: "/task/completed",
				},
				{
					key: "stopped",
					title: this.$t("task.stopped"),
					route: "/task/stopped",
				},
				{
					key: "scheduled",
					title: this.$t("task.scheduled"),
					route: "/task/scheduled",
				},
			];
		},
		title() {
			const subnav = this.subnavs.find((item) => item.key === this.status);
			return subnav?.title ?? this.$t("task.all");
		},
		filterText() {
			return useTaskStore().filterText;
		},
		filterTag() {
			return useTaskStore().filterTag;
		},
		availableTags() {
			const tags = new Set<string>();
			for (const task of useTaskStore().taskList) {
				if (task.tag) {
					tags.add(task.tag.toLowerCase());
				}
			}
			return Array.from(tags).sort();
		},
		sortBy() {
			return useTaskStore().sortBy;
		},
		sortOrder() {
			return useTaskStore().sortOrder;
		},
		sortOptions() {
			return [
				{ label: this.$t("task.sort-default"), value: "default" },
				{ label: this.$t("task.sort-by-name"), value: "name" },
				{ label: this.$t("task.sort-by-size"), value: "size" },
				{ label: this.$t("task.sort-by-time"), value: "time" },
			];
		},
		tasksPerPage() {
			return useTaskStore().tasksPerPage;
		},
		tasksPerPageOptions() {
			return [10, 20, 30, 40, 50];
		},
		currentList() {
			return useTaskStore().currentList;
		},
		isMobile() {
			return is.android();
		},
		showTotalProgress() {
			return (
				this.currentList !== "stopped" &&
				this.currentList !== "completed" &&
				this.currentList !== "scheduled" &&
				this.totalLength > 0
			);
		},
		totalLength() {
			return useTaskStore().totalLength;
		},
		totalCompletedLength() {
			return useTaskStore().totalCompletedLength;
		},
		totalProgressPercent() {
			return useTaskStore().totalProgressPercent;
		},
	},
	watch: {
		status: "onStatusChange",
	},
	data() {
		return {
			taskActionLoading: false,
			taskActionLoadingText: "",
			scheduleDialogVisible: false,
			scheduleTask: null,
			editDialogVisible: false,
			editTask: null,
		};
	},
	methods: {
		onFilterInput(event) {
			useTaskStore().setFilterText(event.target.value);
		},
		onFilterClear() {
			useTaskStore().setFilterText("");
		},
		onFilterTagChange(value) {
			useTaskStore().setFilterTag(value === "__all__" ? "" : value);
		},
		onSortByChange(value) {
			useTaskStore().setSortBy(value);
		},
		onTasksPerPageChange(value) {
			useTaskStore().setTasksPerPage(value);
		},
		formatBytes(value, precision = 1) {
			return bytesToSize(value, precision);
		},
		onToggleSortOrder() {
			useTaskStore().toggleSortOrder();
		},
		async withTaskActionLoading(loadingText, action: () => Promise<unknown>) {
			if (this.taskActionLoading) {
				return false;
			}

			this.taskActionLoading = true;
			this.taskActionLoadingText = `${loadingText || ""}`;
			const startTime = Date.now();
			const MIN_DISPLAY_MS = 400;
			try {
				await action();
				return true;
			} finally {
				const elapsed = Date.now() - startTime;
				if (elapsed < MIN_DISPLAY_MS) {
					await new Promise((r) => setTimeout(r, MIN_DISPLAY_MS - elapsed));
				}
				this.taskActionLoading = false;
				this.taskActionLoadingText = "";
			}
		},
		onStatusChange() {
			this.changeCurrentList();
		},
		changeCurrentList() {
			useTaskStore().changeCurrentList(this.status);
		},
		directAddTask(uri, options = {}) {
			const uris = [uri];
			const payload = {
				uris,
				outs: [] as string[],
				options: {
					...options,
				},
			};
			useTaskStore()
				.addUri(payload)
				.catch((err) => {
					this.$msg.error(err.message);
				});
		},
		showAddTaskDialog(
			uri: string,
			options: { header?: string; [key: string]: unknown } = {},
		) {
			const { header, ...rest } = options;
			logger.log(
				"[Risuko] show add task dialog options: ",
				redactProxySettings(options),
			);

			const headers = parseHeader(header as string);
			const newOptions = {
				...rest,
				...headers,
			};

			const appStore = useAppStore();
			appStore.updateAddTaskUrl(uri);
			appStore.updateAddTaskOptions(newOptions);
			appStore.showAddTaskDialog(ADD_TASK_TYPE.URI);
		},
		async resolveFullTask(task) {
			if (!task?.gid || task._isFileEntry) {
				return task;
			}
			try {
				const fullTask = await api.fetchTaskItem({ gid: task.gid });
				return fullTask ? { ...task, ...fullTask } : task;
			} catch (err) {
				logger.warn(
					"[Risuko] fetch full task before delete files failed:",
					err,
				);
				return task;
			}
		},
		async resolveFullTasks(taskList) {
			const resolved = new Array(taskList.length);
			let next = 0;
			const worker = async () => {
				while (next < taskList.length) {
					const i = next++;
					resolved[i] = await this.resolveFullTask(taskList[i]);
				}
			};
			await Promise.all(
				Array.from(
					{ length: Math.min(DELETE_FILES_CONCURRENCY, taskList.length) },
					worker,
				),
			);
			return resolved;
		},
		async deleteTaskFiles(task) {
			const result = await moveTaskFilesToTrash(task);
			if (!result) {
				throw new Error("task.remove-task-file-fail");
			}
			return true;
		},
		async removeTask(task, taskName, isRemoveWithFiles = false) {
			const loadingText = this.$t("task.loading-delete-task");
			return this.withTaskActionLoading(loadingText, async () => {
				const fileTarget = isRemoveWithFiles
					? await this.resolveFullTask(task)
					: task;
				await this.removeTaskItem(task, taskName);
				if (isRemoveWithFiles) {
					try {
						await this.deleteTaskFiles(fileTarget);
					} catch (err) {
						logger.warn("[Risuko] file delete failed:", err);
						this.$msg.error(
							this.$t(err?.message || "task.remove-task-file-fail"),
						);
					}
				}
			});
		},
		async removeTaskRecord(task, taskName, isRemoveWithFiles = false) {
			const loadingText = this.$t("task.loading-remove-record");
			return this.withTaskActionLoading(loadingText, async () => {
				const fileTarget = isRemoveWithFiles
					? await this.resolveFullTask(task)
					: task;
				await this.removeTaskRecordItem(task, taskName);
				if (isRemoveWithFiles) {
					try {
						await this.deleteTaskFiles(fileTarget);
					} catch (err) {
						logger.warn("[Risuko] file delete failed:", err);
						this.$msg.error(
							this.$t(err?.message || "task.remove-task-file-fail"),
						);
					}
				}
			});
		},
		async removeTaskItem(task, taskName) {
			try {
				await useTaskStore().removeTask(task);
				this.$msg.success(
					this.$t("task.delete-task-success", {
						taskName,
					}),
				);
			} catch (err) {
				if ((err as { code?: number })?.code === 1) {
					this.$msg.error(
						this.$t("task.delete-task-fail", {
							taskName,
						}),
					);
				}
			}
		},
		async removeTaskRecordItem(task, taskName) {
			try {
				await useTaskStore().removeTaskRecord(task);
				this.$msg.success(
					this.$t("task.remove-record-success", {
						taskName,
					}),
				);
			} catch (err) {
				if ((err as { code?: number })?.code === 1) {
					this.$msg.error(
						this.$t("task.remove-record-fail", {
							taskName,
						}),
					);
				}
			}
		},
		async removeTasks(taskList, isRemoveWithFiles = false) {
			const loadingText = this.$t("task.loading-batch-delete-task");
			return this.withTaskActionLoading(loadingText, async () => {
				const gids = [...new Set(taskList.map((task) => task.gid))];
				const fileEntries = taskList.filter((task) => task._isFileEntry);
				const taskEntries = taskList.filter((task) => !task._isFileEntry);
				const fileTargets = isRemoveWithFiles
					? await this.resolveFullTasks([...fileEntries, ...taskEntries])
					: [];
				await this.removeTaskItems(gids);
				if (isRemoveWithFiles) {
					try {
						await this.batchDeleteTaskFiles(fileTargets);
					} catch (err) {
						logger.warn("[Risuko] batch file delete failed:", err);
						this.$msg.error(
							this.$t(err?.message || "task.remove-task-file-fail"),
						);
					}
				}
			});
		},
		async batchDeleteTaskFiles(taskList) {
			const results: PromiseSettledResult<boolean>[] = new Array(
				taskList.length,
			);
			let next = 0;
			const worker = async () => {
				while (next < taskList.length) {
					const i = next++;
					try {
						results[i] = {
							status: "fulfilled",
							value: await this.deleteTaskFiles(taskList[i]),
						};
					} catch (reason) {
						results[i] = { status: "rejected", reason };
					}
				}
			};
			await Promise.all(
				Array.from(
					{ length: Math.min(DELETE_FILES_CONCURRENCY, taskList.length) },
					worker,
				),
			);
			let failed = false;
			results.forEach((r, i) => {
				if (r.status === "rejected") {
					failed = true;
					const task = taskList[i];
					logger.warn(
						`[Risuko] delete task files failed (gid=${task?.gid}, name=${
							task?.bt_name || task?.bittorrent?.info?.name || ""
						}):`,
						r.reason,
					);
				}
			});
			if (failed) {
				throw new Error("task.remove-task-file-fail");
			}
			return true;
		},
		async removeTaskItems(gids) {
			try {
				await useTaskStore().batchRemoveTask(gids);
				this.$msg.success(this.$t("task.batch-delete-task-success"));
			} catch (err) {
				if ((err as { code?: number })?.code === 1) {
					this.$msg.error(this.$t("task.batch-delete-task-fail"));
				}
			}
		},
		handlePauseTask(payload) {
			const { task, taskName } = payload;
			this.$msg.info(this.$t("task.download-pause-message", { taskName }));
			useTaskStore()
				.pauseTask(task)
				.catch(({ code }) => {
					if (code === 1) {
						this.$msg.error(this.$t("task.pause-task-fail", { taskName }));
					}
				});
		},
		handleResumeTask(payload) {
			const { task, taskName } = payload;
			useTaskStore()
				.resumeTask(task)
				.catch(({ code }) => {
					if (code === 1) {
						this.$msg.error(
							this.$t("task.resume-task-fail", {
								taskName,
							}),
						);
					}
				});
		},
		handleStopTaskSeeding(payload) {
			const { task } = payload;
			useTaskStore().stopSeeding(task);
			this.$msg.info({
				message: this.$t("task.bt-stopping-seeding-tip"),
				duration: 8000,
			});
		},
		handleRestartTask(payload) {
			const { task, taskName, showDialog } = payload;
			const { gid } = task;
			const uri = getTaskUri(task);

			useTaskStore()
				.getTaskOption(gid)
				.then((data: Record<string, string>) => {
					logger.log("[Risuko] get task option:", redactProxySettings(data));
					const { dir, header, split } = data;
					const options = {
						dir,
						header,
						split,
						out: taskName,
					};

					if (showDialog) {
						this.showAddTaskDialog(uri, options);
					} else {
						this.directAddTask(uri, options);
						useTaskStore().removeTaskRecord(task);
					}
				});
		},
		handleRevealInFolder(payload) {
			const { path, fallbackPath } = payload;
			showItemInFolder(path, {
				errorMsg: this.$t("task.file-not-exist"),
				fallbackPath,
			});
		},
		async handleDeleteTask(payload) {
			const { task, taskName, deleteWithFiles } = payload;
			const { noConfirmBeforeDelete } = this;

			if (noConfirmBeforeDelete) {
				this.removeTask(task, taskName, deleteWithFiles);
				return;
			}

			const { confirmed, checkboxChecked } = await confirm({
				message: this.$t("task.delete-task-confirm", { taskName }),
				title: this.$t("task.delete-task"),
				kind: "warning",
				confirmText: this.$t("app.yes"),
				cancelText: this.$t("app.no"),
				checkboxLabel: this.$t("task.delete-task-label"),
				checkboxChecked: deleteWithFiles,
			});
			if (confirmed) {
				this.removeTask(task, taskName, checkboxChecked);
			}
		},
		async handleDeleteTaskRecord(payload) {
			const { task, taskName, deleteWithFiles } = payload;
			const { noConfirmBeforeDelete } = this;

			if (noConfirmBeforeDelete) {
				this.removeTaskRecord(task, taskName, deleteWithFiles);
				return;
			}

			const { confirmed, checkboxChecked } = await confirm({
				message: this.$t("task.remove-record-confirm", { taskName }),
				title: this.$t("task.remove-record"),
				kind: "warning",
				confirmText: this.$t("app.yes"),
				cancelText: this.$t("app.no"),
				checkboxLabel: this.$t("task.remove-record-label"),
				checkboxChecked: !!deleteWithFiles,
			});
			if (confirmed) {
				this.removeTaskRecord(task, taskName, checkboxChecked);
			}
		},
		async handleBatchDeleteTask(payload) {
			const { deleteWithFiles } = payload;
			const { noConfirmBeforeDelete, selectedGidListCount, selectedTaskRows } =
				this;
			if (selectedGidListCount === 0) {
				return;
			}

			const selectedTaskList = selectedTaskRows;

			if (noConfirmBeforeDelete) {
				this.removeTasks(selectedTaskList, deleteWithFiles);
				return;
			}

			const count = `${selectedGidListCount}`;
			const { confirmed, checkboxChecked } = await confirm({
				message: this.$t("task.batch-delete-task-confirm", { count }),
				title: this.$t("task.delete-selected-tasks"),
				kind: "warning",
				confirmText: this.$t("app.yes"),
				cancelText: this.$t("app.no"),
				checkboxLabel: this.$t("task.delete-task-label"),
				checkboxChecked: deleteWithFiles,
			});
			if (confirmed) {
				this.removeTasks(selectedTaskList, checkboxChecked);
			}
		},
		handleCopyTaskLink(payload) {
			const { task } = payload;
			const uri = getTaskUri(task);
			copyText(uri)
				.then(() => {
					this.$msg.success(this.$t("task.copy-link-success"));
				})
				.catch(() => {
					this.$msg.error(this.$t("task.copy-link-failed"));
				});
		},
		handleShowTaskInfo(payload) {
			const { task } = payload;
			useTaskStore().showTaskDetail(task);
		},
		handleScheduleTask(payload) {
			this.scheduleTask = payload.task;
			this.scheduleDialogVisible = true;
		},
		handleEditTask(payload) {
			this.editTask = payload.task;
			this.editDialogVisible = true;
		},
		handleStartTaskNow(payload) {
			const { task, taskName } = payload;
			useTaskStore()
				.startNow(task.gid)
				.catch((err: unknown) => {
					this.$msg.error(
						(err as Error)?.message ||
							this.$t("task.start-now-fail", { taskName }),
					);
				});
		},
	},
	created() {
		this.changeCurrentList();
	},
	mounted() {
		commands.on("pause-task", this.handlePauseTask);
		commands.on("resume-task", this.handleResumeTask);
		commands.on("stop-task-seeding", this.handleStopTaskSeeding);
		commands.on("restart-task", this.handleRestartTask);
		commands.on("reveal-in-folder", this.handleRevealInFolder);
		commands.on("delete-task", this.handleDeleteTask);
		commands.on("delete-task-record", this.handleDeleteTaskRecord);
		commands.on("batch-delete-task", this.handleBatchDeleteTask);
		commands.on("copy-task-link", this.handleCopyTaskLink);
		commands.on("show-task-info", this.handleShowTaskInfo);
		commands.on("schedule-task", this.handleScheduleTask);
		commands.on("edit-task", this.handleEditTask);
		commands.on("start-task-now", this.handleStartTaskNow);
	},
	beforeUnmount() {
		commands.off("pause-task", this.handlePauseTask);
		commands.off("resume-task", this.handleResumeTask);
		commands.off("stop-task-seeding", this.handleStopTaskSeeding);
		commands.off("restart-task", this.handleRestartTask);
		commands.off("reveal-in-folder", this.handleRevealInFolder);
		commands.off("delete-task", this.handleDeleteTask);
		commands.off("delete-task-record", this.handleDeleteTaskRecord);
		commands.off("batch-delete-task", this.handleBatchDeleteTask);
		commands.off("copy-task-link", this.handleCopyTaskLink);
		commands.off("show-task-info", this.handleShowTaskInfo);
		commands.off("schedule-task", this.handleScheduleTask);
		commands.off("edit-task", this.handleEditTask);
		commands.off("start-task-now", this.handleStartTaskNow);
	},
};
</script>
