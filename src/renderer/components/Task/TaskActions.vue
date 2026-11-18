<template>
  <div class="task-actions">
    <div class="task-total-progress task-total-progress-desktop" v-if="showTotalProgress">
      <span class="task-total-progress-size">
        {{ formatBytes(totalCompletedLength, 1) }} / {{ formatBytes(totalLength, 1) }}
      </span>
      <span class="task-total-progress-sep">·</span>
      <span class="task-total-progress-percent">{{ totalProgressPercent }}%</span>
    </div>
    <ui-tooltip
      class="item"
      effect="dark"
      placement="bottom"
      :content="allVisibleTasksSelected ? $t('task.deselect-all-task') : $t('task.select-all-task')"
    >
      <button
        type="button"
        class="task-action"
        :disabled="selectableTaskCount === 0"
        :title="allVisibleTasksSelected ? $t('task.deselect-all-task') : $t('task.select-all-task')"
        :aria-label="allVisibleTasksSelected ? $t('task.deselect-all-task') : $t('task.select-all-task')"
        @click="onSelectAllClick"
      >
        <ListChecks :size="14" />
      </button>
    </ui-tooltip>
    <ui-tooltip
      class="item"
      effect="dark"
      placement="bottom"
      :content="$t('task.delete-selected-tasks')"
      v-if="currentList !== 'stopped'"
    >
      <button
        type="button"
        class="task-action"
        :class="{ disabled: selectedGidListCount === 0 }"
        :disabled="selectedGidListCount === 0"
        :title="$t('task.delete-selected-tasks')"
        :aria-label="$t('task.delete-selected-tasks')"
        @click="onBatchDeleteClick"
      >
        <Trash2 :size="14" />
      </button>
    </ui-tooltip>
    <ui-tooltip class="item" effect="dark" placement="bottom" :content="$t('task.refresh-list')">
      <button
        type="button"
        class="task-action"
        :title="$t('task.refresh-list')"
        :aria-label="$t('task.refresh-list')"
        @click="onRefreshClick"
      >
        <RefreshCw :size="14" :class="{ 'animate-spin': refreshing }" />
      </button>
    </ui-tooltip>
    <ui-tooltip
      class="item"
      effect="dark"
      placement="bottom"
      :content="hasSelection ? $t('task.resume-selected-tasks') : $t('task.resume-all-task')"
    >
      <button
        type="button"
        class="task-action"
        :title="hasSelection ? $t('task.resume-selected-tasks') : $t('task.resume-all-task')"
        :aria-label="hasSelection ? $t('task.resume-selected-tasks') : $t('task.resume-all-task')"
        @click="onResumeClick"
      >
        <Play :size="14" />
      </button>
    </ui-tooltip>
    <ui-tooltip
      class="item"
      effect="dark"
      placement="bottom"
      :content="hasSelection ? $t('task.pause-selected-tasks') : $t('task.pause-all-task')"
    >
      <button
        type="button"
        class="task-action"
        :title="hasSelection ? $t('task.pause-selected-tasks') : $t('task.pause-all-task')"
        :aria-label="hasSelection ? $t('task.pause-selected-tasks') : $t('task.pause-all-task')"
        @click="onPauseClick"
      >
        <Pause :size="14" />
      </button>
    </ui-tooltip>
    <ui-tooltip
      class="item"
      effect="dark"
      placement="bottom"
      :content="$t('task.purge-record')"
      v-if="currentList === 'stopped'"
    >
      <button
        type="button"
        class="task-action"
        :title="$t('task.purge-record')"
        :aria-label="$t('task.purge-record')"
        @click="onPurgeRecordClick"
      >
        <Eraser :size="14" />
      </button>
    </ui-tooltip>
  </div>
</template>

<script lang="ts">
import {
	Eraser,
	ListChecks,
	Pause,
	Play,
	RefreshCw,
	Trash2,
} from "@lucide/vue";
import { bytesToSize } from "@shared/utils";
import { commands } from "@/components/CommandManager/instance";
import { useTaskStore } from "@/store/task";

export default {
	name: "task-actions",
	components: {
		Trash2,
		RefreshCw,
		Play,
		Pause,
		Eraser,
		ListChecks,
	},
	data() {
		return {
			refreshing: false,
			t: null as ReturnType<typeof setTimeout> | null,
		};
	},
	computed: {
		currentList() {
			return useTaskStore().currentList;
		},
		selectedGidListCount() {
			return useTaskStore().selectedGidList.length;
		},
		selectableTaskCount() {
			return useTaskStore().paginatedTaskList.length;
		},
		allVisibleTasksSelected() {
			const taskStore = useTaskStore();
			if (taskStore.paginatedTaskList.length === 0) {
				return false;
			}
			const selectedKeys = new Set(taskStore.selectedGidList);
			return taskStore.paginatedTaskList.every((task) =>
				selectedKeys.has(task._displayKey || task.gid),
			);
		},
		hasSelection() {
			return this.selectedGidListCount > 0;
		},
		showTotalProgress() {
			return (
				this.currentList !== "stopped" &&
				this.currentList !== "completed" &&
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
	beforeUnmount() {
		if (this.t) {
			clearTimeout(this.t);
		}
	},
	methods: {
		refreshSpin() {
			if (this.t) {
				clearTimeout(this.t);
			}

			this.refreshing = true;
			this.t = setTimeout(() => {
				this.refreshing = false;
			}, 500);
		},
		onBatchDeleteClick(event) {
			const deleteWithFiles = !!event.shiftKey;
			commands.emit("batch-delete-task", { deleteWithFiles });
		},
		onSelectAllClick() {
			if (this.selectableTaskCount === 0) {
				return;
			}
			useTaskStore().selectAllTask();
		},
		onRefreshClick() {
			this.refreshSpin();
			useTaskStore().fetchList();
		},
		onResumeClick() {
			if (this.hasSelection) {
				useTaskStore()
					.batchResumeSelectedTasks()
					?.then(() => {
						this.$msg.success(this.$t("task.resume-selected-tasks-success"));
					})
					.catch(() => {
						this.$msg.error(this.$t("task.resume-selected-tasks-fail"));
					});
			} else {
				useTaskStore()
					.resumeAllTask()
					.then(() => {
						this.$msg.success(this.$t("task.resume-all-task-success"));
					})
					.catch(() => {
						this.$msg.error(this.$t("task.resume-all-task-fail"));
					});
			}
		},
		onPauseClick() {
			if (this.hasSelection) {
				useTaskStore()
					.batchPauseSelectedTasks()
					?.then(() => {
						this.$msg.success(this.$t("task.pause-selected-tasks-success"));
					})
					.catch(() => {
						this.$msg.error(this.$t("task.pause-selected-tasks-fail"));
					});
			} else {
				useTaskStore()
					.pauseAllTask()
					.then(() => {
						this.$msg.success(this.$t("task.pause-all-task-success"));
					})
					.catch(() => {
						this.$msg.error(this.$t("task.pause-all-task-fail"));
					});
			}
		},
		onPurgeRecordClick() {
			useTaskStore()
				.purgeTaskRecord()
				.then(() => {
					this.$msg.success(this.$t("task.purge-record-success"));
				})
				.catch(() => {
					this.$msg.error(this.$t("task.purge-record-fail"));
				});
		},
		formatBytes(value, precision = 1) {
			return bytesToSize(value, precision);
		},
	},
};
</script>
