<template>
  <div
    class="flyout-task-item"
    :class="{ 'is-active': isActive }"
  >
    <div class="flyout-task-icon" :class="`status-${displayStatus}`">
      <component :is="kindIcon" :size="18" />
    </div>
    <div class="flyout-task-body">
      <div class="flyout-task-head">
        <span class="flyout-task-name" :title="taskName">{{ taskName }}</span>
        <span class="flyout-task-pct">{{ progressPercent }}%</span>
      </div>
      <task-progress
        :completed="Number(task.completedLength)"
        :total="Number(task.totalLength)"
        :status="displayStatus"
      />
      <div class="flyout-task-meta">
        <span v-if="isActive && displayDownloadSpeed > 0" class="flyout-task-speed">
          <ArrowDown :size="10" />
          {{ formatBytes(displayDownloadSpeed) }}/s
        </span>
        <span v-if="isBT && displayUploadSpeed > 0" class="flyout-task-speed">
          <ArrowUp :size="10" />
          {{ formatBytes(displayUploadSpeed) }}/s
        </span>
        <span class="flyout-task-sizes">
          {{ formatBytes(Number(task.completedLength), 1) }}<template v-if="Number(task.totalLength) > 0"> / {{ formatBytes(Number(task.totalLength), 1) }}</template>
        </span>
      </div>
    </div>
    <div class="flyout-task-actions" @click.stop>
      <button
        v-for="action in actions"
        :key="action"
        type="button"
        class="flyout-task-action"
        :title="actionTitle(action)"
        :aria-label="actionTitle(action)"
        @click.stop="onActionClick(action)"
      >
        <Pause v-if="action === 'PAUSE'" :size="14" />
        <Play v-else-if="action === 'RESUME'" :size="14" />
        <Square v-else-if="action === 'STOP'" :size="14" />
        <FolderOpen v-else-if="action === 'OPEN'" :size="14" />
        <Trash2 v-else-if="action === 'DELETE'" :size="14" />
      </button>
    </div>
  </div>
</template>

<script lang="ts">
import {
	ArrowDown,
	ArrowUp,
	FileArchive,
	File as FileIcon,
	FolderOpen,
	Magnet,
	Pause,
	Play,
	Square,
	Trash2,
	Video,
} from "@lucide/vue";
import { TASK_STATUS } from "@shared/constants";
import type { DownloadTask } from "@shared/types/task";
import {
	bytesToSize,
	checkTaskIsBT,
	formatProgressPercent,
	getDisplayDownloadSpeed,
	getDisplayTaskStatus,
	getTaskName,
	isMagnetTask,
} from "@shared/utils";
import logger from "@shared/utils/logger";
import TaskProgress from "@/components/Task/TaskProgress.vue";
import { useTaskStore } from "@/store/task";
import { getTaskFullPath, openItem } from "@/utils/native";

type FlyoutAction = "PAUSE" | "RESUME" | "STOP" | "OPEN" | "DELETE";

const actionsByStatus: Record<string, FlyoutAction[]> = {
	[TASK_STATUS.ACTIVE]: ["PAUSE", "DELETE"],
	[TASK_STATUS.PAUSED]: ["RESUME", "DELETE"],
	[TASK_STATUS.WAITING]: ["RESUME", "DELETE"],
	[TASK_STATUS.ERROR]: ["DELETE"],
	[TASK_STATUS.COMPLETE]: ["OPEN", "DELETE"],
	[TASK_STATUS.REMOVED]: ["DELETE"],
	[TASK_STATUS.SEEDING]: ["STOP", "DELETE"],
};

export default {
	name: "flyout-task-item",
	components: {
		[TaskProgress.name as string]: TaskProgress,
		ArrowDown,
		ArrowUp,
		FolderOpen,
		Pause,
		Play,
		Square,
		Trash2,
	},
	props: {
		task: {
			type: Object as () => DownloadTask,
			required: true,
		},
	},
	computed: {
		taskName(): string {
			return getTaskName(this.task, {
				defaultName: this.$t("task.get-task-name"),
				maxLen: -1,
			});
		},
		isBT(): boolean {
			return checkTaskIsBT(this.task);
		},
		displayStatus(): string {
			return getDisplayTaskStatus(this.task);
		},
		isActive(): boolean {
			return (
				this.displayStatus === TASK_STATUS.ACTIVE ||
				this.displayStatus === TASK_STATUS.SEEDING
			);
		},
		displayDownloadSpeed(): number {
			return getDisplayDownloadSpeed(this.task);
		},
		displayUploadSpeed(): number {
			return Number(this.task.uploadSpeed || 0);
		},
		progressPercent(): string {
			return formatProgressPercent(
				Number(this.task.totalLength),
				Number(this.task.completedLength),
			);
		},
		kindIcon() {
			if (this.isBT || isMagnetTask(this.task)) {
				return isMagnetTask(this.task) ? Magnet : FileArchive;
			}
			const name = `${this.taskName}`.toLowerCase();
			if (/\.(mp4|mkv|webm|avi|mov|flv|m3u8|ts)$/.test(name)) {
				return Video;
			}
			return FileIcon;
		},
		actions(): FlyoutAction[] {
			return actionsByStatus[this.displayStatus] || ["DELETE"];
		},
	},
	methods: {
		formatBytes(value: number, precision?: number): string {
			return bytesToSize(value, precision);
		},
		actionTitle(action: FlyoutAction): string {
			switch (action) {
				case "PAUSE":
					return this.$t("task.pause-task");
				case "RESUME":
					return this.$t("task.resume-task");
				case "STOP":
					return this.$t("task.stop-seeding");
				case "OPEN":
					return this.$t("task.open-task");
				case "DELETE":
					return this.$t("task.delete-task");
			}
		},
		onActionClick(action: FlyoutAction): void {
			const store = useTaskStore();
			switch (action) {
				case "PAUSE":
					store.pauseTask(this.task);
					break;
				case "RESUME":
					store.resumeTask(this.task);
					break;
				case "STOP":
					store.stopSeeding({ gid: this.task.gid });
					break;
				case "DELETE":
					store.removeTask(this.task);
					break;
				case "OPEN":
					this.openTask();
					break;
			}
		},
		async openTask(): Promise<void> {
			try {
				await openItem(getTaskFullPath(this.task));
			} catch (err) {
				logger.warn("[Risuko] flyout open task failed:", err);
			}
		},
	},
};
</script>
