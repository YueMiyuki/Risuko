<template>
  <div
    :key="task._displayKey || task.gid"
    class="task-item"
    :class="{ 'is-active': shouldPulse, 'is-complete': isComplete, selected: selected, 'has-files': isMultiFileBT, 'task-item--card': isCardView, 'has-drag-handle': showDragHandle }"
    v-on:dblclick="onDbClick"
  >
    <div class="task-item-main">
      <div class="task-item-row">
        <span
          v-if="showDragHandle"
          class="task-drag-handle"
          tabindex="0"
          role="slider"
          aria-orientation="vertical"
          :aria-valuemin="1"
          :aria-valuemax="positionCount"
          :aria-valuenow="position"
          :aria-valuetext="`${position} / ${positionCount}`"
          :aria-label="$t('task.reorder-handle')"
          @pointerdown.stop="onHandleDown"
          @keydown="onHandleKeydown"
          @mousedown.stop
          @touchstart.stop
          @click.stop
          @dblclick.stop
        >
          <GripVertical :size="14" />
        </span>
        <span class="task-status-dot" :class="`status-${taskStatus}`"></span>
        <div class="task-name" :title="taskFullName">
          <span class="task-name-text">{{ taskFullName }}</span>
          <span
            v-if="task.tag"
            class="task-tag"
            :style="tagStyle"
          >
            {{ task.tag }}
          </span>
        </div>
        <task-item-actions v-if="!isAndroidCard" mode="LIST" :task="task" />
      </div>
      <div class="task-card-progress">
        <task-progress
          :completed="Number(task.completedLength)"
          :total="Number(task.totalLength)"
          :status="taskStatus"
        />
        <task-progress-info :task="task" />
      </div>
      <div v-if="isMultiFileBT" class="task-files">
        <button
          type="button"
          class="task-files-toggle"
          :title="$t('task.bt-files-count', { count: displayFiles.length })"
          :aria-label="$t('task.bt-files-count', { count: displayFiles.length })"
          @click.stop="toggleFiles"
        >
          <ChevronDown v-if="filesExpanded" :size="12" />
          <ChevronRight v-else :size="12" />
          <span>{{ $t('task.bt-files-count', { count: displayFiles.length }) }}</span>
        </button>
        <div v-show="filesExpanded" class="task-files-list">
          <div
            v-for="file in displayFiles"
            :key="file.index"
            class="task-file-row"
            :class="{ 'is-complete': file.isComplete }"
          >
            <div class="task-file-head">
              <span class="task-file-name" :title="file.path">{{ file.name }}</span>
              <span class="task-file-pct">{{ file.percent }}%</span>
            </div>
            <task-progress
              :completed="file.completed"
              :total="file.total"
              :status="file.status"
            />
            <div class="task-file-meta">
              <span>{{ formatBytes(file.completed) }} / {{ formatBytes(file.total) }}</span>
            </div>
          </div>
        </div>
      </div>
      <div v-if="isAndroidCard" class="task-card-actions">
        <task-item-actions mode="LIST" :task="task" />
      </div>
    </div>
  </div>
</template>

<script lang="ts">
import { ChevronDown, ChevronRight, GripVertical } from "@lucide/vue";
import { TASK_STATUS } from "@shared/constants";
import {
	bytesToSize,
	checkTaskIsBT,
	formatProgressPercent,
	getDisplayTaskStatus,
	getTaskName,
} from "@shared/utils";
import logger from "@shared/utils/logger";
import is from "@/shims/platform";
import { usePreferenceStore } from "@/store/preference";
import { useTaskStore } from "@/store/task";
import { getTaskFullPath, openItem } from "@/utils/native";
import TaskItemActions from "./TaskItemActions.vue";
import TaskProgress from "./TaskProgress.vue";
import TaskProgressInfo from "./TaskProgressInfo.vue";

export default {
	name: "task-item",
	components: {
		[TaskItemActions.name]: TaskItemActions,
		[TaskProgress.name]: TaskProgress,
		[TaskProgressInfo.name]: TaskProgressInfo,
		ChevronDown,
		ChevronRight,
		GripVertical,
	},
	props: {
		task: {
			type: Object,
		},
		selected: {
			type: Boolean,
			default: false,
		},
		showDragHandle: {
			type: Boolean,
			default: false,
		},
		position: {
			type: Number,
			default: 1,
		},
		positionCount: {
			type: Number,
			default: 1,
		},
	},
	emits: ["handle-down", "keyboard-reorder"],
	data() {
		return {
			filesExpanded: true,
		};
	},
	computed: {
		isCardView() {
			return usePreferenceStore().taskListStyle === "card";
		},
		isAndroid() {
			return is.android();
		},
		isAndroidCard() {
			return this.isAndroid && this.isCardView;
		},
		taskFullName() {
			return getTaskName(this.task, {
				defaultName: this.$t("task.get-task-name"),
				maxLen: -1,
			});
		},
		taskName() {
			return getTaskName(this.task, {
				defaultName: this.$t("task.get-task-name"),
			});
		},
		isMultiFileBT() {
			const files = Array.isArray(this.task.files) ? this.task.files : [];
			return checkTaskIsBT(this.task) && files.length > 1;
		},
		displayFiles() {
			const files = Array.isArray(this.task.files) ? this.task.files : [];
			return files
				.filter((f) => f.selected !== "false")
				.map((f) => {
					const total = Number(f.length || 0);
					const completed = Number(f.completedLength || 0);
					const isComplete = total > 0 && completed >= total;
					const segs = `${f.path || ""}`.split(/[/\\]/);
					const name = segs[segs.length - 1] || f.path || "";
					const percent = formatProgressPercent(total, completed);
					return {
						index: f.index,
						path: f.path,
						name,
						total,
						completed,
						percent,
						isComplete,
						status: isComplete ? TASK_STATUS.COMPLETE : this.taskStatus,
					};
				});
		},
		taskStatus() {
			return getDisplayTaskStatus(this.task);
		},
		isActive() {
			return this.taskStatus === TASK_STATUS.ACTIVE;
		},
		shouldPulse() {
			if (!this.isActive) {
				return false;
			}

			const downloadSpeed = Number(this.task.downloadSpeed || 0);
			const uploadSpeed = Number(this.task.uploadSpeed || 0);
			return downloadSpeed > 0 || uploadSpeed > 0;
		},
		isComplete() {
			return this.taskStatus === TASK_STATUS.COMPLETE;
		},
		tagStyle() {
			const tag = this.task.tag || "";
			let hash = 0;
			for (let i = 0; i < tag.length; i++) {
				hash = tag.charCodeAt(i) + ((hash << 5) - hash);
			}
			const hue = Math.abs(hash % 360);
			return {
				backgroundColor: `hsl(${hue} 60% 50% / 0.14)`,
				color: `hsl(${hue} 45% 45%)`,
			};
		},
	},
	methods: {
		onHandleDown(event) {
			this.$emit("handle-down", { task: this.task, event });
		},
		onHandleKeydown(event: KeyboardEvent) {
			const keyDirections: Record<string, string> = {
				ArrowUp: "up",
				ArrowLeft: "up",
				ArrowDown: "down",
				ArrowRight: "down",
				Home: "first",
				End: "last",
			};
			const direction = keyDirections[event.key];
			if (direction) {
				event.preventDefault();
				this.$emit("keyboard-reorder", { task: this.task, direction });
			}
		},
		onDbClick() {
			const { status } = this.task;
			const { COMPLETE, WAITING, PAUSED } = TASK_STATUS;
			if (status === COMPLETE) {
				this.openTask();
			} else if ([WAITING, PAUSED].includes(status)) {
				this.toggleTask();
			}
		},
		async openTask() {
			const { taskName } = this;
			this.$msg.info(this.$t("task.opening-task-message", { taskName }));
			const fullPath = getTaskFullPath(this.task);
			try {
				const result = await openItem(fullPath);
				if (result) {
					this.$msg.error(this.$t("task.file-not-exist"));
				}
			} catch (err) {
				logger.warn("[Risuko] open task failed:", err);
				this.$msg.error(`${err}`);
			}
		},
		toggleTask() {
			useTaskStore().toggleTask(this.task);
		},
		toggleFiles() {
			this.filesExpanded = !this.filesExpanded;
		},
		formatBytes(value) {
			return bytesToSize(value);
		},
	},
};
</script>
