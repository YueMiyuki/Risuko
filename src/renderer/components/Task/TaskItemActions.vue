<template>
  <div
    v-if="isMobileList"
    :key="`${task.gid}-mobile`"
    class="task-item-actions task-item-actions--mobile"
    @click.stop
    v-on:dblclick.stop="() => null"
  >
    <button
      v-if="primaryAction"
      type="button"
      class="task-item-action"
      :title="actionLabel(primaryAction)"
      :aria-label="actionLabel(primaryAction)"
      @click.stop="onActionClick(primaryAction, $event)"
    >
      <component :is="actionIcons[primaryAction]" :size="14" />
    </button>
    <button
      v-if="isCardView"
      type="button"
      class="task-item-action"
      :title="actionLabel('FOLDER')"
      :aria-label="actionLabel('FOLDER')"
      @click.stop="onActionClick('FOLDER', $event)"
    >
      <Folder :size="14" />
    </button>
    <DropdownMenu>
      <DropdownMenuTrigger
        class="task-item-action"
        type="button"
        :title="$t('task.more-actions')"
        :aria-label="$t('task.more-actions')"
      >
        <EllipsisVertical :size="14" />
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end">
        <DropdownMenuItem
          v-for="action in overflowActions"
          :key="action"
          :variant="action === 'DELETE' || action === 'TRASH' ? 'destructive' : 'default'"
          @click="onActionClick(action, $event)"
        >
          <component :is="actionIcons[action]" :size="14" />
          <span>{{ actionLabel(action) }}</span>
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  </div>
  <div
    v-else
    :key="`${task.gid}-desktop`"
    class="task-item-actions"
    @click.stop
    v-on:dblclick.stop="() => null"
  >
    <button
      type="button"
      v-for="(action, index) in taskActions"
      :key="action"
      class="task-item-action"
      :style="{ '--stagger-index': index }"
      :title="actionLabel(action)"
      :aria-label="actionLabel(action)"
      @click.stop="onActionClick(action, $event)"
    >
      <Pause v-if="action === 'PAUSE'" :size="14" />
      <Square v-else-if="action === 'STOP'" :size="14" />
      <Play v-else-if="action === 'RESUME' || action === 'START_NOW'" :size="14" />
      <Clock v-else-if="action === 'SCHEDULE' || action === 'RESCHEDULE'" :size="14" />
      <RotateCcw v-else-if="action === 'RESTART'" :size="14" />
      <Pencil v-else-if="action === 'EDIT'" :size="14" />
      <Trash2 v-else-if="action === 'DELETE'" :size="14" />
      <Trash v-else-if="action === 'TRASH'" :size="14" />
      <Folder v-else-if="action === 'FOLDER'" :size="14" />
      <Link v-else-if="action === 'LINK'" :size="14" />
      <Info v-else-if="action === 'INFO'" :size="14" />
    </button>
  </div>
</template>

<script lang="ts">
import {
	Clock,
	EllipsisVertical,
	Folder,
	Info,
	Link,
	Pause,
	Pencil,
	Play,
	RotateCcw,
	Square,
	Trash,
	Trash2,
} from "@lucide/vue";
import { TASK_STATUS } from "@shared/constants";
import {
	checkTaskIsBT,
	checkTaskIsSeeder,
	getDisplayTaskStatus,
	getTaskName,
} from "@shared/utils";
import { commands } from "@/components/CommandManager/instance";
import {
	DropdownMenu,
	DropdownMenuContent,
	DropdownMenuItem,
	DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import is from "@/shims/platform";
import { usePreferenceStore } from "@/store/preference";
import { useTaskStore } from "@/store/task";
import {
	getTaskFullPath,
	getTaskRevealDir,
	getTaskRevealPath,
} from "@/utils/native";

const taskActionsMap = {
	[TASK_STATUS.ACTIVE]: ["PAUSE", "EDIT", "DELETE"],
	[TASK_STATUS.PAUSED]: ["RESUME", "EDIT", "SCHEDULE", "DELETE"],
	[TASK_STATUS.WAITING]: ["RESUME", "EDIT", "SCHEDULE", "DELETE"],
	[TASK_STATUS.SCHEDULED]: ["START_NOW", "EDIT", "RESCHEDULE", "DELETE"],
	[TASK_STATUS.ERROR]: ["RESTART", "EDIT", "TRASH"],
	[TASK_STATUS.COMPLETE]: ["RESTART", "TRASH"],
	[TASK_STATUS.REMOVED]: ["RESTART", "TRASH"],
	[TASK_STATUS.SEEDING]: ["STOP", "EDIT", "DELETE"],
};

const actionIconsMap = {
	PAUSE: Pause,
	RESUME: Play,
	START_NOW: Play,
	SCHEDULE: Clock,
	RESCHEDULE: Clock,
	STOP: Square,
	RESTART: RotateCcw,
	EDIT: Pencil,
	DELETE: Trash2,
	TRASH: Trash,
	FOLDER: Folder,
	LINK: Link,
	INFO: Info,
};

const actionLabelsMap: Record<string, string> = {
	PAUSE: "task.pause-task",
	RESUME: "task.resume-task",
	START_NOW: "task.start-now",
	SCHEDULE: "task.schedule-task",
	RESCHEDULE: "task.reschedule-task",
	STOP: "task.stop-seeding",
	RESTART: "task.restart-task",
	EDIT: "task.edit-task",
	DELETE: "task.delete-task",
	TRASH: "task.remove-record",
	FOLDER: "task.show-in-folder",
	LINK: "task.copy-link",
	INFO: "task.task-detail-title",
};

const actionLabelFallbacks: Record<string, string> = {
	START_NOW: "Start Now",
	SCHEDULE: "Schedule...",
	RESCHEDULE: "Reschedule...",
};

export default {
	name: "task-item-actions",
	components: {
		Clock,
		Play,
		Pause,
		Pencil,
		Square,
		RotateCcw,
		Trash2,
		Trash,
		Folder,
		Link,
		Info,
		EllipsisVertical,
		DropdownMenu,
		DropdownMenuContent,
		DropdownMenuItem,
		DropdownMenuTrigger,
	},
	props: {
		mode: {
			type: String,
			default: "LIST",
			validator(value: string) {
				return ["LIST", "DETAIL"].indexOf(value) !== -1;
			},
		},
		task: {
			type: Object,
			required: true,
		},
	},
	computed: {
		taskName() {
			return getTaskName(this.task);
		},
		path() {
			return getTaskRevealPath(this.task);
		},
		fallbackPath() {
			const dir = `${this.task?.dir || ""}`.trim();
			return dir || getTaskFullPath(this.task);
		},
		isSeeder() {
			return checkTaskIsSeeder(this.task);
		},
		isMobileList() {
			return is.android() && this.mode === "LIST";
		},
		isCardView() {
			return usePreferenceStore().taskListStyle === "card";
		},
		taskStatus() {
			return getDisplayTaskStatus(this.task);
		},
		isBT() {
			return checkTaskIsBT(this.task);
		},
		statusActions() {
			const actions = taskActionsMap[this.taskStatus] || [];
			if (this.isBT) {
				return actions.filter((a) => a !== "SCHEDULE" && a !== "RESCHEDULE");
			}
			return actions;
		},
		primaryAction() {
			return this.statusActions[0];
		},
		overflowActions() {
			const rest = this.statusActions.slice(1);
			const common = this.isCardView
				? ["LINK", "INFO"]
				: ["FOLDER", "LINK", "INFO"];
			return [...rest, ...common];
		},
		actionIcons() {
			return actionIconsMap;
		},
		taskCommonActions() {
			const { mode } = this;
			const result = is.renderer() ? ["FOLDER"] : [];

			switch (mode) {
				case "LIST":
					result.push("LINK", "INFO");
					break;
				case "DETAIL":
					result.push("LINK");
					break;
			}

			return result;
		},
		taskActions() {
			const { statusActions, taskCommonActions } = this;
			const result = [...statusActions, ...taskCommonActions].reverse();
			return result;
		},
	},
	methods: {
		actionLabel(action: string) {
			const key = actionLabelsMap[action];
			const label = this.$t(key);
			return label === key ? actionLabelFallbacks[action] || key : label;
		},
		onActionClick(action, event) {
			switch (action) {
				case "PAUSE":
					this.onPauseClick();
					break;
				case "STOP":
					this.onStopClick();
					break;
				case "RESUME":
					this.onResumeClick();
					break;
				case "RESTART":
					this.onRestartClick(event);
					break;
				case "EDIT":
					this.onEditClick();
					break;
				case "DELETE":
					this.onDeleteClick(event);
					break;
				case "TRASH":
					this.onTrashClick(event);
					break;
				case "FOLDER":
					this.onFolderClick();
					break;
				case "LINK":
					this.onLinkClick();
					break;
				case "INFO":
					this.onInfoClick();
					break;
				case "SCHEDULE":
				case "RESCHEDULE":
					this.onScheduleClick();
					break;
				case "START_NOW":
					this.onStartNowClick();
					break;
			}
		},
		onResumeClick() {
			const { task, taskName } = this;
			commands.emit("resume-task", {
				task,
				taskName,
			});
		},
		onScheduleClick() {
			const { task, taskName } = this;
			commands.emit("schedule-task", { task, taskName });
		},
		onEditClick() {
			const { task, taskName } = this;
			commands.emit("edit-task", { task, taskName });
		},
		onStartNowClick() {
			const { task, taskName } = this;
			commands.emit("start-task-now", { task, taskName });
		},
		onRestartClick(event) {
			const { task, taskName } = this;
			const { status } = task;
			const showDialog = status === TASK_STATUS.COMPLETE || !!event.altKey;
			commands.emit("restart-task", {
				task,
				taskName,
				showDialog,
			});
		},
		onPauseClick() {
			const { task, taskName } = this;
			commands.emit("pause-task", {
				task,
				taskName,
			});
		},
		onStopClick() {
			if (!this.isSeeder) {
				return;
			}

			const { task } = this;
			commands.emit("stop-task-seeding", { task });
		},
		onDeleteClick(event) {
			const { task, taskName } = this;
			const deleteWithFiles = !!event.shiftKey;
			commands.emit("delete-task", {
				task,
				taskName,
				deleteWithFiles,
			});
		},
		onTrashClick(event) {
			const { task, taskName } = this;
			const deleteWithFiles = !!event.shiftKey;
			commands.emit("delete-task-record", {
				task,
				taskName,
				deleteWithFiles,
			});
		},
		onFolderClick() {
			if (is.android()) {
				const dir = getTaskRevealDir(this.task);
				commands.emit("reveal-in-folder", {
					path: dir,
					fallbackPath: dir,
				});
				return;
			}
			const { path, fallbackPath } = this;
			commands.emit("reveal-in-folder", { path, fallbackPath });
		},
		onLinkClick() {
			const { task } = this;
			commands.emit("copy-task-link", { task });
		},
		onInfoClick() {
			const { task } = this;
			const handled = commands.emit("show-task-info", { task });
			if (!handled) {
				useTaskStore().showTaskDetail(task);
			}
		},
	},
};
</script>
