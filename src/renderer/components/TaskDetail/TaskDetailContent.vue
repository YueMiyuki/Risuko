<template>
  <Tabs v-model="activeTab" default-value="general" class="task-detail-tab">
    <TabsList>
      <TabsTrigger value="general">
        <span class="task-detail-tab-label">
          <Info :size="14" />
          <span>{{ $t('task.detail-tab-general') }}</span>
        </span>
      </TabsTrigger>
      <TabsTrigger value="activity">
        <span class="task-detail-tab-label">
          <Activity :size="14" />
          <span>{{ $t('task.detail-tab-activity') }}</span>
        </span>
      </TabsTrigger>
      <TabsTrigger v-if="isBT" value="trackers">
        <span class="task-detail-tab-label">
          <Radar :size="14" />
          <span>{{ $t('task.detail-tab-trackers') }}</span>
        </span>
      </TabsTrigger>
      <TabsTrigger v-if="isBT" value="peers">
        <span class="task-detail-tab-label">
          <Users :size="14" />
          <span>{{ $t('task.detail-tab-peers') }}</span>
        </span>
      </TabsTrigger>
      <TabsTrigger value="files">
        <span class="task-detail-tab-label">
          <Files :size="14" />
          <span>{{ $t('task.detail-tab-files') }}</span>
        </span>
      </TabsTrigger>
    </TabsList>
    <TabsContent value="general">
      <motion-enter preset="fadeInUp" :duration="0.2">
        <task-general :task="task" />
      </motion-enter>
    </TabsContent>
    <TabsContent value="activity">
      <motion-enter preset="fadeInUp" :duration="0.2">
        <task-activity ref="taskGraphic" :task="task" />
      </motion-enter>
    </TabsContent>
    <TabsContent v-if="isBT" value="trackers">
      <motion-enter preset="fadeInUp" :duration="0.2">
        <task-trackers :task="task" />
      </motion-enter>
    </TabsContent>
    <TabsContent v-if="isBT" value="peers">
      <motion-enter preset="fadeInUp" :duration="0.2" class="task-detail-tab-fill">
        <task-peers :peers="peers" />
      </motion-enter>
    </TabsContent>
    <TabsContent value="files">
      <motion-enter preset="fadeInUp" :duration="0.2" class="task-detail-tab-fill">
        <task-files
          ref="detailFileList"
          mode="DETAIL"
          :files="fileList"
          @selection-change="handleSelectionChange"
        />
      </motion-enter>
    </TabsContent>
  </Tabs>
  <div class="task-detail-actions">
    <div class="action-wrapper action-wrapper-left" v-if="optionsChanged">
      <ui-button @click="resetChanged">
        {{ $t('app.reset') }}
      </ui-button>
    </div>
    <div class="action-wrapper action-wrapper-center" v-if="task">
      <task-item-actions mode="DETAIL" :task="task" />
    </div>
    <div class="action-wrapper action-wrapper-right" v-if="optionsChanged">
      <ui-button variant="primary" @click="saveChanged">
        {{ $t('app.save') }}
      </ui-button>
    </div>
  </div>
</template>

<script lang="ts">
import { Activity, Files, Info, Radar, Users } from "@lucide/vue";
import { NONE_SELECTED_FILES, SELECTED_ALL_FILES } from "@shared/constants";
import { checkTaskIsBT, getFileExtension, getFileName } from "@shared/utils";
import { debounce } from "lodash";
import TaskItemActions from "@/components/Task/TaskItemActions.vue";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { useTaskStore } from "@/store/task";
import TaskActivity from "./TaskActivity.vue";
import TaskFiles from "./TaskFiles.vue";
import TaskGeneral from "./TaskGeneral.vue";
import TaskPeers from "./TaskPeers.vue";
import TaskTrackers from "./TaskTrackers.vue";

const PEERS_POLL_INTERVAL = 3000;

export default {
	name: "task-detail-content",
	components: {
		[TaskItemActions.name]: TaskItemActions,
		[TaskGeneral.name]: TaskGeneral,
		[TaskActivity.name]: TaskActivity,
		[TaskTrackers.name]: TaskTrackers,
		[TaskPeers.name]: TaskPeers,
		[TaskFiles.name]: TaskFiles,
		Tabs,
		TabsContent,
		TabsList,
		TabsTrigger,
		Activity,
		Files,
		Info,
		Radar,
		Users,
	},
	props: {
		gid: {
			type: String,
		},
		task: {
			type: Object,
		},
		files: {
			type: Array,
			default() {
				return [];
			},
		},
		peers: {
			type: Array,
			default() {
				return [];
			},
		},
	},
	data() {
		return {
			activeTab: "general",
			optionsChanged: false,
			filesSelection: "",
			selectionChangedCount: 0,
			updateGraphicWidthDebounced: null,
			peersTimer: null as ReturnType<typeof setInterval> | null,
		};
	},
	computed: {
		isBT() {
			return this.task ? checkTaskIsBT(this.task) : false;
		},
		fileList() {
			const { files, task } = this;
			const parkedFailed = !!(
				task &&
				task.status === "paused" &&
				task.errorCode
			);
			const result = files.map((item) => {
				const name = getFileName(item.path);
				const extension = getFileExtension(name);
				const length = parseInt(item.length, 10);
				const selected = item.selected === "true";
				return {
					idx: Number(item.index),
					selected,
					path: item.path,
					name,
					extension: `.${extension}`,
					length,
					completedLength: item.completedLength,
					failed:
						parkedFailed && selected && Number(item.completedLength) < length,
				};
			});
			return result;
		},
		selectedFileList() {
			const { fileList } = this;
			const result = fileList.filter((item) => item.selected);

			return result;
		},
	},
	mounted() {
		this.updateGraphicWidthDebounced = debounce(() => {
			if (this.activeTab === "activity" && this.$refs.taskGraphic) {
				this.$refs.taskGraphic.updateGraphicWidth();
			}
		}, 250);
		window.addEventListener("resize", this.updateGraphicWidthDebounced);
	},
	beforeUnmount() {
		window.removeEventListener("resize", this.updateGraphicWidthDebounced);
		if (this.updateGraphicWidthDebounced?.cancel) {
			this.updateGraphicWidthDebounced.cancel();
		}
		this.stopPeersPolling();
	},
	watch: {
		gid() {
			this.activeTab = "general";
			this.optionsChanged = false;
			this.resetFaskFilesSelection();
			this.stopPeersPolling();
		},
		activeTab(newTab, oldTab) {
			this.optionsChanged = false;
			switch (oldTab) {
				case "peers":
					this.stopPeersPolling();
					break;
				case "files":
					this.resetFaskFilesSelection();
					break;
			}
			switch (newTab) {
				case "peers":
					this.startPeersPolling();
					break;
				case "files":
					this.$nextTick(() => {
						this.updateFilesListSelection();
					});
					break;
			}
		},
	},
	methods: {
		resetChanged() {
			if (this.activeTab === "files") {
				this.resetFaskFilesSelection();
				this.updateFilesListSelection();
			}
			this.optionsChanged = false;
		},
		startPeersPolling() {
			this.stopPeersPolling();
			if (!this.gid || !this.isBT) {
				return;
			}
			const taskStore = useTaskStore();
			let inFlight = false;
			const tick = async () => {
				if (inFlight || document.hidden || !this.gid) {
					return;
				}
				inFlight = true;
				try {
					await taskStore.fetchItemWithPeers(this.gid);
				} catch {
				} finally {
					inFlight = false;
				}
			};
			tick();
			this.peersTimer = setInterval(tick, PEERS_POLL_INTERVAL);
		},
		stopPeersPolling() {
			if (this.peersTimer !== null) {
				clearInterval(this.peersTimer);
				this.peersTimer = null;
			}
			useTaskStore().currentTaskPeers = [];
		},
		saveChanged() {
			if (this.activeTab === "files") {
				this.saveFaskFilesSelection();
			}
			this.optionsChanged = false;
		},
		updateFilesListSelection() {
			if (!this.$refs.detailFileList) {
				return;
			}

			const { selectedFileList } = this;
			this.$refs.detailFileList.toggleSelection(selectedFileList);
		},
		handleSelectionChange(val) {
			this.filesSelection = val;
			this.selectionChangedCount += 1;
			if (this.selectionChangedCount > 1) {
				this.optionsChanged = true;
			}
		},
		resetFaskFilesSelection() {
			this.filesSelection = "";
			this.selectionChangedCount = 0;
		},
		saveFaskFilesSelection() {
			const { gid, filesSelection } = this;
			if (filesSelection === NONE_SELECTED_FILES) {
				this.$msg.warning(this.$t("task.select-at-least-one"));
				return;
			}

			const options = {
				selectFile: filesSelection !== SELECTED_ALL_FILES ? filesSelection : "",
			};
			useTaskStore().changeTaskOption({ gid, options });
		},
	},
};
</script>
