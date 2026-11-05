<template>
  <div id="container" class="app-shell">
    <app-sidebar v-if="!isAndroid" />
    <div class="app-content">
      <title-bar v-if="showTitleBar" :showActions="showWindowActions" />
      <router-view v-slot="{ Component, route }">
        <Transition name="page" mode="out-in">
          <component
            :is="Component"
            :key="route.path.startsWith('/preference') ? 'preference' : route.path.startsWith('/rss') ? 'rss' : 'task'"
            class="page-view"
          />
        </Transition>
      </router-view>
    </div>
    <nav v-if="isAndroid" class="android-bottom-nav" :aria-label="$t('app.primary-navigation')">
      <button class="android-nav-item" :class="{ active: routePath.startsWith('/task') || routePath === '/' }" @click="nav('/task')">
        <ListTodo :size="22" />
        <span>{{ $t('app.nav-tasks') }}</span>
      </button>
      <button class="android-nav-item" :class="{ active: routePath.startsWith('/rss') }" @click="nav('/rss')">
        <Rss :size="22" />
        <span>{{ $t('app.nav-rss') }}</span>
      </button>
      <button class="android-fab" :aria-label="$t('app.add-task')" @click="showAddTask">
        <Plus :size="24" />
      </button>
      <button class="android-nav-item" :class="{ active: routePath.startsWith('/share') }" @click="nav('/share')">
        <Share2 :size="22" />
        <span>{{ $t('app.nav-share') }}</span>
      </button>
      <button class="android-nav-item" :class="{ active: routePath.startsWith('/preference') }" @click="nav('/preference')">
        <Settings2 :size="22" />
        <span>{{ $t('app.nav-settings') }}</span>
      </button>
    </nav>
    <add-task-dialog :visible="addTaskVisible" :type="addTaskType" />
    <create-torrent-dialog :visible="createTorrentVisible" />
    <missed-schedule-dialog />
    <about-panel :visible="aboutPanelVisible" />
    <drop-zone />
  </div>
</template>

<script lang="ts">
import { ListTodo, Plus, Rss, Settings2, Share2 } from "@lucide/vue";
import { ADD_TASK_TYPE } from "@shared/constants";
import { invoke } from "@tauri-apps/api/core";
import { toast } from "vue-sonner";
import AboutPanel from "@/components/About/AboutPanel.vue";
import Dragger from "@/components/Dragger/Index.vue";
import TitleBar from "@/components/Native/TitleBar.vue";
import Sidebar from "@/components/Sidebar/Index.vue";
import AddTask from "@/components/Task/AddTask.vue";
import CreateTorrentDialog from "@/components/Task/CreateTorrentDialog.vue";
import MissedScheduleDialog from "@/components/Task/MissedScheduleDialog.vue";
import is from "@/shims/platform";
import { useAppStore } from "@/store/app";
import { useTaskStore } from "@/store/task";

export default {
	name: "main-layout",
	components: {
		[AboutPanel.name]: AboutPanel,
		[Sidebar.name]: Sidebar,
		[TitleBar.name]: TitleBar,
		[AddTask.name]: AddTask,
		[CreateTorrentDialog.name]: CreateTorrentDialog,
		[MissedScheduleDialog.name]: MissedScheduleDialog,
		[Dragger.name]: Dragger,
		ListTodo,
		Plus,
		Rss,
		Settings2,
		Share2,
	},
	data() {
		return {
			androidBackGuardTimer: null as number | null,
			lastAndroidRootBackAt: 0,
		};
	},
	computed: {
		isAndroid() {
			return is.android();
		},
		showTitleBar() {
			return is.renderer() && !this.isAndroid;
		},
		showWindowActions() {
			return is.windows() || is.linux();
		},
		routePath() {
			return this.$route.path;
		},
		aboutPanelVisible() {
			return useAppStore().aboutPanelVisible;
		},
		addTaskVisible() {
			return useAppStore().addTaskVisible;
		},
		createTorrentVisible() {
			return useAppStore().createTorrentVisible;
		},
		addTaskType() {
			return useAppStore().addTaskType;
		},
	},
	mounted() {
		this.installAndroidBackGuard();
	},
	beforeUnmount() {
		this.removeAndroidBackGuard();
	},
	watch: {
		routePath() {
			this.scheduleAndroidBackGuard();
		},
	},
	methods: {
		nav(page: string) {
			this.$router
				.push({ path: page })
				.catch(() => undefined)
				.finally(() => this.scheduleAndroidBackGuard());
		},
		showAddTask() {
			useAppStore().showAddTaskDialog(ADD_TASK_TYPE.URI);
		},
		installAndroidBackGuard() {
			if (!this.isAndroid) {
				return;
			}
			this.pushAndroidBackGuard();
			window.addEventListener("popstate", this.onAndroidBackGesture);
			window.addEventListener("risuko-android-back", this.onAndroidBackGesture);
		},
		removeAndroidBackGuard() {
			if (this.androidBackGuardTimer != null) {
				window.clearTimeout(this.androidBackGuardTimer);
				this.androidBackGuardTimer = null;
			}
			window.removeEventListener("popstate", this.onAndroidBackGesture);
			window.removeEventListener(
				"risuko-android-back",
				this.onAndroidBackGesture,
			);
		},
		scheduleAndroidBackGuard() {
			if (!this.isAndroid) {
				return;
			}
			if (this.androidBackGuardTimer != null) {
				window.clearTimeout(this.androidBackGuardTimer);
			}
			this.androidBackGuardTimer = window.setTimeout(() => {
				this.androidBackGuardTimer = null;
				this.pushAndroidBackGuard();
			}, 0);
		},
		pushAndroidBackGuard() {
			if (!this.isAndroid || window.history.state?.risukoAndroidBackGuard) {
				return;
			}
			window.history.pushState(
				{
					...window.history.state,
					risukoAndroidBackGuard: true,
				},
				"",
				window.location.href,
			);
		},
		onAndroidBackGesture() {
			if (!this.isAndroid) {
				return;
			}
			this.handleAndroidBackGesture();
			this.scheduleAndroidBackGuard();
		},
		handleAndroidBackGesture() {
			const appStore = useAppStore();
			const taskStore = useTaskStore();
			if (appStore.cloudflareDialog.visible) {
				appStore.hideCloudflareDialog();
				return;
			}
			if (appStore.addTaskVisible) {
				appStore.hideAddTaskDialog();
				return;
			}
			if (appStore.createTorrentVisible) {
				appStore.hideCreateTorrentDialog();
				return;
			}
			if (appStore.aboutPanelVisible) {
				appStore.hideAboutPanel();
				return;
			}
			if (taskStore.taskDetailVisible) {
				taskStore.hideTaskDetail();
				return;
			}
			if (this.routePath !== "/task" && this.routePath !== "/") {
				this.$router.push({ path: "/task" }).catch(() => undefined);
				this.lastAndroidRootBackAt = 0;
				return;
			}
			const now = Date.now();
			if (now - this.lastAndroidRootBackAt <= 1800) {
				this.lastAndroidRootBackAt = 0;
				invoke("quit_app").catch(() => undefined);
				return;
			}
			this.lastAndroidRootBackAt = now;
			toast.info(this.$t("app.android-back-again-to-quit"));
		},
	},
};
</script>
