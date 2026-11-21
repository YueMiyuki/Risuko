<template>
  <aside
    class="app-sidebar sidebar"
    :class="{ 'sidebar--mac': isMac, 'sidebar--collapsed': collapsed }"
    @mousedown.self.left="handleStartDragging"
  >
    <div
      class="sidebar-brand"
      data-tauri-drag-region
      @mousedown.left.self.prevent="handleStartDragging"
    >
      <img src="@static/logo.svg" class="sidebar-logo" alt="Risuko" />
      <span class="sidebar-wordmark">Risuko</span>
      <span v-if="appVersion" class="sidebar-version">{{ appVersion }}</span>
    </div>
    <div class="sidebar-add">
      <button class="sidebar-add-btn" :title="$t('app.add-task')" @click="showAddTask()">
        <Plus :size="15" />
        <span>{{ $t('app.add-task') }}</span>
      </button>
    </div>
    <LayoutGroup id="sidebar-nav">
      <nav class="sidebar-nav">
        <ul class="sidebar-group">
          <li
            v-for="item in taskItems"
            :key="item.key"
            class="sidebar-item"
            :class="{ active: activeKey === item.key }"
            :title="collapsed ? $t(item.label) : undefined"
            @click="nav(item.route)"
          >
            <Motion
              v-if="activeKey === item.key"
              layout-id="sidebar-active-pill"
              class="sidebar-pill"
              :initial="false"
              :transition="pillTransition"
            />
            <i class="sidebar-item-icon">
              <component :is="item.icon" :size="15" />
            </i>
            <span class="sidebar-item-label">{{ $t(item.label) }}</span>
            <span v-if="taskCounts[item.count] > 0" class="sidebar-item-count">
              {{ taskCounts[item.count] }}
            </span>
          </li>
        </ul>
        <ul class="sidebar-group">
          <li
            v-for="item in appItems"
            :key="item.key"
            class="sidebar-item"
            :class="{ active: activeKey === item.key }"
            :title="collapsed ? $t(item.label) : undefined"
            @click="nav(item.route)"
          >
            <Motion
              v-if="activeKey === item.key"
              layout-id="sidebar-active-pill"
              class="sidebar-pill"
              :initial="false"
              :transition="pillTransition"
            />
            <i class="sidebar-item-icon">
              <component :is="item.icon" :size="15" />
            </i>
            <span class="sidebar-item-label">{{ $t(item.label) }}</span>
          </li>
        </ul>
      </nav>
      <div class="sidebar-foot">
        <div class="sidebar-transfer">
          <span class="sidebar-transfer-rate">
            <ArrowDown :size="12" />
            {{ formatBytes(stat.downloadSpeed) }}/s
          </span>
          <span class="sidebar-transfer-rate">
            <ArrowUp :size="12" />
            {{ formatBytes(stat.uploadSpeed) }}/s
          </span>
          <button
            class="sidebar-transfer-mode"
            :title="$t('preferences.transfer-settings')"
            @click="toggleEngineMode"
          >
            {{ engineMode }}
          </button>
        </div>
        <ul class="sidebar-group">
          <li
            class="sidebar-item"
            :class="{ active: activeKey === 'stats' }"
            :title="collapsed ? $t('app.nav-stats') : undefined"
            @click="nav('/stats')"
          >
            <Motion
              v-if="activeKey === 'stats'"
              layout-id="sidebar-active-pill"
              class="sidebar-pill"
              :initial="false"
              :transition="pillTransition"
            />
            <i class="sidebar-item-icon">
              <Activity :size="15" />
            </i>
            <span class="sidebar-item-label">{{ $t('app.nav-stats') }}</span>
          </li>
          <li
            class="sidebar-item"
            :class="{ active: activeKey === 'settings' }"
            :title="collapsed ? $t('app.nav-settings') : undefined"
            @click="nav('/preference')"
          >
            <Motion
              v-if="activeKey === 'settings'"
              layout-id="sidebar-active-pill"
              class="sidebar-pill"
              :initial="false"
              :transition="pillTransition"
            />
            <i class="sidebar-item-icon">
              <Settings2 :size="15" />
            </i>
            <span class="sidebar-item-label">{{ $t('app.nav-settings') }}</span>
          </li>
          <li class="sidebar-item" :title="collapsed ? $t('app.about') : undefined" @click="showAboutPanel">
            <i class="sidebar-item-icon">
              <Info :size="15" />
            </i>
            <span class="sidebar-item-label">{{ $t('app.about') }}</span>
          </li>
        </ul>
      </div>
    </LayoutGroup>
  </aside>
</template>

<script lang="ts">
import {
	Activity,
	ArrowDown,
	ArrowUp,
	CircleCheck,
	Clock,
	Info,
	LayoutList,
	Pause,
	Play,
	Plus,
	Settings2,
	Share2,
	Square,
} from "@lucide/vue";
import { ADD_TASK_TYPE } from "@shared/constants";
import { bytesToSize } from "@shared/utils";
import logger from "@shared/utils/logger";
import { getCurrentWebviewWindow } from "@tauri-apps/api/webviewWindow";
import { LayoutGroup, Motion } from "motion-v";
import is from "@/shims/platform";
import { useAppStore } from "@/store/app";
import { usePreferenceStore } from "@/store/preference";
import { useTaskStore } from "@/store/task";
import { getRisukoVersion } from "@/utils/version";

const appWindow = getCurrentWebviewWindow();

export default {
	name: "AppSidebar",
	components: {
		Activity,
		ArrowDown,
		ArrowUp,
		Info,
		LayoutGroup,
		Motion,
		Plus,
		Settings2,
	},
	data() {
		return {
			appVersion: "",
		};
	},
	async created() {
		this.appVersion = await getRisukoVersion();
	},
	computed: {
		isMac() {
			return is.macOS();
		},
		stat() {
			return useAppStore().stat;
		},
		engineMode() {
			return usePreferenceStore().engineMode;
		},
		collapsed() {
			return usePreferenceStore().sidebarCollapsed;
		},
		taskCounts() {
			return useTaskStore().taskCountMap;
		},
		taskItems() {
			return [
				{
					key: "task:all",
					icon: LayoutList,
					label: "task.all",
					route: "/task",
					count: "all",
				},
				{
					key: "task:active",
					icon: Play,
					label: "task.active",
					route: "/task/active",
					count: "active",
				},
				{
					key: "task:waiting",
					icon: Pause,
					label: "task.waiting",
					route: "/task/waiting",
					count: "waiting",
				},
				{
					key: "task:completed",
					icon: CircleCheck,
					label: "task.completed",
					route: "/task/completed",
					count: "completed",
				},
				{
					key: "task:stopped",
					icon: Square,
					label: "task.stopped",
					route: "/task/stopped",
					count: "stopped",
				},
				{
					key: "task:scheduled",
					icon: Clock,
					label: "task.scheduled",
					route: "/task/scheduled",
					count: "scheduled",
				},
			];
		},
		appItems() {
			return [
				{ key: "share", icon: Share2, label: "app.nav-share", route: "/share" },
				{
					key: "health",
					icon: Activity,
					label: "app.nav-health",
					route: "/health",
				},
			];
		},
		activeKey() {
			const path = this.$route.path;
			if (path === "/" || path === "/task") {
				return "task:all";
			}
			if (path.startsWith("/task/")) {
				return `task:${this.$route.params.status || "all"}`;
			}
			if (path.startsWith("/share")) {
				return "share";
			}
			if (path.startsWith("/health")) {
				return "health";
			}
			if (path.startsWith("/stats")) {
				return "stats";
			}
			if (path.startsWith("/preference")) {
				return "settings";
			}
			return "";
		},
		pillTransition() {
			return { duration: 0.16, ease: [0.25, 1, 0.5, 1] };
		},
	},
	methods: {
		handleStartDragging() {
			appWindow.startDragging().catch(() => {});
		},
		showAddTask() {
			useAppStore().showAddTaskDialog(ADD_TASK_TYPE.URI);
		},
		showAboutPanel() {
			useAppStore().showAboutPanel();
		},
		toggleEngineMode() {
			usePreferenceStore().toggleEngineMode();
		},
		formatBytes(value: number) {
			return bytesToSize(value);
		},
		nav(page: string) {
			this.$router
				.push({
					path: page,
				})
				.catch((err) => {
					logger.log(err);
				});
		},
	},
};
</script>
