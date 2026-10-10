<template>
  <div class="main panel panel-layout panel-layout--v preference-page">
    <motion-enter tag="header" preset="fadeInDown" class="panel-header preference-header">
      <h4>{{ $t('subnav.preferences') }}</h4>
      <div class="preference-search">
        <Search :size="14" class="preference-search-icon" aria-hidden="true" />
        <input
          v-model="searchQuery"
          type="search"
          class="preference-search-input"
          :placeholder="$t('preferences.search-placeholder')"
          :title="$t('preferences.search-placeholder')"
          :aria-label="$t('preferences.search-placeholder')"
          @keydown.esc="clearSearch"
        />
        <button
          v-if="searchQuery"
          type="button"
          class="preference-search-clear"
          :title="$t('preferences.search-clear')"
          :aria-label="$t('preferences.search-clear')"
          @click="clearSearch"
        >
          <X :size="13" aria-hidden="true" />
        </button>
      </div>
    </motion-enter>
    <nav v-if="isAndroid" class="settings-nav-mobile" :aria-label="$t('subnav.preferences')">
      <DropdownMenu>
        <DropdownMenuTrigger class="settings-nav-trigger" type="button">
          <component :is="currentTab.icon" :size="16" />
          <span class="settings-nav-trigger-label">{{ currentTab.title }}</span>
          <ChevronDown :size="16" class="settings-nav-chevron" />
        </DropdownMenuTrigger>
        <DropdownMenuContent align="start">
          <DropdownMenuItem
            v-for="tab in tabs"
            :key="tab.key"
            @click="onSectionSelect(tab)"
          >
            <component :is="tab.icon" :size="14" />
            <span>{{ tab.title }}</span>
          </DropdownMenuItem>
        </DropdownMenuContent>
      </DropdownMenu>
    </nav>
    <LayoutGroup v-else id="settings-tabs">
      <nav class="settings-tabs" :aria-label="$t('subnav.preferences')">
        <router-link
          v-for="tab in tabs"
          :key="tab.key"
          :to="tab.route"
          class="settings-tab"
          :class="{ active: current === tab.key }"
        >
          <Motion
            v-if="current === tab.key"
            layout-id="settings-tab-pill"
            class="settings-tab-pill"
            :initial="false"
            :transition="pillTransition"
          />
          <component :is="tab.icon" :size="14" class="settings-tab-icon" />
          <span class="settings-tab-label">{{ tab.title }}</span>
        </router-link>
      </nav>
    </LayoutGroup>
    <section
      v-if="hasSearchQuery"
      class="preference-search-results panel-content"
      :aria-label="$t('preferences.search-all-tabs')"
    >
      <div class="preference-search-results-inner">
        <div class="preference-search-results-header">
          <div>
            <h3>{{ $t('preferences.search-all-tabs') }}</h3>
            <p aria-live="polite">
              {{ $t('preferences.search-results', { count: searchResults.length }) }}
            </p>
          </div>
        </div>
        <div v-if="searchResults.length" class="preference-search-result-list">
          <button
            v-for="result in searchResults"
            :key="result.key"
            type="button"
            class="preference-search-result"
            @click="onSearchResultSelect(result)"
          >
            <span class="preference-search-result-icon" aria-hidden="true">
              <component :is="getTab(result.route).icon" :size="15" />
            </span>
            <span class="preference-search-result-content">
              <span class="preference-search-result-label">{{ result.label }}</span>
              <span class="preference-search-result-tab">
                {{ getTab(result.route).title }}
              </span>
            </span>
            <ArrowRight :size="14" class="preference-search-result-arrow" aria-hidden="true" />
          </button>
        </div>
        <div v-else class="preference-search-empty" role="status">
          <SearchX :size="22" aria-hidden="true" />
          <span>{{ $t('preferences.search-no-results') }}</span>
        </div>
      </div>
    </section>
    <div v-show="!hasSearchQuery" class="preference-route-view">
      <router-view v-slot="{ Component, route }">
        <Transition name="page" mode="out-in">
          <component
            :is="Component"
            :key="route.path"
            class="pref-form-view"
            :data-preference-search-route="route.path"
          />
        </Transition>
      </router-view>
    </div>
  </div>
</template>

<script lang="ts">
import {
	ArrowRight,
	ChevronDown,
	Cloud,
	Palette,
	Radio,
	RefreshCw,
	Search,
	SearchX,
	SlidersHorizontal,
	Wrench,
	X,
} from "@lucide/vue";
import { LayoutGroup, Motion } from "motion-v";
import { getLocaleManager } from "@/components/Locale";
import {
	DropdownMenu,
	DropdownMenuContent,
	DropdownMenuItem,
	DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import is from "@/shims/platform";
import { usePreferenceStore } from "@/store/preference";
import {
	buildPreferenceSearchEntries,
	filterPreferenceSearchEntries,
	normalizeSearchText,
	resolvePreferenceSearchTarget,
} from "./search";

const SEARCH_TARGET_RETRY_LIMIT = 40;
const SEARCH_TARGET_RETRY_DELAY = 75;

function getPreferenceSearchEntries() {
	const i18n = getLocaleManager().getI18n();
	return buildPreferenceSearchEntries(
		i18n.getResourceBundle(i18n.language, "translation"),
		i18n.getResourceBundle("en-US", "translation"),
		(key) => i18n.t(key),
		{
			android: is.android(),
			macOS: is.macOS(),
			renderer: is.renderer(),
		},
	);
}

export default {
	name: "preference-page",
	components: {
		LayoutGroup,
		Motion,
		DropdownMenu,
		DropdownMenuContent,
		DropdownMenuItem,
		DropdownMenuTrigger,
		ArrowRight,
		ChevronDown,
		Search,
		SearchX,
		X,
	},
	data() {
		return {
			searchQuery: "",
			searchEntries: getPreferenceSearchEntries(),
			searchRetryTimer: undefined as number | undefined,
		};
	},
	computed: {
		isAndroid() {
			return is.android();
		},
		tabs() {
			return [
				{
					key: "basic",
					title: this.$t("preferences.basic"),
					route: "/preference/basic",
					icon: SlidersHorizontal,
				},
				{
					key: "appearance",
					title: this.$t("preferences.appearance"),
					route: "/preference/appearance",
					icon: Palette,
				},
				{
					key: "advanced",
					title: this.$t("preferences.advanced"),
					route: "/preference/advanced",
					icon: Wrench,
				},
				{
					key: "usenet",
					title: this.$t("preferences.usenet"),
					route: "/preference/usenet",
					icon: Radio,
				},
				{
					key: "cloud-sinks",
					title: this.$t("preferences.cloudSinks"),
					route: "/preference/cloud-sinks",
					icon: Cloud,
				},
				{
					key: "sync",
					title: this.$t("subnav.sync"),
					route: "/preference/sync",
					icon: RefreshCw,
				},
			];
		},
		current() {
			return this.$route.path.split("/")[2] || "basic";
		},
		currentTab() {
			return this.tabs.find((tab) => tab.key === this.current) || this.tabs[0];
		},
		hasSearchQuery() {
			return !!normalizeSearchText(this.searchQuery);
		},
		searchResults() {
			return filterPreferenceSearchEntries(
				this.searchEntries,
				this.searchQuery,
			);
		},
		pillTransition() {
			return { duration: 0.16, ease: [0.25, 1, 0.5, 1] };
		},
	},
	watch: {
		"$route.query.setting": {
			handler(value) {
				if (typeof value === "string" && value && !this.hasSearchQuery) {
					const fallback =
						typeof this.$route.query.fallback === "string"
							? this.$route.query.fallback
							: undefined;
					this.scheduleSearchTarget(value, fallback);
				}
			},
			immediate: true,
		},
	},
	created() {
		getLocaleManager()
			.getI18n()
			.on("languageChanged", this.refreshSearchEntries);
		usePreferenceStore().fetchPreference();
	},
	beforeUnmount() {
		window.clearTimeout(this.searchRetryTimer);
		getLocaleManager()
			.getI18n()
			.off("languageChanged", this.refreshSearchEntries);
	},
	methods: {
		refreshSearchEntries() {
			this.searchEntries = getPreferenceSearchEntries();
		},
		getTab(route) {
			return this.tabs.find((tab) => tab.key === route) || this.tabs[0];
		},
		onSectionSelect(tab) {
			this.$router.push({ path: tab.route }).catch(() => {});
		},
		clearSearch() {
			this.searchQuery = "";
			if (this.$route.query.setting || this.$route.query.fallback) {
				const query = { ...this.$route.query };
				delete query.setting;
				delete query.fallback;
				this.$router.replace({ query }).catch(() => {});
			}
		},
		async onSearchResultSelect(result) {
			const fallback = result.target === result.key ? undefined : result.target;
			try {
				await this.$router.push({
					path: `/preference/${result.route}`,
					query: { setting: result.key, fallback },
				});
				if (
					this.$route.path === `/preference/${result.route}` &&
					this.$route.query.setting === result.key
				) {
					this.searchQuery = "";
					await this.$nextTick();
					this.scheduleSearchTarget(result.key, fallback);
				}
			} catch {}
		},
		scheduleSearchTarget(settingKey, fallbackTarget, attempt = 0) {
			if (this.$route.query.setting !== settingKey) {
				return;
			}
			this.$nextTick(() => {
				if (this.$route.query.setting !== settingKey) {
					return;
				}
				const found = this.scrollToSearchTarget(settingKey, fallbackTarget);
				if (found || attempt >= SEARCH_TARGET_RETRY_LIMIT) {
					return;
				}
				window.clearTimeout(this.searchRetryTimer);
				this.searchRetryTimer = window.setTimeout(
					() =>
						this.scheduleSearchTarget(settingKey, fallbackTarget, attempt + 1),
					SEARCH_TARGET_RETRY_DELAY,
				);
			});
		},
		scrollToSearchTarget(settingKey, fallbackTarget) {
			const target =
				this.findSearchTarget(settingKey) ||
				(fallbackTarget ? this.findSearchTarget(fallbackTarget) : null);
			if (!target) {
				return false;
			}
			target.classList.add("preference-search-target");
			target.scrollIntoView({ behavior: "smooth", block: "center" });
			this.focusSearchTarget(target);
			window.setTimeout(() => {
				target.classList.remove("preference-search-target");
			}, 1400);
			return true;
		},
		findSearchTarget(settingKey) {
			const routeView = [...document.querySelectorAll(".pref-form-view")].find(
				(element) =>
					element.getAttribute("data-preference-search-route") ===
					this.$route.path,
			);
			if (!routeView) {
				return null;
			}

			const markedTargets = [
				...routeView.querySelectorAll("[data-preference-search-target]"),
			].map((element) => ({
				target: element,
				keys: (element.getAttribute("data-preference-search-target") || "")
					.split(/\s+/)
					.filter(Boolean),
			}));

			const label = String(this.$t(settingKey) || "")
				.replace(/\{\{[^}]+\}\}/g, "")
				.trim();
			const selector = [
				".settings-row-title",
				".settings-select-item-label",
				".settings-section-header h3",
				".section-title h3",
				".limit-field-label",
				".toggle-title",
				".archive-summary-title",
				".dev-path-card-label",
				".dev-danger-action-title",
				"label",
			].join(",");
			const candidates = [...routeView.querySelectorAll(selector)].map(
				(element) => ({
					target: this.getSearchTargetContainer(element),
					text: element.textContent || "",
				}),
			);
			const target = resolvePreferenceSearchTarget(
				settingKey,
				label,
				markedTargets,
				candidates,
			);
			if (!target) {
				return null;
			}
			return this.markSearchTarget(target, settingKey);
		},
		getSearchTargetContainer(candidate) {
			return (
				candidate.closest(
					".settings-row, .settings-select-item, .form-item-sub, .typography-row, .limit-field, .archive-summary-row, .dev-path-card, .dev-danger-action, .settings-section",
				) || candidate
			);
		},
		markSearchTarget(target, settingKey) {
			const marker = "data-preference-search-target";
			const keys = (target.getAttribute(marker) || "")
				.split(/\s+/)
				.filter(Boolean);
			if (!keys.includes(settingKey)) {
				target.setAttribute(marker, [...keys, settingKey].join(" "));
			}
			return target;
		},
		focusSearchTarget(target) {
			const focusable = target.querySelector(
				'input:not([disabled]), textarea:not([disabled]), button:not([disabled]), [role="switch"]:not([disabled]), [role="checkbox"]:not([disabled]), [role="radio"]:not([disabled])',
			);
			if (focusable instanceof HTMLElement) {
				focusable.focus({ preventScroll: true });
				return;
			}

			if (!(target instanceof HTMLElement)) {
				return;
			}
			if (!target.hasAttribute("tabindex")) {
				target.setAttribute("tabindex", "-1");
			}
			target.focus({ preventScroll: true });
		},
	},
};
</script>
