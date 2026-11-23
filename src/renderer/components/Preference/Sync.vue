<template>
  <div class="content panel panel-layout panel-layout--v">
    <main class="panel-content">
      <form class="form-preference" @submit.prevent>

        <div class="settings-section" data-preference-search-target="sync.cloud-sync">
          <div class="settings-section-header">
            <div class="section-icon"><Cloud :size="16" /></div>
            <div class="section-title">
              <h3>{{ $t('sync.cloud-sync') }}</h3>
              <p class="section-desc">{{ $t('sync.cloud-sync-description') }}</p>
            </div>
          </div>
          <div class="settings-section-content">
            <div
              class="settings-row sync-server-row"
              data-preference-search-target="sync.cloud-sync-server-url"
            >
              <div class="settings-row-content">
                <div class="settings-row-title">{{ $t('sync.cloud-sync-server-url') }}</div>
                <div class="settings-row-description">
                  {{ $t('sync.cloud-sync-server-url-description') }}
                </div>
                <div class="sync-server-input">
                  <Input
                    v-model="serverUrl"
                    :placeholder="$t('sync.cloud-sync-server-url-placeholder')"
                    @change="saveServerUrl"
                  />
                </div>
              </div>
            </div>


            <div v-if="serverUrl && serverConfigChecked" class="settings-row">
              <div class="settings-row-content">
                <div v-if="authStore.serverConfig" class="sync-status-line sync-status--ok">
                  <CheckCircle2 :size="14" class="sync-status-icon" />
                  {{ $t('sync.server-connected') }}
                </div>
                <div v-else class="sync-status-line sync-status--err">
                  <XCircle :size="14" class="sync-status-icon" />
                  {{ $t('sync.server-unreachable') }}
                </div>
              </div>
            </div>
          </div>
        </div>

        <div class="settings-section">
          <div class="settings-section-header">
            <div class="section-icon"><User :size="16" /></div>
            <div class="section-title">
              <h3>{{ $t('sync.login') }}</h3>
            </div>
          </div>
          <div class="settings-section-content">
            <div v-if="!authStore.isLoggedIn" class="settings-row">
              <div class="settings-row-content">
                <span class="settings-row-title">{{ $t('sync.login-required') }}</span>
              </div>
              <div class="settings-row-action settings-row-action--right">
                <Button size="sm" @click="showLoginModal = true">
                  {{ $t('sync.login') }}
                </Button>
              </div>
            </div>

            <div v-else class="settings-row">
              <div class="settings-row-content">
                <span class="settings-row-title">
                  {{ $t('sync.login-logged-in-as') }}
                  {{ authStore.user?.email || authStore.user?.githubUsername }}
                </span>
              </div>
              <div class="settings-row-action settings-row-action--right">
                <Button size="sm" variant="outline" @click="handleLogout">
                  {{ $t('sync.logout') }}
                </Button>
              </div>
            </div>
          </div>
        </div>

        <div class="settings-section hidden-sm-and-up">
          <div class="settings-section-header">
            <div class="section-icon"><Activity :size="16" /></div>
            <div class="section-title">
              <h3>{{ $t('share.tools') }}</h3>
            </div>
          </div>
          <div class="settings-section-content">
            <button type="button" class="settings-row sync-link-row" @click="goHealth">
              <div class="settings-row-content">
                <div class="settings-row-title">{{ $t('app.nav-health') }}</div>
                <div class="settings-row-description">{{ $t('share.health-description') }}</div>
              </div>
              <div class="settings-row-action settings-row-action--right">
                <ChevronRight :size="18" />
              </div>
            </button>
          </div>
        </div>

        <div class="settings-section" data-preference-search-target="sync.cloud-sync-categories">
          <div class="settings-section-header">
            <div class="section-icon"><RefreshCw :size="16" /></div>
            <div class="section-title">
              <h3>{{ $t('sync.cloud-sync-categories') }}</h3>
              <p class="section-desc">{{ $t('sync.cloud-sync-categories-description') }}</p>
            </div>
            <Button
              v-if="authStore.isLoggedIn && selectedCategories.length > 0"
              size="sm"
              variant="ghost"
              @click="deselectAllCategories"
            >
              {{ $t('sync.cloud-sync-deselect-all') }}
            </Button>
            <Button
              v-else-if="authStore.isLoggedIn"
              size="sm"
              variant="ghost"
              @click="selectAllCategories"
            >
              {{ $t('sync.cloud-sync-select-all') }}
            </Button>
          </div>
          <div class="settings-section-content" :class="{ 'sync-section-disabled': !authStore.isLoggedIn }">
            <div v-if="!authStore.isLoggedIn" class="sync-section-lock-hint">
              {{ $t('sync.cloud-sync-login-required') }}
            </div>

            <div class="settings-row" data-preference-search-target="sync.cloud-sync-auto">
              <div class="settings-row-content">
                <div class="settings-row-title">{{ $t('sync.cloud-sync-auto') }}</div>
                <div class="settings-row-description">
                  {{ $t('sync.cloud-sync-auto-description') }}
                </div>
              </div>
              <div class="settings-row-action">
                <ui-checkbox
                  :model-value="!!form.cloudSyncAuto"
                  :disabled="!authStore.isLoggedIn"
                  @change="(val) => form.cloudSyncAuto = !!val"
                />
              </div>
            </div>

            <div
              v-for="cat in syncCategories"
              :key="cat.id"
              class="settings-row"
              :data-preference-search-target="`sync.category-${cat.id}`"
            >
              <div class="settings-row-content">
                <div class="settings-row-title">{{ $t(`sync.category-${cat.id}`) }}</div>
              </div>
              <div class="settings-row-action">
                <ui-checkbox
                  :model-value="selectedCategories.includes(cat.id)"
                  :disabled="!authStore.isLoggedIn"
                  @change="(val) => toggleCategory(cat.id, !!val)"
                />
              </div>
            </div>
          </div>
        </div>

        <div class="settings-section">
          <div class="settings-section-header">
            <div class="section-icon"><Zap :size="16" /></div>
            <div class="section-title">
              <h3>{{ $t('sync.cloud-sync-actions') }}</h3>
              <p class="section-desc">{{ $t('sync.cloud-sync-actions-description') }}</p>
            </div>
          </div>
          <div class="settings-section-content" :class="{ 'sync-section-disabled': !authStore.isLoggedIn }">
            <div v-if="!authStore.isLoggedIn" class="sync-section-lock-hint">
              {{ $t('sync.cloud-sync-login-required') }}
            </div>

            <div class="settings-row">
              <div class="settings-row-content">
                <div class="settings-row-title">{{ $t('sync.cloud-sync-last-at') }}</div>
              </div>
              <div class="settings-row-action settings-row-action--right">
                <span class="sync-last-time">{{ lastSyncText }}</span>
              </div>
            </div>

            <div v-if="selectedCategories.length === 0" class="sync-empty-state">
              <p class="sync-empty-title">{{ $t('sync.cloud-sync-no-categories') }}</p>
              <p class="sync-empty-hint">{{ $t('sync.cloud-sync-no-categories-description') }}</p>
            </div>

            <div v-else class="settings-row">
              <div class="settings-row-content">
                <span v-if="syncStore.syncing" class="settings-row-title sync-status--syncing">
                  <Loader2 :size="14" class="sync-status-icon animate-spin" />
                  {{ $t('sync.cloud-sync-syncing') }}
                </span>
                <span v-else class="settings-row-description">
                  {{ $t('sync.cloud-sync-categories-selected', { count: selectedCategories.length }) }}
                </span>
              </div>
              <div class="settings-row-action settings-row-action--right sync-actions">
                <Button
                  size="sm"
                  :disabled="!authStore.isLoggedIn || syncStore.syncing"
                  @click="handleSyncNow"
                >
                  <UploadCloud :size="14" class="mr-1" />
                  {{ $t('sync.cloud-sync-push') }}
                </Button>
                <Button
                  size="sm"
                  variant="outline"
                  :disabled="!authStore.isLoggedIn || syncStore.syncing"
                  @click="handleRestore"
                >
                  <DownloadCloud :size="14" class="mr-1" />
                  {{ $t('sync.cloud-sync-pull') }}
                </Button>
              </div>
            </div>

            <p v-if="syncMessage" class="sync-message" :class="{ 'sync-message--err': syncError }">
              {{ syncMessage }}
            </p>
          </div>
        </div>

      </form>
    </main>

    <LoginModal
      v-model:open="showLoginModal"
      @success="onLoginSuccess"
    />
  </div>
</template>

<script lang="ts">
import {
	Activity,
	CheckCircle2,
	ChevronRight,
	Cloud,
	DownloadCloud,
	Loader2,
	RefreshCw,
	UploadCloud,
	User,
	XCircle,
	Zap,
} from "@lucide/vue";
import { syncCategories, syncCategoryIds } from "@shared/syncCategories";
import { defineComponent } from "vue";
import LoginModal from "@/components/Auth/LoginModal.vue";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { useAuthStore } from "@/store/auth";
import { usePreferenceStore } from "@/store/preference";
import { useSyncStore } from "@/store/sync";
import { sameSyncServer } from "@/utils/syncServer";

export default defineComponent({
	name: "preference-sync",
	components: {
		LoginModal,
		Button,
		Input,
		Cloud,
		RefreshCw,
		Zap,
		User,
		CheckCircle2,
		XCircle,
		UploadCloud,
		DownloadCloud,
		Loader2,
		Activity,
		ChevronRight,
	},
	data() {
		const preferenceStore = usePreferenceStore();
		const authStore = useAuthStore();
		const syncStore = useSyncStore();

		return {
			authStore,
			syncStore,
			syncCategories,
			showLoginModal: false,
			serverUrl: "",
			syncMessage: "",
			syncError: false,
			serverConfigChecked: false,
			form: {
				cloudSyncAuto: preferenceStore.config.cloudSyncAuto ?? false,
			},
		};
	},
	computed: {
		selectedCategories(): string[] {
			return this.syncStore.getSelectedCategories();
		},
		lastSyncText(): string {
			const lastAt = this.syncStore.lastSyncAt;
			if (!lastAt) {
				return this.$t("sync.cloud-sync-never");
			}
			return new Date(lastAt).toLocaleString();
		},
	},
	async created() {
		this.serverUrl = this.authStore.serverUrl;
		this.syncStore.lastSyncAt =
			(usePreferenceStore().config.cloudSyncLastAt as number) ?? null;
		if (this.serverUrl) {
			await this.checkServerConfig();
		}
	},
	methods: {
		async checkServerConfig() {
			await this.authStore.fetchServerConfig();
			this.serverConfigChecked = true;
		},
		toggleCategory(catId: string, enabled: boolean) {
			const current = [...this.selectedCategories];
			if (enabled && !current.includes(catId)) {
				current.push(catId);
			} else if (!enabled) {
				const idx = current.indexOf(catId);
				if (idx >= 0) {
					current.splice(idx, 1);
				}
			}
			usePreferenceStore().save({ cloudSyncCategories: current });
		},
		selectAllCategories() {
			usePreferenceStore().save({ cloudSyncCategories: [...syncCategoryIds] });
		},
		deselectAllCategories() {
			usePreferenceStore().save({ cloudSyncCategories: [] });
		},
		async saveServerUrl() {
			if (
				!sameSyncServer(this.serverUrl, this.authStore.serverUrl) &&
				this.authStore.isLoggedIn
			) {
				await this.authStore.logout();
			}
			await usePreferenceStore().save({ cloudSyncServerUrl: this.serverUrl });
			this.serverConfigChecked = false;
			if (this.serverUrl) {
				await this.checkServerConfig();
			}
		},
		async handleSyncNow() {
			this.syncMessage = "";
			this.syncError = false;
			try {
				await this.syncStore.syncBidirectional();
				this.syncMessage = this.$t("sync.cloud-sync-success");
			} catch {
				this.syncMessage = this.$t("sync.cloud-sync-fail");
				this.syncError = true;
			}
		},
		async handleRestore() {
			this.syncMessage = "";
			this.syncError = false;
			try {
				await this.syncStore.pullAll();
				this.syncMessage = this.$t("sync.cloud-sync-restore-success");
			} catch {
				this.syncMessage = this.$t("sync.cloud-sync-fail");
				this.syncError = true;
			}
		},
		async handleLogout() {
			await this.authStore.logout();
		},
		goHealth() {
			this.$router.push({ path: "/health" }).catch(() => undefined);
		},
		onLoginSuccess() {
			this.syncMessage = this.$t("sync.login-verify-success");
		},
	},
	watch: {
		"form.cloudSyncAuto"(val: boolean) {
			usePreferenceStore().save({ cloudSyncAuto: val });
		},
	},
});
</script>
