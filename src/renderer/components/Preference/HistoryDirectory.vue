<template>
  <div class="history-directory">
    <Popover>
      <PopoverTrigger as-child>
        <ui-button
          size="sm"
          variant="ghost"
          class="history-button"
          :title="$t('preferences.history-directories')"
          :aria-label="$t('preferences.history-directories')"
        >
          <History :size="14" />
        </ui-button>
      </PopoverTrigger>
      <PopoverContent
        style="width: 360px"
        class="directory-popper"
        side="bottom"
        align="start"
      >
        <div class="directory-empty" v-if="empty">
          {{ $t('task.no-task') }}
        </div>
        <ul class="directory-list" v-if="favoriteDirectories.length > 0">
          <li v-for="directory in favoriteDirectories" :key="directory">
            <button
              type="button"
              class="directory-path"
              :title="directory"
              :aria-label="directory"
              @click.stop="() => handleSelectItem(directory)"
            >
              <span class="directory-path-label">{{ directory }}</span>
            </button>
            <span class="directory-actions">
              <button
                type="button"
                class="history-icon icon-history-favorited"
                :title="$t('preferences.unfavorite-directory')"
                :aria-label="$t('preferences.unfavorite-directory')"
                @click.stop="() => handleCancelFavoriteItem(directory)"
              >
                <Star :size="18" aria-hidden="true" />
              </button>
              <button
                type="button"
                class="history-icon icon-history-remove"
                :title="$t('preferences.remove-history-directory')"
                :aria-label="$t('preferences.remove-history-directory')"
                @click.stop="() => handleRemoveItem(directory)"
              >
                <Trash2 :size="18" aria-hidden="true" />
              </button>
            </span>
          </li>
        </ul>
        <div class="directory-divider" v-if="showDivider" />
        <ul class="directory-list" v-if="historyDirectories.length > 0">
          <li v-for="directory in historyDirectories" :key="directory">
            <button
              type="button"
              class="directory-path"
              :title="directory"
              :aria-label="directory"
              @click.stop="() => handleSelectItem(directory)"
            >
              <span class="directory-path-label">{{ directory }}</span>
            </button>
            <span class="directory-actions">
              <button
                type="button"
                v-if="showFavoriteAction"
                class="history-icon icon-history-favorite"
                :title="$t('preferences.favorite-directory')"
                :aria-label="$t('preferences.favorite-directory')"
                @click.stop="() => handleFavoriteItem(directory)"
              >
                <StarOff :size="18" aria-hidden="true" />
              </button>
              <button
                type="button"
                class="history-icon icon-history-remove"
                :title="$t('preferences.remove-history-directory')"
                :aria-label="$t('preferences.remove-history-directory')"
                @click.stop="() => handleRemoveItem(directory)"
              >
                <Trash2 :size="18" aria-hidden="true" />
              </button>
            </span>
          </li>
        </ul>
      </PopoverContent>
    </Popover>
  </div>
</template>

<script lang="ts">
import { History, Star, StarOff, Trash2 } from "@lucide/vue";
import { MAX_NUM_OF_DIRECTORIES } from "@shared/constants";
import UiButton from "@/components/ui/compat/UiButton.vue";
import {
	Popover,
	PopoverContent,
	PopoverTrigger,
} from "@/components/ui/popover";
import { usePreferenceStore } from "@/store/preference";

export default {
	name: "history-directory",
	components: {
		[UiButton.name]: UiButton,
		Popover,
		PopoverContent,
		PopoverTrigger,
		History,
		Star,
		StarOff,
		Trash2,
	},
	computed: {
		historyDirectories() {
			return [
				...(usePreferenceStore().config.historyDirectories || []),
			].reverse();
		},
		favoriteDirectories() {
			return [
				...(usePreferenceStore().config.favoriteDirectories || []),
			].reverse();
		},
		empty() {
			const { favoriteDirectories, historyDirectories } = this;
			return favoriteDirectories.length + historyDirectories.length === 0;
		},
		showDivider() {
			const { favoriteDirectories, historyDirectories } = this;
			return favoriteDirectories.length > 0 && historyDirectories.length > 0;
		},
		showFavoriteAction() {
			const { favoriteDirectories } = this;
			return favoriteDirectories.length < MAX_NUM_OF_DIRECTORIES;
		},
	},
	methods: {
		handleSelectItem(directory) {
			this.$emit("selected", directory.trim());
		},
		handleFavoriteItem(directory) {
			usePreferenceStore().favoriteDirectory(directory);
		},
		handleCancelFavoriteItem(directory) {
			usePreferenceStore().cancelFavoriteDirectory(directory);
		},
		handleRemoveItem(directory) {
			usePreferenceStore().removeDirectory(directory);
		},
	},
};
</script>
