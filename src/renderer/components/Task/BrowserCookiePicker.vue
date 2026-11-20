<template>
  <Popover v-model:open="open">
    <PopoverTrigger as-child>
      <UiButton
        type="button"
        size="sm"
        variant="ghost"
        class="h-7 px-2 text-[11px]"
        :disabled="disabled"
      >
        <Globe :size="12" class="mr-1" />
        {{ $t('task.task-cookie-import') }}
      </UiButton>
    </PopoverTrigger>
    <PopoverContent class="w-72 p-1" side="bottom" align="start">
      <div v-if="loading" class="px-3 py-2 text-xs text-muted-foreground">
        {{ $t('app.loading') }}
      </div>
      <div
        v-else-if="browsers.length === 0"
        class="px-3 py-2 text-xs text-muted-foreground"
      >
        {{ $t('task.task-cookie-import-no-browsers') }}
      </div>
      <ul v-else class="flex flex-col gap-0.5">
        <li v-for="b in browsers" :key="b.id">
          <button
            type="button"
            class="flex w-full items-center justify-between gap-2 rounded-md px-2 py-1.5 text-left text-xs transition-colors hover:bg-muted disabled:opacity-50"
            :disabled="!b.available || importing !== ''"
            @click="handlePick(b)"
          >
            <span class="truncate">{{ b.name }}</span>
            <span
              v-if="!b.available"
              class="text-[10px] text-muted-foreground"
            >
              n/a
            </span>
            <span
              v-else-if="importing === b.id"
              class="text-[10px] text-muted-foreground"
            >
              ...
            </span>
          </button>
        </li>
      </ul>
    </PopoverContent>
  </Popover>
</template>

<script lang="ts">
import { Globe } from "@lucide/vue";
import logger from "@shared/utils/logger";
import { toast } from "vue-sonner";
import api, { type BrowserInfo, type ImportedCookies } from "@/api";
import UiButton from "@/components/ui/compat/UiButton.vue";
import {
	Popover,
	PopoverContent,
	PopoverTrigger,
} from "@/components/ui/popover";
import { cookieImportErrorKey } from "@/utils/cookieImport";

export default {
	name: "browser-cookie-picker",
	components: {
		Popover,
		PopoverContent,
		PopoverTrigger,
		UiButton,
		Globe,
	},
	props: {
		url: {
			type: String,
			required: true,
		},
		disabled: {
			type: Boolean,
			default: false,
		},
		persist: {
			type: Boolean,
			default: true,
		},
	},
	emits: ["imported"],
	data() {
		return {
			open: false,
			loading: false,
			browsers: [] as BrowserInfo[],
			importing: "" as string,
		};
	},
	watch: {
		open(value: boolean) {
			if (value && this.browsers.length === 0) {
				this.refresh();
			}
		},
	},
	methods: {
		async refresh() {
			this.loading = true;
			try {
				this.browsers = await api.listBrowsers();
			} finally {
				this.loading = false;
			}
		},
		async handlePick(b: BrowserInfo) {
			const url = (this.url || "").trim();
			if (!url) {
				toast.error(this.$t("task.new-task-uris-required"));
				return;
			}
			this.importing = b.id;
			try {
				logger.log(
					`[Risuko] cookie picker: starting import for browser=${b.id}`,
				);
				const result: ImportedCookies = await api.importBrowserCookies({
					browser: b.id,
					url,
					persist: this.persist,
				});
				logger.log(
					`[Risuko] cookie picker: imported ${result.count} cookie(s) from ${b.id} for host=${result.host}`,
					{
						hasCfClearance: result.hasCfClearance,
					},
				);
				toast.success(
					this.$t("task.task-cookie-import-success", {
						count: result.count,
						browser: b.name,
					}),
					{
						description: result.cookieNames.join(", "),
					},
				);
				this.$emit("imported", { browser: b, result });
				this.open = false;
			} catch (e) {
				logger.error(`[Risuko] cookie picker: import failed: ${String(e)}`);
				toast.error(this.$t(cookieImportErrorKey(e), { error: String(e) }));
			} finally {
				this.importing = "";
			}
		},
	},
};
</script>
