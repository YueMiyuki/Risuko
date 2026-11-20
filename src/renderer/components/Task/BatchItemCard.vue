<template>
  <div
    class="batch-item-card group relative overflow-hidden rounded-md border border-border bg-card/40"
    role="listitem"
  >
    <AccordionItem :value="item.id" :unmount-on-hide="false" class="border-none">
      <div class="flex items-center gap-2 px-3">
        <AccordionTrigger
          class="flex-1 py-3 hover:no-underline"
        >
          <div class="flex items-center gap-2 min-w-0 flex-1">
            <component :is="kindIcon" :size="14" class="shrink-0 text-muted-foreground" />
            <span class="min-w-0 flex-1 truncate text-sm" :title="item.label">{{ item.label }}</span>
            <span
              v-if="statusChip"
              class="ml-auto shrink-0 rounded-full px-2 py-0.5 text-[10px] font-medium"
              :class="statusChipClass"
            >
              {{ statusChip }}
            </span>
          </div>
        </AccordionTrigger>
        <button
          type="button"
          class="shrink-0 rounded-md p-1.5 text-muted-foreground transition-colors hover:bg-muted hover:text-foreground disabled:opacity-50"
          :aria-label="$t('task.batch-remove-item')"
          :title="$t('task.batch-remove-item')"
          :disabled="disabled"
          @click.stop="$emit('remove', item.id)"
        >
          <X :size="14" />
        </button>
      </div>
      <AccordionContent class="px-3 pb-3 pt-0">
        <div v-if="item.kind === 'torrent' && item.path">
          <select-torrent
            :torrent-path="item.path"
            :torrent-name="item.displayName || ''"
            @change="onTorrentChange"
          />
        </div>
        <div v-else-if="item.kind === 'magnet' && item.uri">
          <magnet-files
            :magnet-uri="item.uri"
            @change="onMagnetSelectionChange"
          />
        </div>
        <div v-else-if="showMediaPicker" class="flex flex-col gap-3">
          <div
            v-if="!item.isMedia"
            class="flex items-center justify-between rounded-md border border-border/60 px-3 py-2"
          >
            <div class="min-w-0">
              <div class="text-xs font-medium">{{ $t('task.media-force-ytdlp') }}</div>
              <div class="mt-0.5 text-[11px] text-muted-foreground">
                {{ $t('task.media-force-ytdlp-tips') }}
              </div>
            </div>
            <Switch
              :model-value="!!item.forceYtdlp"
              :disabled="disabled"
              @update:model-value="onToggleForce"
            />
          </div>

          <div class="rounded-md border border-border/60 px-3 py-2.5">
            <div class="flex items-center gap-2">
              <Video :size="13" class="shrink-0 text-muted-foreground" />
              <span class="text-xs font-medium">{{ $t('task.media-format-picker') }}</span>
              <Button
                v-if="item.mediaInfoState !== 'loading'"
                variant="outline"
                size="sm"
                class="ml-auto h-6 px-2 text-[11px]"
                :disabled="disabled || !canFetchFormats"
                @click.stop="fetchFormats"
              >
                {{ item.mediaInfoState === 'ready' ? $t('task.media-format-refresh') : $t('task.media-format-fetch') }}
              </Button>
              <span v-else class="ml-auto text-[11px] text-muted-foreground">
                {{ $t('task.media-format-loading') }}
              </span>
            </div>

            <div v-if="item.mediaInfoError" class="mt-2 text-[11px] text-destructive">
              {{ item.mediaInfoError }}
            </div>

            <div v-if="item.mediaTitle" class="mt-2 truncate text-[11px] text-muted-foreground" :title="item.mediaTitle">
              {{ item.mediaTitle }}
            </div>

            <div v-if="hasFormats" class="mt-2">
              <Select
                :model-value="item.mediaFormatId === '' ? '__auto__' : item.mediaFormatId"
                @update:model-value="onSelectFormat"
              >
                <SelectTrigger class="h-8 w-full text-xs">
                  <SelectValue :placeholder="$t('task.media-format-auto')" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="__auto__">{{ $t('task.media-format-auto') }}</SelectItem>
                  <SelectItem
                    v-for="opt in formatOptions"
                    :key="opt.value"
                    :value="opt.value"
                  >
                    {{ opt.label }}
                  </SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div v-else class="mt-2 text-[11px] text-muted-foreground">
              {{ $t('task.media-format-hint') }}
            </div>
          </div>
        </div>
        <div v-else-if="item.kind === 'metalink' || item.kind === 'nzb'" class="rounded-md border border-dashed border-border/60 px-3 py-4 text-xs text-muted-foreground">
          <div class="break-all font-mono">{{ item.path }}</div>
        </div>
        <div v-else class="rounded-md border border-dashed border-border/60 px-3 py-4 text-xs text-muted-foreground">
          <div class="break-all font-mono">{{ item.uri }}</div>
          <div class="mt-2">{{ $t('task.batch-uri-body') }}</div>
          <div class="mt-3 rounded-md border border-border/60 px-3 py-2.5">
            <div class="text-[11px] text-muted-foreground">{{ $t('task.mirror-hint') }}</div>
            <ul v-if="mirrors.length" class="mt-2 flex flex-col gap-1.5">
              <li
                v-for="(mirror, idx) in mirrors"
                :key="idx"
                class="flex items-center gap-2"
              >
                <span class="min-w-0 flex-1 truncate font-mono text-[11px] text-foreground" :title="mirror">{{ mirror }}</span>
                <button
                  type="button"
                  class="shrink-0 rounded-md p-1 text-muted-foreground transition-colors hover:bg-muted hover:text-foreground disabled:opacity-50"
                  :aria-label="$t('task.mirror-remove')"
                  :title="$t('task.mirror-remove')"
                  :disabled="disabled"
                  @click.stop="removeMirror(idx)"
                >
                  <X :size="12" />
                </button>
              </li>
            </ul>
            <div class="mt-2 flex items-center gap-2">
              <Input
                v-model="mirrorDraft"
                :placeholder="$t('task.mirror-placeholder')"
                :disabled="disabled"
                class="h-8 flex-1 text-xs"
                @keydown.enter.prevent="addMirror"
              />
              <Button
                variant="outline"
                size="sm"
                class="h-8 shrink-0 px-2 text-[11px]"
                :disabled="disabled || !canAddMirror"
                @click.stop="addMirror"
              >
                {{ $t('task.mirror-add') }}
              </Button>
            </div>
          </div>
          <div
            v-if="canOfferYtdlp"
            class="mt-3 flex items-center justify-between rounded-md border border-border/60 px-3 py-2"
          >
            <div class="min-w-0">
              <div class="font-medium text-foreground">{{ $t('task.media-force-ytdlp') }}</div>
              <div class="mt-0.5 text-[11px]">{{ $t('task.media-force-ytdlp-tips') }}</div>
            </div>
            <Switch
              :model-value="!!item.forceYtdlp"
              :disabled="disabled"
              @update:model-value="onToggleForce"
            />
          </div>
        </div>
        <div v-if="item.error" class="mt-2 flex flex-wrap items-center gap-2 text-xs text-destructive">
          <span>{{ item.error }}</span>
        </div>
      </AccordionContent>
    </AccordionItem>
  </div>
</template>

<script lang="ts">
import { FileArchive, Link2, Magnet, Video, X } from "@lucide/vue";
import type { MediaFormat } from "@shared/types/task";
import { bytesToSize } from "@shared/utils";
import api from "@/api";
import MagnetFiles from "@/components/Task/MagnetFiles.vue";
import SelectTorrent from "@/components/Task/SelectTorrent.vue";
import {
	AccordionContent,
	AccordionItem,
	AccordionTrigger,
} from "@/components/ui/accordion";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
	Select,
	SelectContent,
	SelectItem,
	SelectTrigger,
	SelectValue,
} from "@/components/ui/select";
import { Switch } from "@/components/ui/switch";
import type { BatchQueueItem } from "@/store/batchQueue";

export default {
	name: "batch-item-card",
	components: {
		[SelectTorrent.name]: SelectTorrent,
		[MagnetFiles.name]: MagnetFiles,
		AccordionItem,
		AccordionTrigger,
		AccordionContent,
		Button,
		Input,
		Select,
		SelectContent,
		SelectItem,
		SelectTrigger,
		SelectValue,
		Switch,
		Video,
		X,
	},
	props: {
		item: {
			type: Object as () => BatchQueueItem,
			required: true,
		},
		disabled: {
			type: Boolean,
			default: false,
		},
	},
	emits: ["remove", "update:selectFile", "update:media", "update:mirrors"],
	data() {
		return {
			mirrorDraft: "",
		};
	},
	computed: {
		kindIcon() {
			if (
				this.item.kind === "torrent" ||
				this.item.kind === "metalink" ||
				this.item.kind === "nzb"
			) {
				return FileArchive;
			}
			if (this.item.kind === "magnet") {
				return Magnet;
			}
			if (this.item.isMedia || this.item.forceYtdlp) {
				return Video;
			}
			return Link2;
		},
		mirrors(): string[] {
			return this.item.mirrors ?? [];
		},
		canAddMirror(): boolean {
			const candidate = this.mirrorDraft.trim();
			let parsed: URL;
			try {
				parsed = new URL(candidate);
			} catch {
				return false;
			}
			if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
				return false;
			}
			if (!parsed.hostname) {
				return false;
			}
			const lower = candidate.toLowerCase();
			if (this.item.uri && this.item.uri.trim().toLowerCase() === lower) {
				return false;
			}
			return !this.mirrors.some((m) => m.trim().toLowerCase() === lower);
		},
		showMediaPicker(): boolean {
			return (
				this.item.kind === "uri" &&
				!!this.item.uri &&
				!!(this.item.isMedia || this.item.forceYtdlp)
			);
		},
		canOfferYtdlp(): boolean {
			if (this.item.kind !== "uri" || !this.item.uri || this.item.isMedia) {
				return false;
			}
			return /^https?:\/\//i.test(this.item.uri.trim());
		},
		canFetchFormats(): boolean {
			return !!(this.item.isMedia || this.item.forceYtdlp);
		},
		hasFormats(): boolean {
			return (this.item.mediaFormats?.length ?? 0) > 0;
		},
		formatOptions(): Array<{ value: string; label: string }> {
			const formats = this.item.mediaFormats ?? [];
			return formats
				.filter((f) => !f.audio_only || formats.every((x) => x.audio_only))
				.map((f) => ({
					value: f.format_id,
					label: this.describeFormat(f),
				}));
		},
		statusChip(): string {
			switch (this.item.status) {
				case "submitting":
					return this.$t("task.loading-add-task");
				case "success":
					return "OK";
				case "failed":
					return "!";
				default:
					return "";
			}
		},
		statusChipClass() {
			switch (this.item.status) {
				case "success":
					return "bg-green-500/15 text-green-600 dark:text-green-400";
				case "failed":
					return "bg-destructive/15 text-destructive";
				case "submitting":
					return "bg-muted text-muted-foreground";
				default:
					return "bg-muted/60 text-muted-foreground";
			}
		},
	},
	methods: {
		onTorrentChange(_path: string, selectFile: string) {
			this.$emit("update:selectFile", this.item.id, selectFile);
		},
		onMagnetSelectionChange(selectFile: string) {
			this.$emit("update:selectFile", this.item.id, selectFile);
		},
		addMirror() {
			if (!this.canAddMirror) {
				return;
			}
			const next = [...this.mirrors, this.mirrorDraft.trim()];
			this.mirrorDraft = "";
			this.$emit("update:mirrors", this.item.id, next);
		},
		removeMirror(idx: number) {
			const next = this.mirrors.filter((_, i) => i !== idx);
			this.$emit("update:mirrors", this.item.id, next);
		},
		onToggleForce(value: boolean) {
			this.$emit("update:media", this.item.id, {
				forceYtdlp: value,
				...(value
					? {}
					: {
							mediaFormatId: "",
							mediaFormats: [],
							mediaInfoState: "idle",
							mediaInfoError: "",
							mediaTitle: "",
						}),
			});
		},
		onSelectFormat(value: string) {
			const formatId = value === "__auto__" ? "" : value;
			this.$emit("update:media", this.item.id, {
				mediaFormatId: formatId,
			});
		},
		describeFormat(f: MediaFormat): string {
			const parts: string[] = [];
			if (f.audio_only) {
				parts.push("Audio");
				if (f.abr) {
					parts.push(`${Math.round(f.abr)}kbps`);
				}
			} else {
				parts.push(f.resolution || "video");
				if (f.fps) {
					parts.push(`${Math.round(f.fps)}fps`);
				}
				if (f.video_only) {
					parts.push("video-only");
				}
			}
			if (f.ext) {
				parts.push(f.ext);
			}
			const size = f.filesize ?? f.filesize_approx;
			if (size) {
				parts.push(bytesToSize(size));
			}
			return parts.join(" · ");
		},
		async fetchFormats() {
			if (!this.item.uri || !this.canFetchFormats) {
				return;
			}
			this.$emit("update:media", this.item.id, {
				mediaInfoState: "loading",
				mediaInfoError: "",
			});
			try {
				const info = await api.getMediaInfo({
					url: this.item.uri,
					options: this.item.forceYtdlp ? { forceYtdlp: true } : {},
				});
				this.$emit("update:media", this.item.id, {
					mediaInfoState: "ready",
					mediaFormats: info.formats || [],
					mediaTitle: info.title || "",
				});
			} catch (err: unknown) {
				const message =
					typeof err === "string"
						? err
						: (err as { message?: string })?.message ||
							this.$t("task.media-format-error");
				this.$emit("update:media", this.item.id, {
					mediaInfoState: "error",
					mediaInfoError: message,
				});
			}
		},
	},
};
</script>

<style scoped>
.batch-item-card {
  transition: border-color 0.15s ease;
}
.batch-item-card:hover {
  border-color: hsl(var(--border));
}
</style>
