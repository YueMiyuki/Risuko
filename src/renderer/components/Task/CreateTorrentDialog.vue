<template>
  <Dialog :open="visible" @update:open="onOpenChange">
    <DialogContent
      :show-close-button="false"
      class="flex max-h-[85vh] w-[min(36rem,92vw)] flex-col gap-0 overflow-hidden p-0"
    >
      <DialogHeader class="flex shrink-0 flex-row items-center gap-2 border-b border-border/60 px-4 py-3">
        <div class="flex size-7 items-center justify-center rounded-md bg-emerald-500/15 text-emerald-600 dark:text-emerald-400">
          <FilePlus2 :size="14" />
        </div>
        <DialogTitle class="flex-1 text-sm font-medium">{{ $t('task.create-torrent') }}</DialogTitle>
        <button
          type="button"
          class="rounded-md p-1.5 text-muted-foreground transition-colors hover:bg-muted hover:text-foreground"
          :aria-label="$t('window.close')"
          :title="$t('window.close')"
          @click="close"
        >
          <X :size="14" />
        </button>
      </DialogHeader>

      <div v-if="result" class="flex min-h-0 flex-1 flex-col gap-3 overflow-y-auto px-4 py-4" aria-live="polite">
        <div class="flex items-center gap-2">
          <CircleCheck :size="18" class="shrink-0 text-emerald-600 dark:text-emerald-400" />
          <div class="min-w-0">
            <p class="truncate text-sm font-medium" :title="result.name">{{ result.name }}</p>
            <p class="text-[11px] text-muted-foreground">
              {{ $t('task.create-torrent-summary', { count: result.fileCount, size: formatBytes(result.totalLength) }) }}
            </p>
          </div>
        </div>
        <p v-if="result.gid" class="text-xs text-muted-foreground">{{ $t('task.create-torrent-seeding') }}</p>
        <p v-if="result.seedError" class="text-xs text-destructive">
          {{ $t('task.create-torrent-seed-failed', { error: result.seedError }) }}
        </p>
        <div>
          <div class="mb-1 text-[11px] text-muted-foreground">{{ $t('task.create-torrent-saved-to') }}</div>
          <div class="flex items-center gap-2 rounded-md border border-border/60 px-2 py-1.5">
            <span class="min-w-0 flex-1 truncate font-mono text-[11px]" :title="result.output">{{ result.output }}</span>
            <show-in-folder v-if="!isAndroid" :path="result.output" :size="14" />
          </div>
        </div>
        <div v-for="hash in infoHashes" :key="hash.label">
          <div class="mb-1 text-[11px] text-muted-foreground">{{ hash.label }}</div>
          <div class="truncate rounded-md border border-border/60 bg-muted/40 px-2 py-1.5 font-mono text-[11px]" :title="hash.value">
            {{ hash.value }}
          </div>
        </div>
        <div>
          <div class="mb-1 text-[11px] text-muted-foreground">{{ $t('task.create-torrent-magnet') }}</div>
          <div class="flex items-center gap-2">
            <div class="min-w-0 flex-1 truncate rounded-md border border-border/60 bg-muted/40 px-2 py-1.5 font-mono text-[11px]" :title="result.magnet">
              {{ result.magnet }}
            </div>
            <Button size="sm" variant="secondary" @click="copyMagnet">
              <Copy :size="14" />
              {{ $t('edit.copy') }}
            </Button>
          </div>
        </div>
      </div>

      <div v-else class="flex min-h-0 flex-1 flex-col gap-3 overflow-y-auto px-4 py-3">
        <div>
          <div class="mb-1 text-[11px] text-muted-foreground">{{ $t('task.create-torrent-source') }}</div>
          <div class="flex items-center gap-2">
            <div
              class="flex min-h-8 min-w-0 flex-1 items-center gap-2 rounded-md border border-border/60 px-2 py-1.5 text-xs"
              :class="{ 'text-muted-foreground': !form.path }"
            >
              <Folder v-if="form.path && sourceIsDir" :size="14" class="shrink-0" />
              <File v-else-if="form.path" :size="14" class="shrink-0" />
              <span class="min-w-0 flex-1 truncate" :title="form.path">{{ form.path || $t('task.create-torrent-no-source') }}</span>
            </div>
            <Button size="sm" variant="outline" :disabled="busy" @click="pick(false)">
              <FilePlus2 :size="14" />
              {{ $t('task.create-torrent-pick-file') }}
            </Button>
            <Button size="sm" variant="outline" :disabled="busy" @click="pick(true)">
              <FolderPlus :size="14" />
              {{ $t('task.create-torrent-pick-folder') }}
            </Button>
          </div>
        </div>

        <div>
          <label for="create-torrent-output" class="mb-1 block text-[11px] text-muted-foreground">{{ $t('task.create-torrent-save-to') }}</label>
          <div class="input-group input-group--bordered">
            <Input
              id="create-torrent-output"
              v-model="form.outputDir"
              readonly
              class="path-indicator-field flex-1 rounded-none border-none shadow-none noinput"
            />
            <span class="input-append">
              <select-directory :disabled="busy" @selected="(dir: string) => (form.outputDir = dir)" />
            </span>
          </div>
        </div>

        <div class="grid grid-cols-2 gap-3">
          <div>
            <div class="mb-1 text-[11px] text-muted-foreground">{{ $t('task.create-torrent-format') }}</div>
            <Select v-model="form.version" :disabled="busy">
              <SelectTrigger class="w-full text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="hybrid">{{ $t('task.create-torrent-format-hybrid') }}</SelectItem>
                <SelectItem value="v2">{{ $t('task.create-torrent-format-v2') }}</SelectItem>
                <SelectItem value="v1">{{ $t('task.create-torrent-format-v1') }}</SelectItem>
              </SelectContent>
            </Select>
          </div>
          <div>
            <div class="mb-1 text-[11px] text-muted-foreground">{{ $t('task.task-piece-length') }}</div>
            <Select v-model="form.pieceLength" :disabled="busy">
              <SelectTrigger class="w-full text-xs">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem v-for="size in pieceSizes" :key="size.value" :value="size.value">
                  {{ size.label }}
                </SelectItem>
              </SelectContent>
            </Select>
          </div>
        </div>
        <div
          v-if="paddingWarning"
          class="flex items-start gap-2 rounded-md border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-[11px] leading-relaxed text-amber-700 dark:text-amber-300"
        >
          <TriangleAlert :size="14" class="mt-0.5 shrink-0" aria-hidden="true" />
          <span class="min-w-0 flex-1">
            {{ $t('task.create-torrent-padding-warning', { padding: formatBytes(hybridPadding), size: formatBytes(sourceInfo?.totalLength ?? 0) }) }}
          </span>
          <Button size="sm" variant="outline" class="h-6 shrink-0 px-2 text-[11px]" :disabled="busy" @click="form.version = 'v2'">
            {{ $t('task.create-torrent-use-v2') }}
          </Button>
        </div>

        <div>
          <label for="create-torrent-trackers" class="mb-1 block text-[11px] text-muted-foreground">{{ $t('task.create-torrent-trackers') }}</label>
          <Textarea
            id="create-torrent-trackers"
            v-model="form.trackers"
            rows="3"
            class="resize-none font-mono text-xs"
            :disabled="busy"
            :placeholder="$t('task.create-torrent-trackers-hint')"
          />
        </div>

        <div class="flex flex-col gap-2">
          <label class="flex items-start justify-between gap-3 text-xs">
            <span>
              <span class="block">{{ $t('task.create-torrent-seed') }}</span>
              <span class="block text-[11px] text-muted-foreground">{{ $t('task.create-torrent-seed-hint') }}</span>
            </span>
            <ui-switch v-model="form.seed" :disabled="busy" />
          </label>
          <label class="flex items-start justify-between gap-3 text-xs">
            <span>
              <span class="block">{{ $t('task.create-torrent-private') }}</span>
              <span class="block text-[11px] text-muted-foreground">{{ $t('task.create-torrent-private-hint') }}</span>
            </span>
            <ui-switch v-model="form.private" :disabled="busy" />
          </label>
        </div>

        <button
          type="button"
          class="flex items-center gap-1 self-start text-[11px] text-muted-foreground hover:text-foreground"
          :aria-expanded="showMore"
          @click="showMore = !showMore"
        >
          <ChevronDown :size="12" class="transition-transform" :class="{ '-rotate-90': !showMore }" />
          {{ $t('task.create-torrent-more-options') }}
        </button>
        <div v-if="showMore" class="flex flex-col gap-3">
          <div>
            <label for="create-torrent-web-seeds" class="mb-1 block text-[11px] text-muted-foreground">{{ $t('task.create-torrent-web-seeds') }}</label>
            <Textarea
              id="create-torrent-web-seeds"
              v-model="form.webSeeds"
              rows="2"
              class="resize-none font-mono text-xs"
              :disabled="busy"
              :placeholder="$t('task.create-torrent-web-seeds-hint')"
            />
          </div>
          <div class="grid grid-cols-2 gap-3">
            <div>
              <label for="create-torrent-comment" class="mb-1 block text-[11px] text-muted-foreground">{{ $t('task.task-bittorrent-comment') }}</label>
              <Input id="create-torrent-comment" v-model="form.comment" autocomplete="off" :disabled="busy" />
            </div>
            <div>
              <label for="create-torrent-source-tag" class="mb-1 block text-[11px] text-muted-foreground">{{ $t('task.create-torrent-source-tag') }}</label>
              <Input
                id="create-torrent-source-tag"
                v-model="form.source"
                autocomplete="off"
                :disabled="busy"
                :placeholder="$t('task.create-torrent-source-tag-hint')"
              />
            </div>
          </div>
        </div>
      </div>

      <p v-if="addTaskWaiting" class="shrink-0 border-t border-border/60 px-4 py-2 text-[11px] text-muted-foreground" aria-live="polite">
        {{ $t('task.create-torrent-add-task-waiting') }}
      </p>
      <DialogFooter class="flex shrink-0 items-center justify-end gap-2 border-t border-border/60 px-4 py-3">
        <div v-if="busy" class="mr-auto flex min-w-0 flex-1 flex-col gap-1" aria-live="polite">
          <Progress :model-value="percent" class="h-1.5" />
          <p class="truncate text-[11px] text-muted-foreground">
            {{ $t('task.create-torrent-hashing', { percent, done: formatBytes(progress.hashed), total: formatBytes(progress.total) }) }}
          </p>
        </div>
        <template v-if="result">
          <Button variant="ghost" size="sm" @click="reset">{{ $t('task.create-torrent-another') }}</Button>
          <Button size="sm" @click="close">{{ $t('task.create-torrent-done') }}</Button>
        </template>
        <template v-else>
          <Button variant="ghost" size="sm" @click="busy ? cancel() : close()">{{ $t('app.cancel') }}</Button>
          <Button size="sm" :disabled="!canCreate" @click="create">
            <Loader2 v-if="busy" :size="14" class="animate-spin" />
            {{ $t('task.create-torrent-create') }}
          </Button>
        </template>
      </DialogFooter>
    </DialogContent>
  </Dialog>
</template>

<script lang="ts">
import {
	ChevronDown,
	CircleCheck,
	Copy,
	File,
	FilePlus2,
	Folder,
	FolderPlus,
	Loader2,
	TriangleAlert,
	X,
} from "@lucide/vue";
import { bytesToSize } from "@shared/utils";
import { Channel, invoke } from "@tauri-apps/api/core";
import { open } from "@tauri-apps/plugin-dialog";
import SelectDirectory from "@/components/Native/SelectDirectory.vue";
import ShowInFolder from "@/components/Native/ShowInFolder.vue";
import { Button } from "@/components/ui/button";
import UiSwitch from "@/components/ui/compat/UiSwitch.vue";
import {
	Dialog,
	DialogContent,
	DialogFooter,
	DialogHeader,
	DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Progress } from "@/components/ui/progress";
import {
	Select,
	SelectContent,
	SelectItem,
	SelectTrigger,
	SelectValue,
} from "@/components/ui/select";
import { Textarea } from "@/components/ui/textarea";
import is from "@/shims/platform";
import { useAppStore } from "@/store/app";
import { usePreferenceStore } from "@/store/preference";
import { useTaskStore } from "@/store/task";
import { copyText } from "@/utils/clipboard";
import { dirname, safUriToFilesystemPath } from "@/utils/native";

const CANCELLED = "task.create-torrent-cancelled";
const KIB = 1024;
const MIB = 1024 * KIB;

interface CreateTorrentResult {
	output: string;
	name: string;
	infoHash: string | null;
	infoHashV2: string | null;
	magnet: string;
	pieceLength: number;
	totalLength: number;
	fileCount: number;
	gid: string | null;
	seedError: string | null;
}

interface TorrentSourceInfo {
	totalLength: number;
	autoPieceLength: number;
	hybridPadding: Record<string, number>;
}

const emptyForm = () => ({
	path: "",
	outputDir: "",
	version: "hybrid",
	pieceLength: "auto",
	trackers: "",
	webSeeds: "",
	comment: "",
	source: "",
	private: false,
	seed: true,
});

const lines = (text: string) =>
	text
		.split(/\r?\n/)
		.map((line) => line.trim())
		.filter(Boolean);

const trackerTiers = (text: string) =>
	text
		.split(/\r?\n\s*\r?\n/)
		.map(lines)
		.filter((tier) => tier.length > 0);

export default {
	name: "create-torrent-dialog",
	components: {
		Button,
		ChevronDown,
		CircleCheck,
		Copy,
		Dialog,
		DialogContent,
		DialogFooter,
		DialogHeader,
		DialogTitle,
		File,
		FilePlus2,
		Folder,
		FolderPlus,
		Input,
		Loader2,
		Progress,
		Select,
		SelectContent,
		SelectDirectory,
		SelectItem,
		SelectTrigger,
		SelectValue,
		ShowInFolder,
		Textarea,
		TriangleAlert,
		UiSwitch,
		X,
	},
	props: {
		visible: { type: Boolean, default: false },
	},
	data() {
		return {
			form: emptyForm(),
			sourceIsDir: false,
			showMore: false,
			busy: false,
			jobId: "",
			progress: { hashed: 0, total: 0 },
			result: null as CreateTorrentResult | null,
			sourceInfo: null as TorrentSourceInfo | null,
			inspectToken: 0,
		};
	},
	computed: {
		addTaskWaiting() {
			return useAppStore().pendingAddTaskType !== "";
		},
		isAndroid(): boolean {
			return is.android();
		},
		canCreate(): boolean {
			return !this.busy && !!this.form.path && !!this.form.outputDir;
		},
		pieceLengthBytes(): number | null {
			return this.form.pieceLength === "auto"
				? null
				: Number(this.form.pieceLength);
		},
		hybridPadding(): number {
			const info = this.sourceInfo;
			if (!info) {
				return 0;
			}
			const pieceLength = this.pieceLengthBytes ?? info.autoPieceLength;
			return info.hybridPadding[String(pieceLength)] ?? 0;
		},
		paddingWarning(): boolean {
			return (
				this.form.version === "hybrid" &&
				!!this.sourceInfo &&
				this.hybridPadding > this.sourceInfo.totalLength
			);
		},
		percent(): number {
			const { hashed, total } = this.progress;
			return total > 0 ? Math.floor((hashed / total) * 100) : 0;
		},
		pieceSizes(): { value: string; label: string }[] {
			const sizes = [16, 32, 64, 128, 256, 512].map((n) => n * KIB);
			sizes.push(...[1, 2, 4, 8, 16, 32].map((n) => n * MIB));
			return [
				{ value: "auto", label: this.$t("task.create-torrent-piece-auto") },
				...sizes.map((size) => ({
					value: String(size),
					label: bytesToSize(size),
				})),
			];
		},
		infoHashes(): { label: string; value: string }[] {
			const out = [];
			if (this.result?.infoHash) {
				out.push({
					label: this.$t("task.create-torrent-info-hash-v1"),
					value: this.result.infoHash,
				});
			}
			if (this.result?.infoHashV2) {
				out.push({
					label: this.$t("task.create-torrent-info-hash-v2"),
					value: this.result.infoHashV2,
				});
			}
			return out;
		},
	},
	watch: {
		visible(open: boolean) {
			if (!open && this.busy) {
				this.cancel();
			}
			if (open && !this.busy) {
				this.reset();
			}
		},
		"form.path"() {
			this.inspectSource();
		},
	},
	methods: {
		formatBytes: bytesToSize,
		reset() {
			this.form = emptyForm();
			this.sourceIsDir = false;
			this.showMore = false;
			this.result = null;
			this.progress = { hashed: 0, total: 0 };
		},
		onOpenChange(open: boolean) {
			if (!open && !this.busy) {
				this.close();
			}
		},
		close() {
			useAppStore().hideCreateTorrentDialog();
		},
		cancel() {
			if (this.jobId) {
				invoke("cancel_create_torrent", { id: this.jobId }).catch(
					() => undefined,
				);
			}
		},
		async ensureStorageAccess(): Promise<boolean> {
			if (!this.isAndroid) {
				return true;
			}
			const granted = await invoke<boolean>("ensure_android_storage_access");
			if (!granted) {
				this.$msg.warning(this.$t("app.android-storage-access-required"));
			}
			return granted;
		},
		setSource(path: string, isDir: boolean) {
			if (path.startsWith("content://")) {
				this.$msg.error(this.$t("task.create-torrent-device-storage-only"));
				return;
			}
			this.form.path = path;
			this.sourceIsDir = isDir;
			if (!this.form.outputDir) {
				this.form.outputDir = dirname(path);
			}
		},
		async pick(directory: boolean) {
			try {
				if (!(await this.ensureStorageAccess())) {
					return;
				}
				const picked =
					directory && this.isAndroid
						? await invoke<string | null>("select_android_directory")
						: await open({ multiple: false, directory });
				if (picked) {
					this.setSource(safUriToFilesystemPath(String(picked)), directory);
				}
			} catch (err) {
				this.$msg.error(`${err}`);
			}
		},
		async inspectSource() {
			const token = ++this.inspectToken;
			this.sourceInfo = null;
			if (!this.form.path) {
				return;
			}
			const info = await invoke<TorrentSourceInfo>("inspect_torrent_source", {
				path: this.form.path,
			}).catch(() => null);
			if (token === this.inspectToken) {
				this.sourceInfo = info;
			}
		},
		async create() {
			if (!this.canCreate) {
				return;
			}
			this.busy = true;
			this.jobId = `create-${Date.now()}-${Math.random().toString(36).slice(2)}`;
			this.progress = { hashed: 0, total: 0 };
			const onProgress = new Channel<{ hashed: number; total: number }>();
			onProgress.onmessage = (message) => {
				this.progress = message;
			};
			try {
				this.result = await invoke<CreateTorrentResult>("create_torrent", {
					id: this.jobId,
					request: {
						path: this.form.path,
						outputDir: this.form.outputDir,
						version: this.form.version,
						pieceLength: this.pieceLengthBytes,
						trackers: trackerTiers(this.form.trackers),
						webSeeds: lines(this.form.webSeeds),
						comment: this.form.comment,
						source: this.form.source,
						private: this.form.private,
						seed: this.form.seed,
					},
					onProgress,
				});
				usePreferenceStore().recordHistoryDirectory(this.form.outputDir);
				if (this.result.gid) {
					useTaskStore().fetchList();
				}
			} catch (err) {
				const message = `${err}`;
				if (message === CANCELLED) {
					this.$msg.info(this.$t(CANCELLED));
				} else {
					this.$msg.error(message);
				}
			} finally {
				this.busy = false;
				this.jobId = "";
			}
		},
		async copyMagnet() {
			if (!this.result) {
				return;
			}
			try {
				await copyText(this.result.magnet);
				this.$msg.success(this.$t("task.create-torrent-copied"));
			} catch (err) {
				this.$msg.error(`${err}`);
			}
		},
	},
};
</script>
