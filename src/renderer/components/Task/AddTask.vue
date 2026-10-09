<template>
  <Dialog :open="visible" @update:open="handleDialogOpenChange">
    <DialogContent
      :show-close-button="false"
      class="add-task-dialog flex max-h-[80vh] w-170 flex-col gap-0 overflow-hidden p-0"
    >
      <DialogHeader class="flex shrink-0 flex-row items-center gap-2 border-b border-border/60 px-4 py-3">
        <div class="flex size-7 items-center justify-center rounded-md bg-muted text-muted-foreground">
          <Download :size="14" />
        </div>
        <DialogTitle class="flex-1 text-sm font-medium">{{ $t('task.batch-add-title') }}</DialogTitle>
        <button
          type="button"
          class="rounded-md p-1.5 text-muted-foreground transition-colors hover:bg-muted hover:text-foreground disabled:opacity-50"
          :aria-label="$t('window.close')"
          :title="$t('window.close')"
          :disabled="submitting"
          @click="handleClose"
        >
          <X :size="14" />
        </button>
      </DialogHeader>

      <div class="atd-scroll min-h-0 flex-1 overflow-y-auto">
        <div class="flex flex-col gap-2 px-4 pt-3">
          <Textarea
            ref="uri"
            auto-complete="off"
            rows="3"
            :placeholder="$t('task.uri-task-tips')"
            v-model="uriDraft"
            class="resize-none text-xs"
            :disabled="submitting || uriPreparing"
            @paste="handleUriPaste"
            @keydown.enter.exact="handleUriEnter"
            @keydown.enter.meta.exact="handleUriSubmitShortcut"
            @keydown.enter.ctrl.exact="handleUriSubmitShortcut"
          />
          <div
            class="flex items-center justify-center gap-2 rounded-md border border-dashed border-border/60 px-3 py-2.5 text-xs text-muted-foreground transition-colors"
            :class="{ 'border-primary/60 bg-primary/5': dragOver }"
            @dragover.prevent
            @dragenter.prevent="dragOver = true"
            @dragleave.prevent="dragOver = false"
            @drop.prevent="handleDrop"
          >
            <FileArchive :size="13" />
            <span>{{ $t('task.batch-drop-hint') }}</span>
            <button
              v-if="isRenderer"
              type="button"
              class="ml-1 rounded-md border border-border/60 px-2 py-0.5 text-[11px] hover:bg-muted"
              :title="$t('app.browse')"
              :aria-label="$t('app.browse')"
              :disabled="submitting || uriPreparing"
              @click="browseTorrentFiles"
            >
              {{ $t('app.browse') }}
            </button>
          </div>
          <div
            v-if="showTorrentNetworkInterferenceWarning"
            role="note"
            class="flex items-start gap-2 rounded-md border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-[11px] leading-relaxed text-amber-700 dark:text-amber-300"
          >
            <TriangleAlert :size="14" class="mt-0.5 shrink-0" aria-hidden="true" />
            <span>{{ $t('task.torrent-network-interference-warning') }}</span>
          </div>
        </div>

        <div class="mt-3 mb-12 px-4">
          <div
            v-if="queue.length === 0"
            class="flex min-h-30 items-center justify-center rounded-md border border-dashed border-border/40 text-xs text-muted-foreground"
          >
            {{ $t('task.batch-queue-empty') }}
          </div>
          <Accordion
            v-else
            type="multiple"
            v-model="openItemIds"
            class="flex flex-col gap-2"
          >
            <AnimatePresence>
              <Motion
                v-for="item in queue"
                :key="item.id"
                tag="div"
                layout
                :initial="motionInitial"
                :animate="motionAnimate"
                :exit="motionExit"
                :transition="motionTransition"
              >
                <batch-item-card
                  :item="item"
                  :disabled="submitting || uriPreparing"
                  @remove="removeItem"
                  @update:select-file="updateSelection"
                  @update:media="updateMedia"
                  @update:mirrors="updateMirrors"
                />
              </Motion>
            </AnimatePresence>
          </Accordion>
        </div>

        <div class="border-t border-border/60 px-4 py-3">
          <div class="grid grid-cols-[1fr_88px] gap-2">
            <div class="col-span-2">
              <label class="mb-1 block text-[11px] text-muted-foreground">{{ $t('task.task-out') }}</label>
              <Input
                v-model="form.out"
                auto-complete="off"
                :placeholder="$t('task.task-out-tips')"
                :disabled="submitting"
              />
            </div>
            <div>
              <label class="mb-1 block text-[11px] text-muted-foreground">{{ $t('task.task-dir') }}</label>
              <div class="input-group input-group--bordered">
                <span class="input-prepend">
                  <history-directory @selected="handleHistoryDirectorySelected" />
                </span>
                <Input
                  v-model="form.dir"
                  readonly
                  class="path-indicator-field flex-1 shadow-none rounded-none border-none noinput"
                />
                <span class="input-append" v-if="isRenderer">
                  <select-directory @selected="handleNativeDirectorySelected" />
                </span>
              </div>
            </div>
            <div>
              <label class="mb-1 block text-[11px] text-muted-foreground">{{ $t('task.task-split') }}</label>
              <NumberInput v-model="form.split" :min="1" :max="128" />
            </div>
          </div>
        </div>

        <AnimatePresence>
          <Motion
            v-if="showAdvanced"
            key="advanced"
            tag="div"
            class="overflow-hidden border-t border-border/60"
            :initial="{ height: 0, opacity: 0 }"
            :animate="{ height: 'auto', opacity: 1 }"
            :exit="{ height: 0, opacity: 0 }"
            :transition="{ duration: reducedMotion ? 0 : 0.22, ease: [0.22, 0.61, 0.36, 1] }"
          >
            <div class="grid grid-cols-2 gap-x-3 gap-y-3 px-4 py-3">
            <div class="col-span-2">
              <label class="mb-1 block text-[11px] text-muted-foreground">{{ $t('task.task-user-agent') }}</label>
              <Textarea
                auto-complete="off"
                rows="2"
                :placeholder="$t('task.task-user-agent')"
                v-model="form.userAgent"
                class="resize-none text-xs"
              />
            </div>
            <div class="col-span-2">
              <label class="mb-1 block text-[11px] text-muted-foreground">{{ $t('task.task-authorization') }}</label>
              <Textarea
                auto-complete="off"
                rows="2"
                :placeholder="$t('task.task-authorization')"
                v-model="form.authorization"
                class="resize-none text-xs"
              />
            </div>
            <div class="col-span-2">
              <label class="mb-1 block text-[11px] text-muted-foreground">{{ $t('task.task-referer') }}</label>
              <Textarea
                auto-complete="off"
                rows="2"
                :placeholder="$t('task.task-referer')"
                v-model="form.referer"
                class="resize-none text-xs"
              />
            </div>
            <div class="col-span-2">
              <div class="mb-1 flex items-center justify-between gap-2">
                <label class="text-[11px] text-muted-foreground">{{ $t('task.task-cookie') }}</label>
                <BrowserCookiePicker
                  :url="cookiePickerUrl || 'https://'"
                  :disabled="!cookiePickerUrl"
                  @imported="onCookiesImported"
                />
              </div>
              <Textarea
                auto-complete="off"
                rows="2"
                :placeholder="$t('task.task-cookie')"
                v-model="form.cookie"
                class="resize-none text-xs"
              />
            </div>
            <div class="col-span-2">
              <label class="mb-1 flex items-center gap-1 text-[11px] text-muted-foreground">
                {{ $t('task.task-proxy') }}
                <a
                  class="inline-flex items-center gap-0.5 text-[10px] text-primary hover:underline"
                  target="_blank"
                  href="https://risuko.app/docs/guides/proxy"
                  rel="noopener noreferrer"
                >
                  {{ $t('preferences.proxy-tips') }}
                  <ExternalLink :size="10" />
                </a>
              </label>
              <Input
                :placeholder="$t('task.task-proxy-placeholder')"
                v-model="form.allProxy"
              />
            </div>
            <div class="col-span-2">
              <ui-checkbox
                :model-value="!!form.newTaskShowDownloading"
                @change="onNewTaskShowDownloadingChange"
              >
                {{ $t('task.navigate-to-downloading') }}
              </ui-checkbox>
            </div>
            <div v-if="showScheduleField" class="col-span-2">
              <label class="mb-1 block text-[11px] text-muted-foreground">{{
                $t('task.schedule-start-at')
              }}</label>
              <DateTimePicker
                v-model="form.startAt"
                :placeholder="$t('task.schedule-start-at-placeholder')"
              />
              <p
                v-if="showScheduleTorrentHint"
                class="mt-1 text-[11px] leading-snug text-muted-foreground"
              >
                {{ $t('task.schedule-torrent-ignored') }}
              </p>
            </div>
            <div class="col-span-2">
              <ui-checkbox
                :model-value="!!form.completionScriptOverride"
                @change="(v) => (form.completionScriptOverride = !!v)"
              >
                {{ $t('task.completion-script-override') }}
              </ui-checkbox>
            </div>
            <template v-if="form.completionScriptOverride">
              <div class="col-span-2">
                <label class="mb-1 block text-[11px] text-muted-foreground">{{
                  $t('preferences.completion-script-command')
                }}</label>
                <Input
                  v-model="form.completionScriptCommand"
                  :placeholder="
                    $t('preferences.completion-script-command-placeholder')
                  "
                />
              </div>
              <div class="col-span-2">
                <label class="mb-1 block text-[11px] text-muted-foreground">{{
                  $t('preferences.completion-script-args')
                }}</label>
                <Input
                  v-model="form.completionScriptArgs"
                  :placeholder="
                    $t('preferences.completion-script-args-placeholder')
                  "
                />
              </div>
              <div>
                <label class="mb-1 block text-[11px] text-muted-foreground">{{
                  $t('preferences.completion-script-timeout')
                }}</label>
                <NumberInput
                  v-model="form.completionScriptTimeoutMs"
                  :min="1000"
                  :max="300000"
                  :step="1000"
                />
              </div>
            </template>
            </div>
          </Motion>
        </AnimatePresence>
      </div>

      <DialogFooter class="flex shrink-0 flex-row items-center justify-between border-t border-border/60 px-4 py-3 sm:justify-between">
        <button
          type="button"
          class="flex items-center gap-1.5 rounded-md px-2 py-1 text-xs text-muted-foreground transition-colors hover:bg-muted hover:text-foreground disabled:opacity-50"
          :disabled="submitting"
          :aria-expanded="showAdvanced"
          :aria-label="$t('task.show-advanced-options')"
          :title="$t('task.show-advanced-options')"
          @click="showAdvanced = !showAdvanced"
        >
          <SlidersHorizontal :size="12" />
          {{ $t('task.show-advanced-options') }}
          <ChevronDown
            :size="12"
            class="transition-transform"
            :class="{ 'rotate-180': showAdvanced }"
          />
        </button>
        <div class="flex items-center gap-2">
          <ui-button :disabled="submitting" @click="handleCancel">{{ $t('app.cancel') }}</ui-button>
          <Motion :while-tap="reducedMotion ? undefined : { scale: 0.97 }" tag="div">
            <ui-button
              variant="primary"
              :disabled="submitting || uriPreparing || !canSubmit"
              @click="submitForm"
            >
              <Download :size="14" style="margin-right: 6px" />
              {{ submitting ? $t('task.loading-add-task') : $t('task.batch-submit', { count: submitCount }) }}
            </ui-button>
          </Motion>
        </div>
      </DialogFooter>
      <loading-overlay :show="submitting || uriPreparing" :text="$t('task.loading-add-task')" />
    </DialogContent>
  </Dialog>
</template>

<script lang="ts">
import {
	ChevronDown,
	Download,
	ExternalLink,
	FileArchive,
	SlidersHorizontal,
	TriangleAlert,
	X,
} from "@lucide/vue";
import {
	ADD_TASK_TYPE,
	NONE_SELECTED_FILES,
	SELECTED_ALL_FILES,
} from "@shared/constants";
import {
	detectUriProtocol,
	splitTaskLinks,
	splitTaskLinksWithRenames,
} from "@shared/utils";
import logger from "@shared/utils/logger";
import { buildOuts } from "@shared/utils/rename";
import { readText } from "@tauri-apps/plugin-clipboard-manager";
import { open as tauriOpen } from "@tauri-apps/plugin-dialog";
import { AnimatePresence, Motion } from "motion-v";
import { toast } from "vue-sonner";
import SelectDirectory from "@/components/Native/SelectDirectory.vue";
import HistoryDirectory from "@/components/Preference/HistoryDirectory.vue";
import BatchItemCard from "@/components/Task/BatchItemCard.vue";
import BrowserCookiePicker from "@/components/Task/BrowserCookiePicker.vue";
import { Accordion } from "@/components/ui/accordion";
import UiButton from "@/components/ui/compat/UiButton.vue";
import { DateTimePicker } from "@/components/ui/date-time-picker";
import {
	Dialog,
	DialogContent,
	DialogFooter,
	DialogHeader,
	DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import LoadingOverlay from "@/components/ui/LoadingOverlay.vue";
import NumberInput from "@/components/ui/NumberInput.vue";
import { Textarea } from "@/components/ui/textarea";
import is from "@/shims/platform";
import { useAppStore } from "@/store/app";
import {
	type BatchQueueItem,
	batchItemForFilePath,
	createUriBatchItem,
} from "@/store/batchQueue";
import { usePreferenceStore } from "@/store/preference";
import { useTaskStore } from "@/store/task";
import { buildOption, initTaskForm, type TaskForm } from "@/utils/task";
import { decodeThunderLink } from "@/utils/thunder";

interface FileLikeWithPath {
	name: string;
	path?: string;
}

export default {
	name: "add-task-dialog",
	components: {
		[BatchItemCard.name]: BatchItemCard,
		[HistoryDirectory.name]: HistoryDirectory,
		[SelectDirectory.name]: SelectDirectory,
		[LoadingOverlay.name]: LoadingOverlay,
		BrowserCookiePicker,
		UiButton,
		NumberInput,
		DateTimePicker,
		Dialog,
		DialogContent,
		DialogHeader,
		DialogTitle,
		DialogFooter,
		Input,
		Textarea,
		Accordion,
		AnimatePresence,
		Motion,
		Download,
		FileArchive,
		SlidersHorizontal,
		TriangleAlert,
		ChevronDown,
		ExternalLink,
		X,
	},
	props: {
		visible: { type: Boolean, default: false },
		type: { type: String, default: ADD_TASK_TYPE.URI },
	},
	data() {
		return {
			form: {} as TaskForm,
			uriDraft: "",
			uriCommit: Promise.resolve() as Promise<void>,
			uriGeneration: 0,
			uriCommitVersion: 0,
			uriSessionActive: false,
			uriPreparing: false,
			submitting: false,
			dragOver: false,
			showAdvanced: false,
			openItemIds: [] as string[],
			reducedMotion: false,
			lastQueueLength: 0,
		};
	},
	computed: {
		isRenderer: () => is.renderer(),
		queue(): BatchQueueItem[] {
			return useAppStore().addTaskQueue;
		},
		motionInitial() {
			return this.reducedMotion ? false : { opacity: 0, y: 8, scale: 0.98 };
		},
		motionAnimate() {
			return this.reducedMotion
				? { opacity: 1 }
				: { opacity: 1, y: 0, scale: 1 };
		},
		motionExit() {
			return this.reducedMotion
				? { opacity: 0 }
				: { opacity: 0, x: -24, height: 0 };
		},
		motionTransition() {
			return this.reducedMotion
				? { duration: 0 }
				: { type: "spring", stiffness: 360, damping: 30 };
		},
		draftLinkCount(): number {
			return splitTaskLinks(this.uriDraft || "").length;
		},
		showTorrentNetworkInterferenceWarning(): boolean {
			return (
				this.queue.some(
					(item) => item.kind === "magnet" || item.kind === "torrent",
				) ||
				splitTaskLinks(this.uriDraft || "").some((uri) => /^magnet:/i.test(uri))
			);
		},
		canSubmit(): boolean {
			return this.queue.length > 0 || this.draftLinkCount > 0;
		},
		hasSchedulableQueueItem(): boolean {
			return this.queue.some((it) => it.kind !== ADD_TASK_TYPE.TORRENT);
		},
		hasTorrentQueueItem(): boolean {
			return this.queue.some((it) => it.kind === ADD_TASK_TYPE.TORRENT);
		},
		showScheduleField(): boolean {
			if (this.hasSchedulableQueueItem || this.draftLinkCount > 0) {
				return true;
			}
			return this.queue.length === 0 && this.type !== ADD_TASK_TYPE.TORRENT;
		},
		showScheduleTorrentHint(): boolean {
			return !!this.form.startAt && this.hasTorrentQueueItem;
		},
		submitCount(): number {
			return this.queue.length + this.draftLinkCount;
		},
		cookiePickerUrl(): string {
			const fromDraft = splitTaskLinks(this.uriDraft || "").find((u) =>
				/^https?:\/\//i.test(u),
			);
			if (fromDraft) {
				return fromDraft;
			}
			const queued = this.queue
				.map((it) => (it as { uri?: string }).uri || "")
				.find((u) => /^https?:\/\//i.test(u));
			return queued || "";
		},
	},
	watch: {
		visible(current) {
			if (current) {
				void this.handleOpen();
			} else {
				this.invalidateUriCommitSession();
				this.uriDraft = "";
			}
		},
		queue: {
			deep: false,
			handler(items: BatchQueueItem[]) {
				if (items.length > this.lastQueueLength) {
					const newOnes = items.slice(this.lastQueueLength);
					const ids = new Set(this.openItemIds);
					for (const it of newOnes) {
						ids.add(it.id);
					}
					this.openItemIds = Array.from(ids);
				}
				this.lastQueueLength = items.length;
			},
		},
	},
	mounted() {
		if (typeof window !== "undefined" && window.matchMedia) {
			const mq = window.matchMedia("(prefers-reduced-motion: reduce)");
			this.reducedMotion = mq.matches;
			const handler = (e: MediaQueryListEvent) => {
				this.reducedMotion = e.matches;
			};
			this._reducedMotionMq = mq;
			this._reducedMotionHandler = handler;
			if (typeof mq.addEventListener === "function") {
				mq.addEventListener("change", handler);
			} else if (
				typeof (
					mq as MediaQueryList & {
						addListener?: (l: (e: MediaQueryListEvent) => void) => void;
					}
				).addListener === "function"
			) {
				(
					mq as MediaQueryList & {
						addListener: (l: (e: MediaQueryListEvent) => void) => void;
					}
				).addListener(handler);
			}
		}
		if (this.visible) {
			void this.handleOpen();
		}
	},
	beforeUnmount() {
		this.invalidateUriCommitSession();
		const mq = this._reducedMotionMq;
		const handler = this._reducedMotionHandler;
		if (mq && handler) {
			if (typeof mq.removeEventListener === "function") {
				mq.removeEventListener("change", handler);
			} else if (
				typeof (
					mq as MediaQueryList & {
						removeListener?: (l: (e: MediaQueryListEvent) => void) => void;
					}
				).removeListener === "function"
			) {
				(
					mq as MediaQueryList & {
						removeListener: (l: (e: MediaQueryListEvent) => void) => void;
					}
				).removeListener(handler);
			}
		}
		this._reducedMotionMq = null;
		this._reducedMotionHandler = null;
	},
	methods: {
		beginUriCommitSession() {
			this.uriGeneration += 1;
			this.uriCommitVersion += 1;
			this.uriSessionActive = true;
			this.uriPreparing = false;
			this.uriCommit = Promise.resolve();
			return this.uriGeneration;
		},
		invalidateUriCommitSession() {
			this.uriGeneration += 1;
			this.uriCommitVersion += 1;
			this.uriSessionActive = false;
			this.uriPreparing = false;
			this.uriCommit = Promise.resolve();
		},
		isUriCommitCurrent(generation: number) {
			return (
				this.uriSessionActive &&
				this.visible &&
				this.uriGeneration === generation
			);
		},
		restoreUriDraft(text: string) {
			if (!(this.uriDraft || "").trim()) {
				this.uriDraft = text;
			}
		},
		async handleOpen() {
			const generation = this.beginUriCommitSession();
			this.showAdvanced = false;
			this.form = initTaskForm({
				app: useAppStore().$state,
				preference: usePreferenceStore().$state,
			});
			const seeded = useAppStore().addTaskTorrents;
			if (seeded.length > 0) {
				const items = seeded
					.map((s) => batchItemForFilePath(`${s.path || ""}`, s.name))
					.filter(Boolean);
				if (items.length > 0) {
					useAppStore().enqueueBatchItems(items);
				}
				useAppStore().addTaskAddTorrents({ fileList: [] });
			}
			const seededUri = useAppStore().addTaskUrl;
			let hasSeededUri = false;
			if (seededUri?.trim()) {
				hasSeededUri = true;
				this.uriDraft = seededUri;
				useAppStore().updateAddTaskUrl("");
				await this.commitUriDraft();
				if (!this.isUriCommitCurrent(generation)) {
					return;
				}
			}
			if (!this.isUriCommitCurrent(generation)) {
				return;
			}
			this.lastQueueLength = useAppStore().addTaskQueue.length;
			this.openItemIds = useAppStore().addTaskQueue.map((it) => it.id);
			if (!hasSeededUri && !(this.uriDraft || "").trim()) {
				void this.tryFillFromClipboard(generation);
			}
			this.$nextTick(() => {
				if (this.isUriCommitCurrent(generation)) {
					this.focusUriInput();
				}
			});
		},
		// WebKitGTK denies navigator.clipboard.readText() (NotAllowedError, no DOM paste
		// access), so Linux also reads through the Rust plugin; its command runs on the
		// async runtime, and the race in tryFillFromClipboard bounds an unanswered read
		readClipboardText(): Promise<string> {
			return readText();
		},
		async tryFillFromClipboard(generation = this.uriGeneration) {
			if (!this.isUriCommitCurrent(generation)) {
				return;
			}
			let text = "";
			let timeoutId: ReturnType<typeof setTimeout> | undefined;
			try {
				text = await Promise.race([
					this.readClipboardText(),
					new Promise<string>((_resolve, reject) => {
						timeoutId = setTimeout(
							() => reject(new Error("clipboard timeout")),
							1000,
						);
					}),
				]);
			} catch (err) {
				logger.warn("[Risuko] readText clipboard failed:", err);
				return;
			} finally {
				clearTimeout(timeoutId);
			}
			if (!text || text.length > 4096) {
				return;
			}
			if (!this.isUriCommitCurrent(generation)) {
				return;
			}
			const firstLine = text.split(/\r?\n/)[0]?.trim() || "";
			if (!firstLine || !isLikelyTaskLink(firstLine)) {
				return;
			}
			if (this.uriPreparing || (this.uriDraft || "").trim()) {
				return;
			}
			this.uriDraft = text.trim();
		},
		focusUriInput() {
			const ref = this.$refs.uri as { $el?: HTMLElement } | undefined;
			const el = ref?.$el;
			if (el && typeof (el as HTMLTextAreaElement).focus === "function") {
				(el as HTMLTextAreaElement).focus();
			}
		},
		handleDialogOpenChange(open: boolean) {
			if (!open && !this.submitting) {
				this.handleClose();
			}
		},
		handleCancel() {
			if (this.submitting) {
				return;
			}
			this.handleClose();
		},
		handleClose() {
			if (this.submitting) {
				return;
			}
			this.invalidateUriCommitSession();
			const appStore = useAppStore();
			appStore.hideAddTaskDialog();
			appStore.updateAddTaskOptions({});
			this.uriDraft = "";
		},
		handleUriPaste() {
			this.$nextTick(() => {
				void this.commitUriDraft();
			});
		},
		handleUriEnter(ev: KeyboardEvent) {
			ev.preventDefault();
			void this.commitUriDraft();
		},
		handleUriSubmitShortcut(ev: KeyboardEvent) {
			ev.preventDefault();
			if (this.submitting || this.uriPreparing || !this.canSubmit) {
				return;
			}
			this.submitForm();
		},
		commitUriDraft(): Promise<void> {
			if (!this.uriSessionActive || !this.visible || this.uriPreparing) {
				return this.uriCommit;
			}
			const text = this.uriDraft || "";
			if (!text.trim()) {
				return this.uriCommit;
			}
			const generation = this.uriGeneration;
			const commitVersion = ++this.uriCommitVersion;
			this.uriDraft = "";
			const commit = async () => {
				let enqueued = false;
				try {
					if (!this.isUriCommitCurrent(generation)) {
						return;
					}
					const parsed = splitTaskLinksWithRenames(text);
					if (parsed.length === 0) {
						return;
					}
					const decoded = await Promise.all(
						parsed.map(async ({ uri, rename }) => ({
							uri: await decodeThunderLink(uri),
							rename,
						})),
					);
					if (!this.isUriCommitCurrent(generation)) {
						return;
					}
					const items = decoded.map(({ uri, rename }) => {
						const item = createUriBatchItem(uri);
						if (rename) {
							item.out = rename;
						}
						return item;
					});
					useAppStore().enqueueBatchItems(items);
					enqueued = true;
					this.notifyDetectedProtocols(decoded.map((item) => item.uri));
				} catch (error) {
					if (
						!enqueued &&
						this.isUriCommitCurrent(generation) &&
						this.uriCommitVersion === commitVersion
					) {
						this.restoreUriDraft(text);
					}
					logger.warn("[Risuko] URI commit failed; continuing:", error);
				}
			};
			const previous = this.uriCommit.catch((error: unknown) => {
				logger.warn("[Risuko] URI commit chain recovered:", error);
			});
			this.uriCommit = previous.then(commit);
			return this.uriCommit;
		},
		notifyDetectedProtocols(links: string[]) {
			if (!links || links.length === 0) {
				return;
			}
			const counts = new Map<string, number>();
			let unknown = 0;
			for (const uri of links) {
				const proto = detectUriProtocol(uri);
				if (proto) {
					counts.set(proto, (counts.get(proto) || 0) + 1);
				} else {
					unknown += 1;
				}
			}
			if (counts.size === 0 && unknown === 0) {
				return;
			}
			const parts = Array.from(counts.entries()).map(([name, n]) =>
				n > 1 ? `${name} \u00d7${n}` : name,
			);
			if (unknown > 0) {
				parts.push(
					unknown > 1
						? this.$t("task.unknown-protocol-multi", { count: unknown })
						: this.$t("task.unknown-protocol"),
				);
			}
			const summary = parts.join(", ");
			const title =
				counts.size === 1 && unknown === 0 && links.length === 1
					? this.$t("task.link-detected-single", { name: parts[0] })
					: this.$t("task.links-detected");
			toast.info(title, { description: summary });
		},
		handleDrop(ev: DragEvent) {
			this.dragOver = false;
			const files = Array.from(
				ev.dataTransfer?.files || [],
			) as FileLikeWithPath[];
			this.acceptTorrentFiles(files);
		},
		acceptTorrentFiles(files: FileLikeWithPath[]) {
			const items: BatchQueueItem[] = [];
			for (const f of files) {
				const path = `${f.path || ""}`.trim();
				const item = path ? batchItemForFilePath(path, f.name) : null;
				if (item) {
					items.push(item);
				}
			}
			if (items.length === 0) {
				toast.error(this.$t("task.new-task-file-required"));
				return;
			}
			useAppStore().enqueueBatchItems(items);
			toast.info(
				items.length === 1
					? this.$t("task.download-file-detected")
					: this.$t("task.download-file-detected-multi", {
							count: items.length,
						}),
			);
		},
		async browseTorrentFiles() {
			try {
				const selected = await tauriOpen({
					multiple: true,
					filters: [
						{
							name: "Torrent / Metalink / NZB",
							extensions: ["torrent", "meta4", "metalink", "nzb"],
						},
					],
				});
				if (!selected) {
					return;
				}
				const arr = Array.isArray(selected) ? selected : [selected];
				const items: BatchQueueItem[] = [];
				for (const path of arr) {
					if (typeof path !== "string" || !path) {
						continue;
					}
					const item = batchItemForFilePath(path, segmentName(path));
					if (item) {
						items.push(item);
					}
				}
				if (items.length > 0) {
					useAppStore().enqueueBatchItems(items);
				}
			} catch (err) {
				logger.warn("[Risuko] browseTorrentFiles failed:", err);
			}
		},
		removeItem(id: string) {
			useAppStore().removeBatchItem(id);
			this.openItemIds = this.openItemIds.filter((v) => v !== id);
		},
		updateSelection(id: string, selectFile: string) {
			useAppStore().updateBatchItem(id, { selectFile });
		},
		updateMedia(id: string, patch: Record<string, unknown>) {
			useAppStore().updateBatchItem(id, patch);
		},
		updateMirrors(id: string, mirrors: string[]) {
			useAppStore().updateBatchItem(id, { mirrors });
		},
		handleHistoryDirectorySelected(dir: string) {
			this.form.dir = dir;
		},
		handleNativeDirectorySelected(dir: string) {
			this.form.dir = dir;
			usePreferenceStore().recordHistoryDirectory(dir);
		},
		onNewTaskShowDownloadingChange(enable: boolean) {
			this.form.newTaskShowDownloading = !!enable;
		},
		buildSharedOptions(type = ADD_TASK_TYPE.URI, includeFileName = false) {
			const base = buildOption(type, this.form);
			delete (base as Record<string, unknown>).selectFile;
			if (!includeFileName) {
				delete (base as Record<string, unknown>).out;
			}
			return base;
		},
		normalizeAddTaskError(err: unknown): string {
			if (typeof err === "string") {
				return err.startsWith("task.") ? this.$t(err) : err;
			}
			const e = err as { rawMessage?: string; message?: string } | null;
			const raw = e?.rawMessage;
			if (raw) {
				return raw;
			}
			const key = e?.message;
			if (key) {
				return key.startsWith("task.") ? this.$t(key) : key;
			}
			return this.$t("task.batch-all-failed");
		},
		async submitPathBatch(
			items: BatchQueueItem[],
			submit: () => Promise<
				{ path: string; gid: string | null; error: string | null }[]
			>,
		): Promise<{ ok: number; fail: number }> {
			const appStore = useAppStore();
			let ok = 0;
			let fail = 0;
			try {
				const results = await submit();
				for (const res of results) {
					const item = items.find((it) => it.path === res.path);
					if (!item) {
						continue;
					}
					if (res.gid) {
						ok += 1;
						appStore.updateBatchItem(item.id, {
							status: "success",
							gid: res.gid,
						});
					} else {
						fail += 1;
						appStore.updateBatchItem(item.id, {
							status: "failed",
							error: res.error || this.$t("task.batch-all-failed"),
						});
					}
				}
			} catch (err: unknown) {
				fail += items.length;
				const msg = this.normalizeAddTaskError(err);
				for (const it of items) {
					appStore.updateBatchItem(it.id, {
						status: "failed",
						error: msg,
					});
				}
			}
			return { ok, fail };
		},
		async submitForm() {
			if (this.submitting || this.uriPreparing) {
				return;
			}
			const appStore = useAppStore();
			const generation = this.uriGeneration;
			if (!this.isUriCommitCurrent(generation)) {
				return;
			}
			try {
				if ((this.uriDraft || "").trim()) {
					this.commitUriDraft();
				}
				this.uriPreparing = true;
				do {
					const pending = this.uriCommit;
					await pending;
					if (pending === this.uriCommit) {
						break;
					}
				} while (this.isUriCommitCurrent(generation));
			} catch (error) {
				if (this.isUriCommitCurrent(generation)) {
					this.uriPreparing = false;
					logger.warn("[Risuko] URI preparation failed:", error);
				}
				return;
			}
			if (!this.isUriCommitCurrent(generation)) {
				return;
			}
			const items = appStore.addTaskQueue;
			if (items.length === 0) {
				this.uriPreparing = false;
				return;
			}
			this.uriPreparing = false;
			this.submitting = true;

			const torrentItems = items.filter(
				(it) => it.kind === "torrent" && it.path,
			);
			const invalidTorrentItems = items.filter(
				(it) => it.kind === "torrent" && !it.path,
			);
			const metalinkItems = items.filter(
				(it) => it.kind === "metalink" && it.path,
			);
			const nzbItems = items.filter((it) => it.kind === "nzb" && it.path);
			const uriItems = items.filter(
				(it) => it.kind === "uri" || it.kind === "magnet",
			);

			const taskStore = useTaskStore();
			let okCount = 0;
			let failCount = 0;

			for (const it of items) {
				appStore.updateBatchItem(it.id, { status: "submitting", error: "" });
			}

			if (invalidTorrentItems.length > 0) {
				const invalidMsg = this.$t("task.new-task-torrent-required");
				for (const it of invalidTorrentItems) {
					appStore.updateBatchItem(it.id, {
						status: "failed",
						error: invalidMsg,
					});
					failCount += 1;
				}
			}

			if (torrentItems.length > 0) {
				const r = await this.submitPathBatch(torrentItems, () =>
					taskStore.addTorrents({
						paths: torrentItems.map((it) => it.path as string),
						options: this.buildSharedOptions(
							ADD_TASK_TYPE.TORRENT,
							torrentItems.length === 1,
						),
					}),
				);
				okCount += r.ok;
				failCount += r.fail;
			}

			if (metalinkItems.length > 0) {
				const r = await this.submitPathBatch(metalinkItems, () =>
					taskStore.addMetalinks({
						paths: metalinkItems.map((it) => it.path as string),
						options: this.buildSharedOptions(),
					}),
				);
				okCount += r.ok;
				failCount += r.fail;
			}

			if (nzbItems.length > 0) {
				const r = await this.submitPathBatch(nzbItems, () =>
					taskStore.addNzbs({
						paths: nzbItems.map((it) => it.path as string),
						options: this.buildSharedOptions(),
					}),
				);
				okCount += r.ok;
				failCount += r.fail;
			}

			const formOuts = buildOuts(
				uriItems.map((it) => it.uri || ""),
				`${this.form.out || ""}`.trim(),
			);
			for (const [index, it] of uriItems.entries()) {
				try {
					const uri = it.uri as string;
					const uris = [uri, ...(it.mirrors ?? [])]
						.map((s) => s.trim())
						.filter(Boolean);
					const sharedOpts = this.buildSharedOptions();
					const sel = it.selectFile;
					if (
						sel &&
						sel !== SELECTED_ALL_FILES &&
						sel !== NONE_SELECTED_FILES
					) {
						sharedOpts.selectFile = sel;
					}
					if (it.forceYtdlp) {
						sharedOpts.forceYtdlp = true;
					}
					if ((it.isMedia || it.forceYtdlp) && it.mediaFormatId) {
						sharedOpts.mediaFormat = it.mediaFormatId;
					}
					const out = `${it.out || formOuts[index] || ""}`.trim();
					const outs = out ? [out] : [];
					await taskStore.addUri({
						uris,
						outs,
						options: sharedOpts as Record<string, string>,
					});
					okCount += 1;
					appStore.updateBatchItem(it.id, { status: "success" });
				} catch (err: unknown) {
					failCount += 1;
					const msg = this.normalizeAddTaskError(err);
					appStore.updateBatchItem(it.id, {
						status: "failed",
						error: msg,
					});
				}
			}

			this.submitting = false;
			const total = items.length;

			if (failCount === 0) {
				toast.success(this.$t("task.batch-all-success", { count: okCount }));
				appStore.hideAddTaskDialog();
				if (this.form.newTaskShowDownloading) {
					this.$router
						.push({ path: "/task/active" })
						.catch((err: unknown) => logger.log(err));
				}
			} else if (okCount === 0) {
				toast.error(this.$t("task.batch-all-failed"));
			} else {
				toast.warning(
					this.$t("task.batch-partial-success", { ok: okCount, total }),
				);
				const remaining = appStore.addTaskQueue.filter(
					(it) => it.status !== "success",
				);
				appStore.resetBatchQueue();
				appStore.enqueueBatchItems(remaining);
			}
		},
		onCookiesImported({
			result,
		}: {
			result: { cookieHeader: string; userAgent: string };
		}) {
			if (result.cookieHeader) {
				this.form.cookie = result.cookieHeader;
			}
			if (result.userAgent && !(this.form.userAgent || "").trim()) {
				this.form.userAgent = result.userAgent;
			}
		},
	},
};

function segmentName(path: string): string {
	const segs = `${path}`.split(/[/\\]/);
	return segs[segs.length - 1] || path;
}

function isLikelyTaskLink(line: string): boolean {
	const lower = line.toLowerCase();
	return (
		lower.startsWith("http://") ||
		lower.startsWith("https://") ||
		lower.startsWith("ftp://") ||
		lower.startsWith("ftps://") ||
		lower.startsWith("sftp://") ||
		lower.startsWith("magnet:") ||
		lower.startsWith("thunder://") ||
		lower.startsWith("ed2k://") ||
		lower.startsWith("adc://") ||
		lower.startsWith("adcs://") ||
		lower.startsWith("dchub://") ||
		lower.startsWith("nmdc://") ||
		lower.startsWith("gnutella://") ||
		lower.startsWith("gnet://") ||
		lower.startsWith("g2://") ||
		lower.startsWith("gift://")
	);
}
</script>

<style scoped>
.atd-scroll {
  scrollbar-width: none;
  -ms-overflow-style: none;
}
.atd-scroll::-webkit-scrollbar {
  width: 0;
  height: 0;
  display: none;
}
</style>
