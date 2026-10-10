<template>
  <Dialog :open="visible" @update:open="onOpenChange">
    <DialogContent
      class="flex max-h-[85vh] w-[min(36rem,92vw)] flex-col gap-0 overflow-hidden p-0"
    >
      <DialogHeader class="flex shrink-0 flex-row items-center gap-2 border-b border-border/60 px-4 py-3">
        <div class="flex size-7 items-center justify-center rounded-md bg-sky-500/15 text-sky-600 dark:text-sky-400">
          <Pencil :size="14" />
        </div>
        <DialogTitle class="flex-1 text-sm font-medium">
          {{ $t('task.edit-dialog-title') }}
        </DialogTitle>
      </DialogHeader>

      <div class="min-h-0 flex-1 overflow-y-auto px-4 py-3">
        <p v-if="taskName" class="mb-3 truncate text-xs font-medium">{{ taskName }}</p>

        <div
          v-if="loading"
          class="flex items-center justify-center py-10 text-xs text-muted-foreground"
        >
          …
        </div>

        <Tabs v-else v-model="activeTab" class="w-full">
          <TabsList class="mb-3 w-full justify-start">
            <TabsTrigger v-if="!isBT" value="general">{{ $t('task.edit-tab-general') }}</TabsTrigger>
            <TabsTrigger v-if="!isBT" value="sources">{{ $t('task.edit-tab-sources') }}</TabsTrigger>
            <TabsTrigger v-if="isBT" value="trackers">{{ $t('task.edit-tab-trackers') }}</TabsTrigger>
            <TabsTrigger value="network">{{ $t('task.edit-tab-network') }}</TabsTrigger>
          </TabsList>

          <TabsContent v-if="!isBT" value="general" class="mt-0 flex flex-col gap-3">
            <div>
              <label for="edit-task-out" class="mb-1 block text-[11px] text-muted-foreground">{{ $t('task.task-out') }}</label>
              <Input id="edit-task-out" v-model="form.out" autocomplete="off" :placeholder="$t('task.task-out-tips')" />
            </div>
            <div>
              <label for="edit-task-dir" class="mb-1 block text-[11px] text-muted-foreground">{{ $t('task.task-dir') }}</label>
              <div class="input-group input-group--bordered">
                <span class="input-prepend">
                  <history-directory @selected="onHistoryDir" />
                </span>
                <Input
                  id="edit-task-dir"
                  v-model="form.dir"
                  readonly
                  class="path-indicator-field flex-1 rounded-none border-none shadow-none noinput"
                />
                <span class="input-append" v-if="isRenderer">
                  <select-directory @selected="onNativeDir" />
                </span>
              </div>
            </div>
            <div class="grid grid-cols-2 gap-3">
              <div>
                <label for="edit-task-split" class="mb-1 block text-[11px] text-muted-foreground">{{ $t('task.task-split') }}</label>
                <NumberInput id="edit-task-split" v-model="form.split" :min="1" :max="128" />
              </div>
              <div>
                <label for="edit-task-speed-limit" class="mb-1 block text-[11px] text-muted-foreground">{{ $t('task.edit-speed-limit') }}</label>
                <Input
                  id="edit-task-speed-limit"
                  v-model="form.maxDownloadLimit"
                  autocomplete="off"
                  :placeholder="$t('task.edit-speed-limit-placeholder')"
                />
              </div>
            </div>
          </TabsContent>

          <TabsContent v-if="!isBT" value="sources" class="mt-0 flex flex-col gap-3">
            <div>
              <label for="edit-task-primary-uri" class="mb-1 block text-[11px] text-muted-foreground">{{ $t('task.edit-primary-uri') }}</label>
              <Textarea
                id="edit-task-primary-uri"
                v-model="form.primaryUri"
                rows="2"
                class="resize-none font-mono text-xs"
                :placeholder="$t('task.edit-primary-uri-placeholder')"
              />
            </div>
            <div>
              <label for="edit-task-mirror-draft" class="mb-1 block text-[11px] text-muted-foreground">{{ $t('task.edit-mirrors') }}</label>
              <p class="mb-2 text-[11px] leading-relaxed text-muted-foreground">
                {{ $t('task.mirror-hint') }}
              </p>
              <ul v-if="form.mirrors.length" class="mb-2 flex flex-col gap-1.5">
                <li
                  v-for="(mirror, idx) in form.mirrors"
                  :key="`${mirror}-${idx}`"
                  class="flex items-center gap-2 rounded-md border border-border/60 px-2 py-1.5"
                >
                  <span class="min-w-0 flex-1 truncate font-mono text-[11px] text-foreground [&_a]:text-inherit [&_a]:no-underline" :title="mirror">{{ mirror }}</span>
                  <button
                    type="button"
                    class="text-muted-foreground hover:text-destructive"
                    :aria-label="$t('task.mirror-remove')"
                    :title="$t('task.mirror-remove')"
                    @click="removeMirror(idx)"
                  >
                    <X :size="12" />
                  </button>
                </li>
              </ul>
              <div class="flex gap-2">
                <Input
                  id="edit-task-mirror-draft"
                  v-model="mirrorDraft"
                  class="flex-1 font-mono text-xs"
                  :placeholder="$t('task.mirror-placeholder')"
                  @keydown.enter.prevent="addMirror"
                />
                <Button size="sm" variant="secondary" :disabled="!canAddMirror" @click="addMirror">
                  {{ $t('task.edit-mirror-add') }}
                </Button>
              </div>
            </div>
          </TabsContent>

          <TabsContent v-if="isBT" value="trackers" class="mt-0 flex flex-col gap-3">
            <div v-if="existingTrackers.length">
              <div class="mb-1 text-[11px] text-muted-foreground">{{ $t('preferences.bt-tracker') }}</div>
              <ul class="max-h-36 space-y-1.5 overflow-y-auto">
                <li
                  v-for="(tracker, idx) in existingTrackers"
                  :key="`${tracker}-${idx}`"
                  class="block min-h-7 truncate rounded-md border border-border/60 bg-muted/40 px-2 py-1.5 font-mono text-[11px] leading-5 text-foreground [&_a]:text-inherit [&_a]:no-underline"
                  :title="tracker"
                >
                  {{ tracker }}
                </li>
              </ul>
            </div>
            <div>
              <label for="edit-task-trackers" class="mb-2 block text-[11px] leading-relaxed text-muted-foreground">
                {{ $t('task.edit-trackers-hint') }}
              </label>
              <Textarea
                id="edit-task-trackers"
                v-model="form.trackersToAdd"
                rows="5"
                class="resize-y font-mono text-xs"
                :placeholder="$t('preferences.bt-tracker-input-tips')"
              />
            </div>
          </TabsContent>

          <TabsContent value="network" class="mt-0 flex flex-col gap-3">
            <div>
              <label for="edit-task-proxy" class="mb-1 block text-[11px] text-muted-foreground">{{ $t('task.task-proxy') }}</label>
              <Input
                id="edit-task-proxy"
                v-model="form.allProxy"
                :placeholder="$t('task.task-proxy-placeholder')"
              />
            </div>
            <div>
              <label for="edit-task-user-agent" class="mb-1 block text-[11px] text-muted-foreground">{{ $t('task.task-user-agent') }}</label>
              <Textarea id="edit-task-user-agent" v-model="form.userAgent" rows="2" class="resize-none text-xs" />
            </div>
            <div>
              <label for="edit-task-referer" class="mb-1 block text-[11px] text-muted-foreground">{{ $t('task.task-referer') }}</label>
              <Textarea id="edit-task-referer" v-model="form.referer" rows="2" class="resize-none text-xs" />
            </div>
            <div>
              <label for="edit-task-cookie" class="mb-1 block text-[11px] text-muted-foreground">{{ $t('task.task-cookie') }}</label>
              <Textarea id="edit-task-cookie" v-model="form.cookie" rows="2" class="resize-none text-xs" />
            </div>
            <div>
              <label for="edit-task-authorization" class="mb-1 block text-[11px] text-muted-foreground">{{ $t('task.task-authorization') }}</label>
              <Textarea id="edit-task-authorization" v-model="form.authorization" rows="2" class="resize-none text-xs" />
            </div>
          </TabsContent>
        </Tabs>

        <div v-if="!loading && showRestartWarning" class="mt-3 rounded-md border border-amber-500/40 bg-amber-500/10 px-3 py-2 text-[11px] leading-relaxed text-amber-700 dark:text-amber-300">
          {{ $t('task.edit-restart-warning') }}
        </div>
        <div v-if="!loading && showProgressWarning" class="mt-2 rounded-md border border-orange-500/40 bg-orange-500/10 px-3 py-2 text-[11px] leading-relaxed text-orange-700 dark:text-orange-300">
          {{ $t('task.edit-progress-warning') }}
        </div>
      </div>

      <DialogFooter class="flex shrink-0 justify-end gap-2 border-t border-border/60 px-4 py-3">
        <Button variant="ghost" size="sm" :disabled="submitting" @click="close">
          {{ $t('app.cancel') }}
        </Button>
        <Button size="sm" :disabled="!canConfirm || submitting" @click="confirm">
          {{ $t('task.edit-confirm') }}
        </Button>
      </DialogFooter>
    </DialogContent>
  </Dialog>
</template>

<script lang="ts">
import { Pencil, X } from "@lucide/vue";
import { TASK_STATUS } from "@shared/constants";
import type { DownloadTask } from "@shared/types/task";
import { checkTaskIsBT, getTaskName } from "@shared/utils";
import { convertTrackerDataToLine } from "@shared/utils/tracker";
import type { PropType } from "vue";
import SelectDirectory from "@/components/Native/SelectDirectory.vue";
import HistoryDirectory from "@/components/Preference/HistoryDirectory.vue";
import { Button } from "@/components/ui/button";
import {
	Dialog,
	DialogContent,
	DialogFooter,
	DialogHeader,
	DialogTitle,
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import NumberInput from "@/components/ui/NumberInput.vue";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Textarea } from "@/components/ui/textarea";
import is from "@/shims/platform";
import { usePreferenceStore } from "@/store/preference";
import { useTaskStore } from "@/store/task";

type EditForm = {
	out: string;
	dir: string;
	split: number;
	maxDownloadLimit: string;
	primaryUri: string;
	mirrors: string[];
	trackersToAdd: string;
	allProxy: string;
	userAgent: string;
	referer: string;
	cookie: string;
	authorization: string;
};

type Snapshot = EditForm;

function emptyForm(): EditForm {
	return {
		out: "",
		dir: "",
		split: 16,
		maxDownloadLimit: "",
		primaryUri: "",
		mirrors: [],
		trackersToAdd: "",
		allProxy: "",
		userAgent: "",
		referer: "",
		cookie: "",
		authorization: "",
	};
}

function normalizeLimit(raw: unknown): string {
	if (raw === undefined || raw === null) {
		return "";
	}
	const s = String(raw).trim();
	if (!s || s === "0") {
		return "";
	}
	return s;
}

function splitHeaderLines(header: unknown): {
	cookie: string;
	authorization: string;
	userAgent: string;
	others: string[];
} {
	let cookie = "";
	let authorization = "";
	let userAgent = "";
	const others: string[] = [];
	const lines: string[] = Array.isArray(header)
		? header.map(String)
		: typeof header === "string"
			? header.split(/\r?\n/)
			: [];
	for (const line of lines) {
		const idx = line.indexOf(":");
		if (idx < 0) {
			continue;
		}
		const name = line.substring(0, idx).trim().toLowerCase();
		const value = line.substring(idx + 1).trim();
		if (name === "cookie") {
			cookie ||= value;
		} else if (name === "authorization") {
			authorization ||= value;
		} else if (name === "user-agent") {
			userAgent ||= value;
		} else {
			others.push(line.trim());
		}
	}
	return { cookie, authorization, userAgent, others };
}

function parseTrackerLines(text: string): string[] {
	const out: string[] = [];
	const seen = new Set<string>();
	for (const part of text.split(/[\n\r,]+/)) {
		const t = part.trim();
		if (!t) {
			continue;
		}
		const key = t.toLowerCase();
		if (seen.has(key)) {
			continue;
		}
		seen.add(key);
		out.push(t);
	}
	return out;
}

export default {
	name: "edit-task-dialog",
	components: {
		Pencil,
		X,
		Button,
		Dialog,
		DialogContent,
		DialogFooter,
		DialogHeader,
		DialogTitle,
		Input,
		NumberInput,
		Textarea,
		Tabs,
		TabsList,
		TabsTrigger,
		TabsContent,
		[HistoryDirectory.name]: HistoryDirectory,
		[SelectDirectory.name]: SelectDirectory,
	},
	props: {
		visible: { type: Boolean, default: false },
		task: { type: Object as PropType<DownloadTask | null>, default: null },
	},
	emits: ["update:visible"],
	data() {
		return {
			activeTab: "general",
			loading: false,
			submitting: false,
			mirrorDraft: "",
			form: emptyForm(),
			snapshot: emptyForm() as Snapshot,
			existingTrackers: [] as string[],
			otherHeaders: [] as string[],
			reloadGeneration: 0,
		};
	},
	computed: {
		isRenderer: () => is.renderer(),
		taskName(): string {
			return this.task ? getTaskName(this.task) : "";
		},
		isBT(): boolean {
			return checkTaskIsBT(this.task);
		},
		isActive(): boolean {
			return this.task?.status === TASK_STATUS.ACTIVE;
		},
		canAddMirror(): boolean {
			const candidate = this.mirrorDraft.trim();
			if (!candidate) {
				return false;
			}
			const lower = candidate.toLowerCase();
			if (this.form.primaryUri.trim().toLowerCase() === lower) {
				return false;
			}
			return !this.form.mirrors.some((m) => m.trim().toLowerCase() === lower);
		},
		urisChanged(): boolean {
			if (this.isBT) {
				return false;
			}
			const next = [
				this.form.primaryUri.trim(),
				...this.form.mirrors.map((m) => m.trim()).filter(Boolean),
			].filter(Boolean);
			const prev = [
				this.snapshot.primaryUri.trim(),
				...this.snapshot.mirrors.map((m) => m.trim()).filter(Boolean),
			].filter(Boolean);
			if (next.length !== prev.length) {
				return true;
			}
			return next.some((u, i) => u !== prev[i]);
		},
		primaryUriChanged(): boolean {
			if (this.isBT) {
				return false;
			}
			return (
				this.form.primaryUri.trim() !== this.snapshot.primaryUri.trim() &&
				!!this.form.primaryUri.trim()
			);
		},
		pathOrOptionsChanged(): boolean {
			return (
				(!this.isBT && this.form.out.trim() !== this.snapshot.out.trim()) ||
				(!this.isBT && this.form.dir.trim() !== this.snapshot.dir.trim()) ||
				(!this.isBT &&
					Number(this.form.split) !== Number(this.snapshot.split)) ||
				normalizeLimit(this.form.maxDownloadLimit) !==
					normalizeLimit(this.snapshot.maxDownloadLimit) ||
				this.form.allProxy.trim() !== this.snapshot.allProxy.trim() ||
				this.form.userAgent.trim() !== this.snapshot.userAgent.trim() ||
				this.form.referer.trim() !== this.snapshot.referer.trim() ||
				this.form.cookie.trim() !== this.snapshot.cookie.trim() ||
				this.form.authorization.trim() !== this.snapshot.authorization.trim()
			);
		},
		newTrackers(): string[] {
			if (!this.isBT) {
				return [];
			}
			const known = new Set(this.existingTrackers.map((t) => t.toLowerCase()));
			return parseTrackerLines(this.form.trackersToAdd).filter(
				(t) => !known.has(t.toLowerCase()),
			);
		},
		trackersChanged(): boolean {
			return this.newTrackers.length > 0;
		},
		hasChanges(): boolean {
			return (
				this.urisChanged || this.pathOrOptionsChanged || this.trackersChanged
			);
		},
		canConfirm(): boolean {
			if (this.loading || this.submitting || !this.task) {
				return false;
			}
			if (!this.hasChanges) {
				return false;
			}
			if (!this.isBT && !this.form.primaryUri.trim()) {
				return false;
			}
			if (!this.isBT && !this.form.dir.trim()) {
				return false;
			}
			return true;
		},
		showRestartWarning(): boolean {
			return (
				this.isActive &&
				!this.isBT &&
				(this.urisChanged || this.pathOrOptionsChanged)
			);
		},
		showProgressWarning(): boolean {
			return this.primaryUriChanged;
		},
	},
	watch: {
		visible(v: boolean) {
			if (v) {
				this.reload();
			}
		},
		task() {
			if (this.visible) {
				this.reload();
			}
		},
	},
	methods: {
		onOpenChange(open: boolean) {
			if (!open) {
				this.close();
			}
		},
		close() {
			this.$emit("update:visible", false);
		},
		onHistoryDir(dir: string) {
			this.form.dir = dir;
		},
		onNativeDir(dir: string) {
			this.form.dir = dir;
			usePreferenceStore().recordHistoryDirectory(dir);
		},
		addMirror() {
			if (!this.canAddMirror) {
				return;
			}
			this.form.mirrors = [...this.form.mirrors, this.mirrorDraft.trim()];
			this.mirrorDraft = "";
		},
		removeMirror(idx: number) {
			this.form.mirrors = this.form.mirrors.filter((_, i) => i !== idx);
		},
		taskUris(): string[] {
			const files = this.task?.files;
			if (files?.[0]?.uris?.length) {
				return files[0].uris.map((u) => u.uri).filter(Boolean);
			}
			return [];
		},
		async reload() {
			if (!this.task) {
				return;
			}
			const task = this.task;
			const generation = ++this.reloadGeneration;
			this.loading = true;
			this.activeTab = this.isBT ? "trackers" : "general";
			this.mirrorDraft = "";
			try {
				const options = (await useTaskStore().getTaskOption(
					task.gid,
				)) as Record<string, unknown>;
				if (
					generation !== this.reloadGeneration ||
					this.task?.gid !== task.gid
				) {
					return;
				}
				const uris = this.taskUris();
				const {
					cookie: headerCookie,
					authorization,
					userAgent: headerUserAgent,
					others: otherHeaders,
				} = splitHeaderLines(options.header);
				const cookie =
					(typeof options.cookie === "string" && options.cookie) ||
					headerCookie;
				const announceList = this.task.bittorrent?.announceList || [];
				const trackerLine = convertTrackerDataToLine(
					announceList
						.flatMap((tier) => (Array.isArray(tier) ? tier : [tier]))
						.filter(Boolean),
				);
				const optionTracker =
					typeof options.btTracker === "string"
						? options.btTracker
						: typeof options["bt-tracker"] === "string"
							? (options["bt-tracker"] as string)
							: "";
				const form: EditForm = {
					out:
						(typeof options.out === "string" && options.out) ||
						this.task.files?.[0]?.path?.split(/[/\\]/).pop() ||
						"",
					dir:
						(typeof options.dir === "string" && options.dir) ||
						this.task.dir ||
						"",
					split: Math.max(1, Math.min(128, Number(options.split) || 16)),
					maxDownloadLimit: normalizeLimit(
						options.maxDownloadLimit ?? options["max-download-limit"],
					),
					primaryUri: uris[0] || "",
					mirrors: uris.slice(1),
					trackersToAdd: "",
					allProxy:
						(typeof options.allProxy === "string" && options.allProxy) ||
						(typeof options["all-proxy"] === "string" &&
							(options["all-proxy"] as string)) ||
						"",
					userAgent:
						headerUserAgent ||
						(typeof options.userAgent === "string" && options.userAgent) ||
						(typeof options["user-agent"] === "string" &&
							(options["user-agent"] as string)) ||
						"",
					referer:
						(typeof options.referer === "string" && options.referer) || "",
					cookie: cookie || "",
					authorization: authorization || "",
				};
				this.existingTrackers = parseTrackerLines(
					[trackerLine, optionTracker].filter(Boolean).join("\n"),
				);
				this.otherHeaders = otherHeaders;
				this.form = form;
				this.snapshot = {
					...form,
					mirrors: [...form.mirrors],
				};
			} catch (err) {
				if (
					generation !== this.reloadGeneration ||
					this.task?.gid !== task.gid
				) {
					return;
				}
				this.$msg?.error?.(
					(err as Error)?.message || this.$t("task.edit-fail"),
				);
				this.close();
			} finally {
				if (generation === this.reloadGeneration) {
					this.loading = false;
				}
			}
		},
		buildPatch(): {
			uris?: string[];
			dir?: string;
			out?: string;
			trackers?: string[];
			options?: Record<string, unknown>;
		} | null {
			const patch: {
				uris?: string[];
				dir?: string;
				out?: string;
				trackers?: string[];
				options?: Record<string, unknown>;
			} = {};
			const options: Record<string, unknown> = {};

			if (!this.isBT && this.urisChanged) {
				const uris = [
					this.form.primaryUri.trim(),
					...this.form.mirrors.map((m) => m.trim()).filter(Boolean),
				].filter(Boolean);
				patch.uris = uris;
			}

			if (!this.isBT && this.form.out.trim() !== this.snapshot.out.trim()) {
				patch.out = this.form.out.trim();
			}
			if (!this.isBT && this.form.dir.trim() !== this.snapshot.dir.trim()) {
				patch.dir = this.form.dir.trim();
			}
			if (
				!this.isBT &&
				Number(this.form.split) !== Number(this.snapshot.split)
			) {
				options.split = Math.max(
					1,
					Math.min(128, Number(this.form.split) || 1),
				);
			}

			const nextLimit = normalizeLimit(this.form.maxDownloadLimit);
			const prevLimit = normalizeLimit(this.snapshot.maxDownloadLimit);
			if (nextLimit !== prevLimit) {
				options.maxDownloadLimit = nextLimit || "0";
			}

			if (this.form.allProxy.trim() !== this.snapshot.allProxy.trim()) {
				options.allProxy = this.form.allProxy.trim();
			}
			if (this.form.userAgent.trim() !== this.snapshot.userAgent.trim()) {
				options.userAgent = this.form.userAgent.trim();
			}
			if (this.form.referer.trim() !== this.snapshot.referer.trim()) {
				options.referer = this.form.referer.trim();
			}

			const headerChanged =
				this.form.cookie.trim() !== this.snapshot.cookie.trim() ||
				this.form.authorization.trim() !== this.snapshot.authorization.trim() ||
				this.form.userAgent.trim() !== this.snapshot.userAgent.trim();
			if (headerChanged) {
				const header: string[] = [...this.otherHeaders];
				if (this.form.userAgent.trim()) {
					header.push(`User-Agent: ${this.form.userAgent.trim()}`);
				}
				if (this.form.cookie.trim()) {
					header.push(`Cookie: ${this.form.cookie.trim()}`);
				}
				if (this.form.authorization.trim()) {
					header.push(`Authorization: ${this.form.authorization.trim()}`);
				}
				options.header = header;
				if (this.form.cookie.trim()) {
					options.cookie = this.form.cookie.trim();
				} else {
					options.cookie = null;
				}
			}

			if (this.newTrackers.length) {
				patch.trackers = this.newTrackers;
			}

			if (Object.keys(options).length) {
				patch.options = options;
			}

			if (
				!patch.uris &&
				!patch.dir &&
				!patch.out &&
				!patch.trackers &&
				!patch.options
			) {
				return null;
			}
			return patch;
		},
		async confirm() {
			if (!this.task || !this.canConfirm) {
				return;
			}
			const patch = this.buildPatch();
			if (!patch) {
				this.$msg?.info?.(this.$t("task.edit-no-changes"));
				return;
			}
			this.submitting = true;
			try {
				await useTaskStore().updateTask(this.task.gid, patch);
				this.$msg?.success?.(this.$t("task.edit-success"));
				this.close();
			} catch (err) {
				this.$msg?.error?.(
					(err as Error)?.message || this.$t("task.edit-fail"),
				);
			} finally {
				this.submitting = false;
			}
		},
	},
};
</script>
