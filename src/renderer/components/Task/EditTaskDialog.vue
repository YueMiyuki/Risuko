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
            <TabsTrigger v-if="showSeedingTab" value="seeding">{{ $t('task.edit-tab-seeding') }}</TabsTrigger>
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

          <TabsContent v-if="showSeedingTab" value="seeding" class="mt-0 flex flex-col gap-3">
            <p v-if="seedingInPlace" class="text-[11px] leading-relaxed text-muted-foreground">
              {{ $t('task.seeding-in-place-note') }}
            </p>
            <template v-else>
              <RadioGroupRoot
                :model-value="form.seedingChoice"
                class="flex flex-col gap-2.5"
                :aria-label="$t('task.edit-tab-seeding')"
                @update:model-value="onSeedingChoice"
              >
                <div v-for="option in seedingOptions" :key="option.value">
                  <div class="flex items-center gap-2">
                    <RadioGroupItem
                      :id="`edit-task-seeding-${option.value}`"
                      :value="option.value"
                      class="flex size-4 shrink-0 items-center justify-center rounded-full border border-input shadow-xs outline-none transition-shadow focus-visible:border-ring focus-visible:ring-[3px] focus-visible:ring-ring/50 data-[state=checked]:border-primary"
                      @select="seedingTouched = true"
                    >
                      <RadioGroupIndicator class="size-2 rounded-full bg-primary" />
                    </RadioGroupItem>
                    <label :for="`edit-task-seeding-${option.value}`" class="text-xs">{{ option.label }}</label>
                  </div>
                  <p v-if="option.value === 'global'" class="mt-1 ml-6 text-[11px] text-muted-foreground">
                    {{ globalSeedingText }}
                  </p>
                  <div
                    v-if="option.value === 'limits' && form.seedingChoice === 'limits'"
                    class="mt-2 ml-6 flex flex-col gap-2"
                  >
                    <div class="grid grid-cols-2 gap-3">
                      <div>
                        <label for="edit-task-seed-ratio" class="mb-1 block text-[11px] text-muted-foreground">{{ $t('preferences.seed-ratio') }}</label>
                        <NumberInput id="edit-task-seed-ratio" v-model="form.seedRatio" :min="0" :max="100" :step="0.1" />
                      </div>
                      <div>
                        <label for="edit-task-seed-time" class="mb-1 block text-[11px] text-muted-foreground">{{ $t('preferences.seed-time') }} ({{ $t('preferences.seed-time-unit') }})</label>
                        <NumberInput id="edit-task-seed-time" v-model="form.seedTime" :min="0" :max="525600" :step="1" />
                      </div>
                    </div>
                    <p class="text-[11px] leading-relaxed text-muted-foreground">{{ $t('task.seeding-limits-hint') }}</p>
                    <p v-if="seedingLimitsInvalid" class="text-[11px] text-destructive">{{ $t('task.seeding-limits-required') }}</p>
                  </div>
                </div>
              </RadioGroupRoot>

              <div
                v-if="showSeedingStatus"
                class="grid grid-cols-[auto_1fr] gap-x-3 gap-y-1 rounded-md border border-border/60 bg-muted/40 px-3 py-2 text-[11px]"
              >
                <span class="text-muted-foreground">{{ $t('task.task-upload-length') }}</span>
                <span class="tabular-nums">{{ uploadedText }}</span>
                <span class="text-muted-foreground">{{ $t('task.task-ratio') }}</span>
                <span class="tabular-nums">{{ currentRatio }}</span>
                <template v-if="seedingForText">
                  <span class="text-muted-foreground">{{ $t('task.seeding-for') }}</span>
                  <span class="tabular-nums">{{ seedingForText }}</span>
                </template>
              </div>

              <p v-if="seedingNotStarted" class="text-[11px] leading-relaxed text-muted-foreground">
                {{ $t('task.seeding-applies-after-download') }}
              </p>
              <p v-if="ed2kSharingOff" class="text-[11px] leading-relaxed text-muted-foreground">
                {{ $t('task.seeding-ed2k-sharing-off') }}
              </p>
              <p v-if="seedingLeftover" class="text-[11px] leading-relaxed text-muted-foreground">
                {{ $t('task.seeding-leftover-values') }}
              </p>
              <div
                v-if="seedingWarning"
                class="rounded-md border border-amber-500/40 bg-amber-500/10 px-3 py-2 text-[11px] leading-relaxed text-amber-700 dark:text-amber-300"
              >
                {{ seedingWarning }}
              </div>
            </template>
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
import {
	bytesToSize,
	calcRatio,
	checkTaskIsBT,
	checkTaskIsSeeder,
	checkTaskUploads,
	getFileName,
	getTaskName,
	parseBooleanConfig,
	timeFormat,
} from "@shared/utils";
import { convertTrackerDataToLine } from "@shared/utils/tracker";
import { RadioGroupIndicator, RadioGroupItem, RadioGroupRoot } from "reka-ui";
import type { PropType } from "vue";
import api from "@/api";
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
import {
	type GlobalSeedingKind,
	type GlobalSeedingSummary,
	globalSeedingSummary,
	type SeedingChoice,
	type SeedingRuleOptions,
	seedingChoiceOf,
	seedingRuleOptions,
} from "@/utils/seedingRule";

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
	seedingChoice: SeedingChoice;
	seedRatio: number;
	seedTime: number;
};

type Snapshot = EditForm;

type SeedingStatus = Partial<DownloadTask> & { uploadLength?: string };

const SEEDING_STATUS_KEYS = [
	"gid",
	"status",
	"seeder",
	"totalLength",
	"completedLength",
	"uploadLength",
	"seedingTime",
];

const GLOBAL_SEEDING_KEYS: Record<GlobalSeedingKind, string> = {
	off: "task.seeding-global-off",
	forever: "task.seeding-global-forever",
	ratio: "task.seeding-global-ratio",
	minutes: "task.seeding-global-minutes",
	both: "task.seeding-global-both",
};

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
		seedingChoice: "global",
		seedRatio: 0,
		seedTime: 0,
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
		RadioGroupIndicator,
		RadioGroupItem,
		RadioGroupRoot,
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
			seedingInPlace: false,
			seedingLeftover: false,
			seedingTouched: false,
			seedingStatus: null as SeedingStatus | null,
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
		showSeedingTab(): boolean {
			return checkTaskUploads(this.task);
		},
		seedingEditable(): boolean {
			return this.showSeedingTab && !this.seedingInPlace;
		},
		seedingOptions(): { value: SeedingChoice; label: string }[] {
			return [
				{ value: "global", label: this.$t("task.seeding-rule-global") },
				{ value: "forever", label: this.$t("task.seeding-rule-forever") },
				{ value: "limits", label: this.$t("task.seeding-rule-limits") },
				{ value: "none", label: this.$t("task.seeding-rule-none") },
			];
		},
		globalSeeding(): GlobalSeedingSummary {
			const { keepSeeding, seedTime, seedRatio } = usePreferenceStore().config;
			return globalSeedingSummary({ keepSeeding, seedTime, seedRatio });
		},
		globalSeedingText(): string {
			const { kind, ratio, minutes } = this.globalSeeding;
			return this.$t(GLOBAL_SEEDING_KEYS[kind], { ratio, minutes });
		},
		seedingRule(): SeedingRuleOptions | null {
			return seedingRuleOptions(this.form.seedingChoice, {
				ratio: Number(this.form.seedRatio),
				minutes: Number(this.form.seedTime),
			});
		},
		seedingLimitsInvalid(): boolean {
			return (
				this.seedingEditable &&
				this.form.seedingChoice === "limits" &&
				!this.seedingRule
			);
		},
		seedingChanged(): boolean {
			if (!this.seedingEditable) {
				return false;
			}
			if (this.form.seedingChoice !== this.snapshot.seedingChoice) {
				return true;
			}
			if (this.seedingLeftover && this.seedingTouched) {
				return true;
			}
			if (this.form.seedingChoice !== "limits") {
				return false;
			}
			return (
				Number(this.form.seedRatio) !== Number(this.snapshot.seedRatio) ||
				Number(this.form.seedTime) !== Number(this.snapshot.seedTime)
			);
		},
		seedingTask(): SeedingStatus | null {
			return this.seedingStatus ?? this.task;
		},
		isSeeder(): boolean {
			return checkTaskIsSeeder(this.seedingTask);
		},
		uploadedLength(): string {
			return this.seedingTask?.uploadLength ?? "0";
		},
		showSeedingStatus(): boolean {
			return Number(this.uploadedLength) > 0 || this.isSeeder;
		},
		uploadedText(): string {
			return bytesToSize(this.uploadedLength);
		},
		currentRatio(): number {
			return calcRatio(this.seedingTask?.totalLength, this.uploadedLength);
		},
		seedingTimeMs(): number | null {
			const raw = this.seedingTask?.seedingTime;
			if (raw === undefined || raw === null || `${raw}`.trim() === "") {
				return null;
			}
			const ms = Number(raw);
			return Number.isFinite(ms) && ms >= 0 ? ms : null;
		},
		seedingForText(): string {
			if (this.seedingTimeMs === null) {
				return "";
			}
			return timeFormat(Math.floor(this.seedingTimeMs / 1000), {
				i18n: {
					gt1d: this.$t("app.gt1d"),
					hour: this.$t("app.hour"),
					minute: this.$t("app.minute"),
					second: this.$t("app.second"),
				},
			}).trim();
		},
		seedingNotStarted(): boolean {
			if (this.seedingTask?.seeder === "true") {
				return false;
			}
			const total = Number(this.seedingTask?.totalLength) || 0;
			const completed = Number(this.seedingTask?.completedLength) || 0;
			return !(total > 0 && completed >= total);
		},
		ed2kSharingOff(): boolean {
			if (!this.task?.ed2kLink) {
				return false;
			}
			const { ed2kEnableUpload, ed2KEnableUpload } =
				usePreferenceStore().config;
			return !parseBooleanConfig(ed2kEnableUpload ?? ed2KEnableUpload, true);
		},
		seedingGoal(): { keep: boolean; ratio: number; minutes: number } {
			switch (this.form.seedingChoice) {
				case "global": {
					const { kind, ratio, minutes } = this.globalSeeding;
					return { keep: kind !== "off", ratio, minutes };
				}
				case "limits":
					return {
						keep: true,
						ratio: Number(this.seedingRule?.seedRatio) || 0,
						minutes: Number(this.seedingRule?.seedTime) || 0,
					};
				case "none":
					return { keep: false, ratio: 0, minutes: 0 };
				default:
					return { keep: true, ratio: 0, minutes: 0 };
			}
		},
		seedingWarning(): string {
			if (!this.isSeeder || !this.seedingChanged || this.seedingLimitsInvalid) {
				return "";
			}
			const { keep, ratio, minutes } = this.seedingGoal;
			if (!keep) {
				return this.$t("task.seeding-stops-now");
			}
			const ratioReached = ratio > 0 && this.currentRatio >= ratio;
			const timeReached =
				minutes > 0 &&
				this.seedingTimeMs !== null &&
				this.seedingTimeMs >= minutes * 60_000;
			return ratioReached || timeReached
				? this.$t("task.seeding-limit-reached")
				: "";
		},
		hasChanges(): boolean {
			return (
				this.urisChanged ||
				this.pathOrOptionsChanged ||
				this.trackersChanged ||
				this.seedingChanged
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
			if (this.seedingLimitsInvalid) {
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
		onSeedingChoice(value: unknown) {
			const choice = this.seedingOptions.find(
				(option) => option.value === value,
			)?.value;
			if (!choice) {
				return;
			}
			this.seedingTouched = true;
			if (
				choice === "limits" &&
				!(Number(this.form.seedRatio) > 0) &&
				!(Number(this.form.seedTime) > 0)
			) {
				const { ratio, minutes } = this.globalSeeding;
				const fromGlobal = ratio > 0 || minutes > 0;
				this.form.seedRatio = fromGlobal ? ratio : 1;
				this.form.seedTime = fromGlobal ? minutes : 0;
			}
			this.form.seedingChoice = choice;
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
			this.seedingTouched = false;
			this.seedingStatus = null;
			try {
				const statusRequest = checkTaskUploads(task)
					? api
							.fetchTaskItem({ gid: task.gid, keys: SEEDING_STATUS_KEYS })
							.catch(() => null)
					: Promise.resolve(null);
				const options = (await useTaskStore().getTaskOption(
					task.gid,
				)) as Record<string, unknown>;
				const status = await statusRequest;
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
					typeof options.btTracker === "string" ? options.btTracker : "";
				const seeding = seedingChoiceOf(options);
				const form: EditForm = {
					out:
						(typeof options.out === "string" && options.out) ||
						getFileName(this.task.files?.[0]?.path ?? "") ||
						"",
					dir:
						this.task.dir ||
						(typeof options.dir === "string" && options.dir) ||
						"",
					split: Math.max(1, Math.min(128, Number(options.split) || 16)),
					maxDownloadLimit: normalizeLimit(options.maxDownloadLimit),
					primaryUri: uris[0] || "",
					mirrors: uris.slice(1),
					trackersToAdd: "",
					allProxy:
						(typeof options.allProxy === "string" && options.allProxy) || "",
					userAgent:
						headerUserAgent ||
						(typeof options.userAgent === "string" && options.userAgent) ||
						"",
					referer:
						(typeof options.referer === "string" && options.referer) || "",
					cookie: cookie || "",
					authorization: authorization || "",
					seedingChoice: seeding.choice,
					seedRatio: seeding.ratio,
					seedTime: seeding.minutes,
				};
				this.existingTrackers = parseTrackerLines(
					[trackerLine, optionTracker].filter(Boolean).join("\n"),
				);
				this.otherHeaders = otherHeaders;
				this.seedingInPlace = seeding.inPlace;
				this.seedingLeftover = seeding.leftover;
				this.seedingStatus = status;
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

			if (this.seedingChanged && this.seedingRule) {
				Object.assign(options, this.seedingRule);
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
