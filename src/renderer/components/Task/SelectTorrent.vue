<template>
  <div class="selective-torrent relative">
    <div class="torrent-info">
      <ui-tooltip class="item torrent-name" effect="dark" :content="name" placement="top">
        <span>{{ name }}</span>
      </ui-tooltip>
    </div>
    <div v-if="previewDisabled" class="torrent-preview-notice">
      <div class="torrent-preview-notice-text">{{ previewNotice }}</div>
      <ui-button size="sm" variant="outline" :disabled="isBusy" @click="handlePreviewAnyway">
        {{
          previewLoading ? $t('task.torrent-preview-loading') : $t('task.torrent-preview-anyway')
        }}
      </ui-button>
    </div>
    <div v-else-if="previewPaged" class="torrent-preview-paged">
      <div class="torrent-preview-paged-tip">
        {{
          $t('task.torrent-preview-root-tip', {
            rootCount: previewRootItems.length,
            fileCount: previewTotalCount,
          })
        }}
      </div>
      <div class="torrent-preview-paged-tip">
        {{
          $t('task.torrent-preview-selected-count', {
            count: previewSelectedCount,
            total: previewTotalCount,
          })
        }}
      </div>
      <div class="torrent-preview-explorer">
        <div class="torrent-preview-explorer-head">
          <span class="torrent-preview-head-checkbox">
            <Checkbox
              :model-value="previewLoadedSelectionState"
              :disabled="previewLoadedFileIndices.length === 0"
              :aria-label="$t('task.select-all-task')"
              :title="$t('task.select-all-task')"
              @update:model-value="toggleLoadedPreviewSelection"
            />
          </span>
          <span>{{ $t('task.file-name') }}</span>
          <span>{{ $t('task.file-size') }}</span>
        </div>
        <div class="torrent-preview-explorer-body">
          <div
            v-for="row in previewTreeRows"
            :key="row.key"
            class="torrent-preview-explorer-row"
            :class="{ 'is-selected': row.type === 'file' && isPreviewFileSelected(row) }"
            :style="{ paddingInlineStart: `${8 + row.depth * 14}px` }"
          >
            <span class="torrent-preview-row-checkbox">
              <Checkbox
                v-if="row.type === 'file'"
                :model-value="isPreviewFileSelected(row)"
                :title="`${$t('task.file-name')}: ${row.name}`"
                :aria-label="`${$t('task.file-name')}: ${row.name}`"
                @update:model-value="(checked) => togglePreviewFileSelection(row, checked)"
              />
              <Checkbox
                v-else-if="row.type === 'folder'"
                :model-value="previewFolderSelectionState(row)"
                :disabled="row.loading || !hasPreviewFolderSelectableRange(row)"
                :title="`${$t('task.torrent-preview-folder-select')}: ${row.name}`"
                :aria-label="`${$t('task.torrent-preview-folder-select')}: ${row.name}`"
                @update:model-value="(checked) => togglePreviewFolderSelection(row, checked)"
              />
              <span v-else class="torrent-preview-checkbox-placeholder"></span>
            </span>
            <button
              v-if="row.type === 'folder'"
              type="button"
              class="torrent-preview-toggle"
              :disabled="row.loading || isBusy"
              :title="$t('task.torrent-preview-folder-toggle')"
              :aria-label="$t('task.torrent-preview-folder-toggle')"
              @click="togglePreviewFolder(row.path)"
            >
              <ChevronRight :size="12" :class="{ 'is-expanded': row.expanded }" />
            </button>
            <span v-else class="torrent-preview-toggle-placeholder"></span>
            <Folder v-if="row.type === 'folder'" :size="13" class="torrent-preview-icon" />
            <File v-else-if="row.type === 'file'" :size="13" class="torrent-preview-icon" />
            <span v-else class="torrent-preview-load-more-icon"></span>
            <span class="torrent-preview-name" :title="row.fullPath">
              <button
                v-if="row.type === 'load-more'"
                type="button"
                class="torrent-preview-load-more-btn"
                :disabled="row.loading || isBusy"
                @click="loadPreviewFolderPage(row.path, true)"
              >
                {{
                  row.loading
                    ? $t('task.torrent-preview-loading-more')
                    : $t('task.torrent-preview-load-more')
                }}
              </button>
              <template v-else>
                <button
                  type="button"
                  class="torrent-preview-name-text"
                  :disabled="isPreviewNameDisabled(row)"
                  :aria-pressed="previewNameAriaPressed(row)"
                  @click="onPreviewNameClick(row)"
                >{{ row.name }}</button>
                <span v-if="row.loading" class="torrent-preview-loading-tag">
                  {{ $t('task.torrent-preview-folder-loading') }}
                </span>
              </template>
            </span>
            <span class="torrent-preview-size">
              {{ row.type === 'file' ? formatBytes(row.length) : '' }}
            </span>
          </div>
        </div>
      </div>
    </div>
    <task-files
      v-else
      mode="ADD"
      :files="files"
      @selection-change="handleSelectionChange"
    />
    <loading-overlay :show="isBusy" :text="busyText" />
  </div>
</template>

<script lang="ts">
import { ChevronRight, File, Folder } from "@lucide/vue";
import { NONE_SELECTED_FILES, SELECTED_ALL_FILES } from "@shared/constants";
import { bytesToSize, listTorrentFiles } from "@shared/utils";
import logger from "@shared/utils/logger";
import { invoke } from "@tauri-apps/api/core";
import TaskFiles from "@/components/TaskDetail/TaskFiles.vue";
import { Checkbox } from "@/components/ui/checkbox";
import UiButton from "@/components/ui/compat/UiButton.vue";
import LoadingOverlay from "@/components/ui/LoadingOverlay.vue";

type ResolvedTorrentFile = {
	path: string;
	length: number;
	name: string;
};

type ResolvedTorrentItem = {
	path: string;
	length: number;
	name: string;
	type: "file" | "folder";
	hasChildren: boolean;
	index?: number;
	selectRanges?: string;
};

type ResolvedTorrentPayload = {
	files: ResolvedTorrentFile[];
	items: ResolvedTorrentItem[];
	fileCount: number;
	itemsTotal: number;
	nextOffset: number;
	previewDisabled: boolean;
	previewReason: string;
};

type PreviewTreeRow = {
	key: string;
	type: "file" | "folder" | "load-more";
	name: string;
	path: string;
	fullPath: string;
	depth: number;
	length: number;
	index: number;
	selectRanges: string;
	expanded: boolean;
	loading: boolean;
};

type PreviewFolderPageState = {
	nextOffset: number;
	total: number;
	hasMore: boolean;
};

type PreviewIndexRange = {
	start: number;
	end: number;
};

const PREVIEW_PAGE_SIZE = 300;

export default {
	name: "select-torrent",
	components: {
		[UiButton.name]: UiButton,
		[LoadingOverlay.name]: LoadingOverlay,
		Checkbox,
		[TaskFiles.name]: TaskFiles,
		ChevronRight,
		File,
		Folder,
	},
	props: {
		torrentPath: { type: String, required: true },
		torrentName: { type: String, default: "" },
	},
	data() {
		return {
			name: "",
			currentTorrentPath: "",
			files: [],
			previewDisabled: false,
			previewNotice: "",
			previewLoading: false,
			previewPaged: false,
			previewRootItems: [] as ResolvedTorrentItem[],
			previewChildrenByFolder: {} as Record<string, ResolvedTorrentItem[]>,
			previewPageStateByFolder: {} as Record<string, PreviewFolderPageState>,
			previewExpandedFolders: [] as string[],
			previewLoadingFolders: [] as string[],
			previewTotalCount: 0,
			previewExcludedRanges: [] as PreviewIndexRange[],
			previewSelectRangesCache: {} as Record<string, PreviewIndexRange[]>,
			parseVersion: 0,
			resolvingTorrent: false,
		};
	},
	computed: {
		torrents() {
			return [
				{
					name: this.torrentName || this.torrentPath,
					path: this.torrentPath,
				},
			];
		},
		previewTreeRows(): PreviewTreeRow[] {
			if (!this.previewPaged) {
				return [];
			}
			return this.flattenPreviewRows("", 0);
		},
		previewLoadedFileIndices(): number[] {
			const loaded = new Set<number>();
			this.previewTreeRows.forEach((row) => {
				if (row.type !== "file") {
					return;
				}
				const index = this.normalizePreviewFileIndex(row.index);
				if (index > 0) {
					loaded.add(index);
				}
			});
			return Array.from(loaded).sort((a, b) => a - b);
		},
		previewLoadedSelectionState(): boolean | "indeterminate" {
			const loaded = this.previewLoadedFileIndices;
			if (loaded.length === 0) {
				return false;
			}

			const selectedCount = loaded.reduce((count, index) => {
				return this.isPreviewFileIndexExcluded(index) ? count : count + 1;
			}, 0);
			if (selectedCount <= 0) {
				return false;
			}
			if (selectedCount >= loaded.length) {
				return true;
			}
			return "indeterminate";
		},
		previewExcludedCount() {
			return this.countPreviewRangesLength(this.previewExcludedRanges);
		},
		previewSelectedCount() {
			const total = Number(this.previewTotalCount || 0);
			if (total <= 0) {
				return 0;
			}
			return Math.max(0, total - this.previewExcludedCount);
		},
		isBusy() {
			return this.resolvingTorrent || this.previewLoading;
		},
		busyText() {
			if (this.previewLoading) {
				return this.$t("task.torrent-preview-loading");
			}
			if (this.resolvingTorrent) {
				return this.$t("task.loading-resolve-torrent");
			}
			return "";
		},
		previewExpandedFolderSet() {
			return new Set(this.previewExpandedFolders);
		},
		previewLoadingFolderSet() {
			return new Set(this.previewLoadingFolders);
		},
	},
	watch: {
		torrents: {
			immediate: true,
			async handler(fileList) {
				await this.processTorrents(fileList);
			},
		},
	},
	methods: {
		async processTorrents(fileList = []) {
			if (!Array.isArray(fileList) || fileList.length === 0) {
				this.reset();
				return;
			}

			const file = fileList[0];
			const filePath = this.extractNativePath(file);
			if (!filePath) {
				this.reset();
				this.$msg.error(this.$t("task.new-task-torrent-required"));
				return;
			}

			this.parseVersion += 1;
			const parseVersion = this.parseVersion;
			this.resolvingTorrent = true;

			try {
				const resolved = await this.resolveTorrentByPath(filePath, file.name);
				if (parseVersion !== this.parseVersion) {
					return;
				}

				this.name = file.name;
				this.currentTorrentPath = filePath;
				this.previewDisabled = !!resolved.previewDisabled;
				this.previewNotice = this.buildPreviewNotice(resolved);
				this.previewLoading = false;
				this.previewPaged = false;
				this.resetPreviewState();
				this.previewTotalCount = Number(resolved.fileCount || 0);
				this.previewExcludedRanges = [];
				this.previewSelectRangesCache = {};
				this.files = this.previewDisabled
					? []
					: listTorrentFiles(resolved.files || []);

				this.$emit("change", filePath, SELECTED_ALL_FILES);
			} catch (err: unknown) {
				logger.warn(
					"[Risuko] parse torrent failed:",
					err instanceof Error ? err.message : err,
				);
				if (parseVersion !== this.parseVersion) {
					return;
				}
				this.reset();
				this.$msg.error(this.$t("task.new-task-torrent-required"));
			} finally {
				if (parseVersion === this.parseVersion) {
					this.resolvingTorrent = false;
				}
			}
		},
		resolveTorrentByPath(
			path,
			fileName,
			options: Record<string, unknown> = {},
		): Promise<ResolvedTorrentPayload> {
			const {
				forcePreview = false,
				parentPath = "",
				offset = 0,
				limit = PREVIEW_PAGE_SIZE,
			} = options;
			return invoke<ResolvedTorrentPayload>("resolve_torrent_path", {
				path,
				fileName,
				forcePreview,
				parentPath,
				offset,
				limit,
			});
		},
		extractNativePath(file: { path?: string; raw?: { path?: string } } = {}) {
			const path = `${file?.path || file?.raw?.path || ""}`.trim();
			if (!path || !/\.torrent$/i.test(path)) {
				return "";
			}
			return path;
		},
		formatBytes(value: string | number) {
			return bytesToSize(value);
		},
		buildPreviewNotice(payload: ResolvedTorrentPayload) {
			if (!payload.previewDisabled) {
				return "";
			}

			const reason = `${payload.previewReason || ""}`.toLowerCase();
			if (reason === "count" && Number(payload.fileCount) > 0) {
				return this.$t("task.torrent-preview-disabled-count", {
					count: payload.fileCount,
				});
			}

			return this.$t("task.torrent-preview-disabled-size");
		},
		resetPreviewState() {
			this.previewRootItems = [];
			this.previewChildrenByFolder = {};
			this.previewPageStateByFolder = {};
			this.previewExpandedFolders = [];
			this.previewLoadingFolders = [];
		},
		isPreviewFolderLoaded(path = "") {
			return Object.hasOwn(this.previewPageStateByFolder, path);
		},
		isPreviewFolderExpanded(path = "") {
			return this.previewExpandedFolderSet.has(path);
		},
		isPreviewFolderLoading(path = "") {
			return this.previewLoadingFolderSet.has(path);
		},
		getPreviewFolderPageState(path = ""): PreviewFolderPageState {
			const state = this.previewPageStateByFolder[path];
			if (!state) {
				return {
					nextOffset: 0,
					total: 0,
					hasMore: false,
				};
			}
			return state;
		},
		updatePreviewFolderPageState(path = "", payload: ResolvedTorrentPayload) {
			const total = Math.max(0, Number(payload?.itemsTotal || 0));
			const nextOffset = Math.max(0, Number(payload?.nextOffset || 0));
			this.previewPageStateByFolder = {
				...this.previewPageStateByFolder,
				[path]: {
					total,
					nextOffset,
					hasMore: nextOffset < total,
				},
			};
		},
		hasPreviewFolderMore(path = "") {
			return this.getPreviewFolderPageState(path).hasMore;
		},
		getPreviewChildren(parentPath = ""): ResolvedTorrentItem[] {
			if (!parentPath) {
				return this.previewRootItems;
			}
			return this.previewChildrenByFolder[parentPath] || [];
		},
		flattenPreviewRows(parentPath = "", depth = 0): PreviewTreeRow[] {
			const rows: PreviewTreeRow[] = [];
			const children = this.getPreviewChildren(parentPath);
			children.forEach((item, index) => {
				const isFolder = item.type === "folder";
				const expanded = isFolder && this.isPreviewFolderExpanded(item.path);
				const loading = isFolder && this.isPreviewFolderLoading(item.path);
				rows.push({
					key: `${item.type}:${item.path}:${index}`,
					type: item.type,
					name: item.name,
					path: item.path,
					fullPath: item.path,
					depth,
					length: Number(item.length) || 0,
					index: this.normalizePreviewFileIndex(item.index),
					selectRanges: `${item.selectRanges || ""}`,
					expanded,
					loading,
				});
				if (isFolder && expanded && this.isPreviewFolderLoaded(item.path)) {
					rows.push(...this.flattenPreviewRows(item.path, depth + 1));
				}
			});
			if (this.hasPreviewFolderMore(parentPath)) {
				const pageState = this.getPreviewFolderPageState(parentPath);
				rows.push({
					key: `load-more:${parentPath || "__root__"}:${pageState.nextOffset}`,
					type: "load-more",
					name: "",
					path: parentPath,
					fullPath: parentPath,
					depth,
					length: 0,
					index: 0,
					selectRanges: "",
					expanded: false,
					loading: this.isPreviewFolderLoading(parentPath),
				});
			}
			return rows;
		},
		normalizePreviewFileIndex(value: unknown) {
			const index = Number(value);
			if (!Number.isFinite(index)) {
				return 0;
			}
			const normalized = Math.trunc(index);
			return normalized > 0 ? normalized : 0;
		},
		normalizePreviewRange(range: PreviewIndexRange): PreviewIndexRange | null {
			const totalCount = Number(this.previewTotalCount || 0);
			const start = this.normalizePreviewFileIndex(range?.start);
			const end = this.normalizePreviewFileIndex(range?.end);
			if (start <= 0 || end <= 0) {
				return null;
			}
			const normalizedStart = Math.min(start, end);
			const normalizedEnd = Math.max(start, end);
			if (totalCount > 0 && normalizedStart > totalCount) {
				return null;
			}
			return {
				start: normalizedStart,
				end:
					totalCount > 0 ? Math.min(normalizedEnd, totalCount) : normalizedEnd,
			};
		},
		normalizePreviewExcludedRanges(ranges: PreviewIndexRange[] = []) {
			const normalized = ranges
				.map((range) => this.normalizePreviewRange(range))
				.filter((range): range is PreviewIndexRange => !!range)
				.sort((a, b) => {
					if (a.start !== b.start) {
						return a.start - b.start;
					}
					return a.end - b.end;
				});

			if (normalized.length === 0) {
				return [];
			}

			const merged: PreviewIndexRange[] = [normalized[0]];
			normalized.slice(1).forEach((range) => {
				const current = merged[merged.length - 1];
				if (range.start <= current.end + 1) {
					current.end = Math.max(current.end, range.end);
					return;
				}
				merged.push({ start: range.start, end: range.end });
			});
			return merged;
		},
		subtractPreviewRanges(
			sourceRanges: PreviewIndexRange[] = [],
			subtractRanges: PreviewIndexRange[] = [],
		): PreviewIndexRange[] {
			if (sourceRanges.length === 0 || subtractRanges.length === 0) {
				return sourceRanges;
			}

			const result: PreviewIndexRange[] = [];
			let j = 0;

			sourceRanges.forEach((range) => {
				let currentStart = range.start;
				const currentEnd = range.end;

				while (
					j < subtractRanges.length &&
					subtractRanges[j].end < currentStart
				) {
					j += 1;
				}

				let k = j;
				while (
					k < subtractRanges.length &&
					subtractRanges[k].start <= currentEnd
				) {
					const cut = subtractRanges[k];
					if (cut.start > currentStart) {
						result.push({
							start: currentStart,
							end: Math.min(currentEnd, cut.start - 1),
						});
					}
					if (cut.end >= currentEnd) {
						currentStart = currentEnd + 1;
						break;
					}
					currentStart = Math.max(currentStart, cut.end + 1);
					k += 1;
				}

				if (currentStart <= currentEnd) {
					result.push({ start: currentStart, end: currentEnd });
				}
			});

			return result;
		},
		buildPreviewRangesFromIndices(indices: number[] = []) {
			const totalCount = Number(this.previewTotalCount || 0);
			const normalized = Array.from(new Set(indices))
				.map((index) => this.normalizePreviewFileIndex(index))
				.filter((index) => {
					if (index <= 0) {
						return false;
					}
					if (totalCount <= 0) {
						return true;
					}
					return index <= totalCount;
				})
				.sort((a, b) => a - b);

			if (normalized.length === 0) {
				return [];
			}

			const ranges: PreviewIndexRange[] = [];
			let start = normalized[0];
			let end = normalized[0];
			normalized.slice(1).forEach((index) => {
				if (index <= end + 1) {
					end = index;
					return;
				}
				ranges.push({ start, end });
				start = index;
				end = index;
			});
			ranges.push({ start, end });
			return ranges;
		},
		parsePreviewSelectRanges(value = "") {
			const text = `${value || ""}`.trim();
			if (!text) {
				return [];
			}
			const ranges: PreviewIndexRange[] = [];
			text.split(",").forEach((part) => {
				const token = part.trim();
				if (!token) {
					return;
				}
				const [startText, endText] = token.split("-", 2);
				const start = this.normalizePreviewFileIndex(startText);
				const end = this.normalizePreviewFileIndex(endText ?? startText);
				if (start <= 0 || end <= 0) {
					return;
				}
				ranges.push({
					start: Math.min(start, end),
					end: Math.max(start, end),
				});
			});
			return this.normalizePreviewExcludedRanges(ranges);
		},
		isPreviewFileIndexExcluded(index: number) {
			const normalized = this.normalizePreviewFileIndex(index);
			if (normalized <= 0) {
				return false;
			}
			return this.previewExcludedRanges.some(
				(range) => normalized >= range.start && normalized <= range.end,
			);
		},
		getPreviewRowSelectableRanges(row: PreviewTreeRow): PreviewIndexRange[] {
			if (!row) {
				return [];
			}
			if (row.type === "file") {
				const index = this.normalizePreviewFileIndex(row.index);
				return index > 0 ? [{ start: index, end: index }] : [];
			}
			if (row.type === "folder") {
				const cacheKey = `${row.path}::${row.selectRanges}`;
				const cached = this.previewSelectRangesCache[cacheKey];
				if (cached) {
					return cached;
				}
				const parsed = this.parsePreviewSelectRanges(row.selectRanges);
				this.previewSelectRangesCache = {
					...this.previewSelectRangesCache,
					[cacheKey]: parsed,
				};
				return parsed;
			}
			return [];
		},
		hasPreviewFolderSelectableRange(row: PreviewTreeRow) {
			if (row?.type !== "folder") {
				return false;
			}
			return this.getPreviewRowSelectableRanges(row).length > 0;
		},
		countPreviewRangesLength(ranges: PreviewIndexRange[] = []) {
			return ranges.reduce(
				(sum, range) => sum + Math.max(0, range.end - range.start + 1),
				0,
			);
		},
		countPreviewExcludedInRanges(ranges: PreviewIndexRange[] = []) {
			if (ranges.length === 0 || this.previewExcludedRanges.length === 0) {
				return 0;
			}

			let excludedIndex = 0;
			let total = 0;
			ranges.forEach((range) => {
				while (
					excludedIndex < this.previewExcludedRanges.length &&
					this.previewExcludedRanges[excludedIndex].end < range.start
				) {
					excludedIndex += 1;
				}

				let cursor = excludedIndex;
				while (
					cursor < this.previewExcludedRanges.length &&
					this.previewExcludedRanges[cursor].start <= range.end
				) {
					const overlapStart = Math.max(
						range.start,
						this.previewExcludedRanges[cursor].start,
					);
					const overlapEnd = Math.min(
						range.end,
						this.previewExcludedRanges[cursor].end,
					);
					if (overlapStart <= overlapEnd) {
						total += overlapEnd - overlapStart + 1;
					}
					cursor += 1;
				}
			});
			return total;
		},
		previewFolderSelectionState(
			row: PreviewTreeRow,
		): boolean | "indeterminate" {
			if (row?.type !== "folder") {
				return false;
			}
			const ranges = this.getPreviewRowSelectableRanges(row);
			if (ranges.length === 0) {
				return false;
			}
			const total = this.countPreviewRangesLength(ranges);
			if (total <= 0) {
				return false;
			}
			const excluded = this.countPreviewExcludedInRanges(ranges);
			if (excluded <= 0) {
				return true;
			}
			if (excluded >= total) {
				return false;
			}
			return "indeterminate";
		},
		runPreviewSelectionUpdate(
			ranges: PreviewIndexRange[] = [],
			checked: boolean | "indeterminate",
		): void {
			const normalizedRanges = this.normalizePreviewExcludedRanges(ranges);
			if (normalizedRanges.length === 0) {
				return;
			}
			const currentExcluded = this.previewExcludedRanges;
			this.previewExcludedRanges =
				checked === true
					? this.subtractPreviewRanges(currentExcluded, normalizedRanges)
					: this.normalizePreviewExcludedRanges([
							...currentExcluded,
							...normalizedRanges,
						]);
			this.emitPreviewSelectionChange();
		},
		isPreviewNameDisabled(row: PreviewTreeRow): boolean {
			return (
				row.type === "folder" &&
				(row.loading || !this.hasPreviewFolderSelectableRange(row))
			);
		},
		previewNameAriaPressed(row: PreviewTreeRow): boolean | "mixed" {
			if (row.type === "file") {
				return this.isPreviewFileSelected(row);
			}
			if (row.type === "folder") {
				const state = this.previewFolderSelectionState(row);
				if (state === "indeterminate") {
					return "mixed";
				}
				return state === true;
			}
			return false;
		},
		onPreviewNameClick(row: PreviewTreeRow) {
			if (row.type === "file") {
				this.togglePreviewFileSelection(row, !this.isPreviewFileSelected(row));
				return;
			}
			if (row.type === "folder") {
				if (this.isPreviewNameDisabled(row)) {
					return;
				}
				this.togglePreviewFolderSelection(
					row,
					this.previewFolderSelectionState(row) !== true,
				);
			}
		},
		togglePreviewFolderSelection(
			row: PreviewTreeRow,
			checked: boolean | "indeterminate",
		) {
			if (row?.type !== "folder") {
				return;
			}
			const ranges = this.getPreviewRowSelectableRanges(row);
			if (ranges.length === 0) {
				return;
			}
			this.runPreviewSelectionUpdate(ranges, checked);
		},
		isPreviewFileSelected(row: PreviewTreeRow) {
			if (row?.type !== "file") {
				return false;
			}
			const index = this.normalizePreviewFileIndex(row.index);
			if (index <= 0) {
				return true;
			}
			return !this.isPreviewFileIndexExcluded(index);
		},
		buildSelectFileRangesFromExcluded() {
			const totalCount = Number(this.previewTotalCount || 0);
			if (totalCount <= 0) {
				return NONE_SELECTED_FILES;
			}

			const excluded = this.normalizePreviewExcludedRanges(
				this.previewExcludedRanges,
			);
			if (excluded.length === 0) {
				return SELECTED_ALL_FILES;
			}
			const excludedCount = this.countPreviewRangesLength(excluded);
			if (excludedCount >= totalCount) {
				return NONE_SELECTED_FILES;
			}

			const selected: string[] = [];
			let start = 1;
			excluded.forEach((range) => {
				if (range.start > start) {
					const end = range.start - 1;
					selected.push(start === end ? `${start}` : `${start}-${end}`);
				}
				start = Math.max(start, range.end + 1);
			});
			if (start <= totalCount) {
				selected.push(
					start === totalCount ? `${start}` : `${start}-${totalCount}`,
				);
			}
			return selected.join(",") || NONE_SELECTED_FILES;
		},
		emitPreviewSelectionChange() {
			if (!this.previewPaged) {
				return;
			}
			this.$emit(
				"change",
				this.currentTorrentPath,
				this.buildSelectFileRangesFromExcluded(),
			);
		},
		togglePreviewFileSelection(
			row: PreviewTreeRow,
			checked: boolean | "indeterminate",
		) {
			if (row?.type !== "file") {
				return;
			}
			const index = this.normalizePreviewFileIndex(row.index);
			if (index <= 0) {
				return;
			}
			this.runPreviewSelectionUpdate([{ start: index, end: index }], checked);
		},
		toggleLoadedPreviewSelection(checked: boolean | "indeterminate") {
			const loaded = this.previewLoadedFileIndices;
			if (loaded.length === 0) {
				return;
			}
			const loadedRanges = this.buildPreviewRangesFromIndices(loaded);
			this.runPreviewSelectionUpdate(loadedRanges, checked);
		},
		mergePreviewItems(
			existing: ResolvedTorrentItem[] = [],
			incoming: ResolvedTorrentItem[] = [],
		) {
			const merged = [...existing];
			const known = new Set(merged.map((item) => `${item.type}:${item.path}`));
			incoming.forEach((item) => {
				const key = `${item.type}:${item.path}`;
				if (known.has(key)) {
					return;
				}
				known.add(key);
				merged.push(item);
			});
			return merged;
		},
		async loadPreviewFolderPage(parentPath = "", append = false) {
			const normalizedParentPath = `${parentPath || ""}`.trim();
			if (this.isPreviewFolderLoading(normalizedParentPath)) {
				return;
			}
			if (!this.currentTorrentPath) {
				return;
			}
			if (!append && this.isPreviewFolderLoaded(normalizedParentPath)) {
				return;
			}
			if (append && !this.hasPreviewFolderMore(normalizedParentPath)) {
				return;
			}

			const pageState = this.getPreviewFolderPageState(normalizedParentPath);
			const offset = append ? pageState.nextOffset : 0;
			this.previewLoadingFolders = [
				...this.previewLoadingFolders,
				normalizedParentPath,
			];
			const parseVersion = this.parseVersion;
			try {
				const resolved = await this.resolveTorrentByPath(
					this.currentTorrentPath,
					this.name,
					{
						forcePreview: true,
						parentPath: normalizedParentPath,
						offset,
						limit: PREVIEW_PAGE_SIZE,
					},
				);
				if (parseVersion !== this.parseVersion) {
					return;
				}
				const incomingItems = resolved.items || [];
				if (!normalizedParentPath) {
					this.previewRootItems = append
						? this.mergePreviewItems(this.previewRootItems, incomingItems)
						: incomingItems;
				} else {
					const existingItems =
						this.previewChildrenByFolder[normalizedParentPath] || [];
					this.previewChildrenByFolder = {
						...this.previewChildrenByFolder,
						[normalizedParentPath]: append
							? this.mergePreviewItems(existingItems, incomingItems)
							: incomingItems,
					};
				}
				this.updatePreviewFolderPageState(normalizedParentPath, resolved);
				this.previewTotalCount = Number(
					resolved.fileCount || this.previewTotalCount,
				);
			} catch (err: unknown) {
				logger.warn(
					"[Risuko] load folder preview failed:",
					err instanceof Error ? err.message : err,
				);
				this.$msg.error(this.$t("task.new-task-torrent-required"));
			} finally {
				this.previewLoadingFolders = this.previewLoadingFolders.filter(
					(value) => value !== normalizedParentPath,
				);
			}
		},
		dropPreviewFolderCache(path = "") {
			const folderPath = `${path || ""}`.trim();
			if (!folderPath) {
				return;
			}
			const descendantPrefix = `${folderPath}/`;
			const nextChildren: Record<string, ResolvedTorrentItem[]> = {};
			Object.keys(this.previewChildrenByFolder).forEach((key) => {
				if (key !== folderPath && !key.startsWith(descendantPrefix)) {
					nextChildren[key] = this.previewChildrenByFolder[key];
				}
			});
			this.previewChildrenByFolder = nextChildren;

			const nextPageState: Record<string, PreviewFolderPageState> = {};
			Object.keys(this.previewPageStateByFolder).forEach((key) => {
				if (!key || (key !== folderPath && !key.startsWith(descendantPrefix))) {
					nextPageState[key] = this.previewPageStateByFolder[key];
				}
			});
			this.previewPageStateByFolder = nextPageState;
		},
		async togglePreviewFolder(path = "") {
			const folderPath = `${path || ""}`.trim();
			if (!folderPath) {
				return;
			}
			const descendantPrefix = `${folderPath}/`;
			if (this.isPreviewFolderExpanded(folderPath)) {
				this.previewExpandedFolders = this.previewExpandedFolders.filter(
					(value) =>
						value !== folderPath && !value.startsWith(descendantPrefix),
				);
				this.previewLoadingFolders = this.previewLoadingFolders.filter(
					(value) =>
						value !== folderPath && !value.startsWith(descendantPrefix),
				);
				this.dropPreviewFolderCache(folderPath);
				return;
			}

			const keptExpanded = this.previewExpandedFolders.filter(
				(value) => value === folderPath || !value.startsWith(descendantPrefix),
			);
			this.previewExpandedFolders = keptExpanded.includes(folderPath)
				? keptExpanded
				: [...keptExpanded, folderPath];
			await this.loadPreviewFolderPage(folderPath, false);
		},
		async handlePreviewAnyway() {
			if (
				!this.previewDisabled ||
				this.previewLoading ||
				!this.currentTorrentPath
			) {
				return;
			}

			this.previewLoading = true;
			const parseVersion = this.parseVersion;
			try {
				const resolved = await this.resolveTorrentByPath(
					this.currentTorrentPath,
					this.name,
					{
						forcePreview: true,
						parentPath: "",
						offset: 0,
						limit: PREVIEW_PAGE_SIZE,
					},
				);
				if (parseVersion !== this.parseVersion) {
					return;
				}
				this.previewDisabled = !!resolved.previewDisabled;
				this.previewNotice = this.buildPreviewNotice(resolved);
				this.previewPaged = !this.previewDisabled;
				this.resetPreviewState();
				this.previewRootItems = this.previewDisabled
					? []
					: resolved.items || [];
				this.previewTotalCount = Number(resolved.fileCount || 0);
				if (!this.previewDisabled) {
					this.updatePreviewFolderPageState("", resolved);
				}
				this.previewExcludedRanges = [];
				this.previewSelectRangesCache = {};
				this.files = [];
				this.emitPreviewSelectionChange();
			} catch (err: unknown) {
				logger.warn(
					"[Risuko] force preview torrent failed:",
					err instanceof Error ? err.message : err,
				);
				this.$msg.error(this.$t("task.new-task-torrent-required"));
			} finally {
				this.previewLoading = false;
			}
		},
		reset(emitChange = true) {
			this.name = "";
			this.currentTorrentPath = "";
			this.files = [];
			this.resolvingTorrent = false;
			this.previewDisabled = false;
			this.previewNotice = "";
			this.previewLoading = false;
			this.previewPaged = false;
			this.resetPreviewState();
			this.previewTotalCount = 0;
			this.previewExcludedRanges = [];
			this.previewSelectRangesCache = {};
			this.parseVersion += 1;
			if (emitChange) {
				this.$emit("change", "", NONE_SELECTED_FILES);
			}
		},
		handleSelectionChange(val) {
			const { currentTorrentPath } = this;
			this.$emit("change", currentTorrentPath, val);
		},
	},
};
</script>
