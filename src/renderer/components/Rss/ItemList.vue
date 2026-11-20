<template>
  <div class="rss-item-list-wrap">
    <div v-if="items.length === 0" class="rss-items-empty">
      <p>{{ $t('rss.no-items') }}</p>
    </div>
    <template v-else>
      <div class="rss-item-list">
        <ul :class="['rss-items', densityMode === 'compact' ? 'rss-items--compact' : 'rss-items--comfortable']">
          <motion-enter
            v-for="(item, index) in items"
            :key="`${item.feed_id}:${item.id}`"
            tag="li"
            preset="fadeInUp"
            :duration="0.4"
            :delay="itemDelay(index)"
            class="rss-item"
            :class="{
              'rss-item--read': item.is_read,
              'rss-item--downloaded': item.is_downloaded,
              'rss-item--selected': isSelected(item.id),
            }"
          >
            <div class="rss-item-header">
              <Checkbox
                class="rss-item-checkbox"
                :model-value="isSelected(item.id)"
                @update:model-value="() => toggleSelect(item.id)"
                @click.stop
              />
              <button
                type="button"
                class="rss-item-info"
                :title="`${$t('rss.open-link')}: ${item.title}`"
                :aria-label="`${$t('rss.open-link')}: ${item.title}`"
                @click="openPreview(item)"
              >
                <span class="rss-item-title" :title="item.title">
                  {{ item.title }}
                </span>
                <span class="rss-item-meta">
                  <span v-if="item.pub_date" class="rss-item-date">
                    {{ formatDate(item.pub_date) }}
                  </span>
                  <span v-if="item.enclosure_type" class="rss-item-type">
                    {{ item.enclosure_type }}
                  </span>
                  <span v-if="item.enclosure_length" class="rss-item-size">
                    {{ formatSize(item.enclosure_length) }}
                  </span>
                  <span
                    v-for="tag in qualityTagsFor(item)"
                    :key="`q-${item.id}-${tag}`"
                    class="rss-item-quality"
                  >{{ tag }}</span>
                  <span
                    v-if="item.matched_rule_id"
                    class="rss-item-rule"
                    :title="ruleNameFor(item.matched_rule_id)"
                  >{{ ruleNameFor(item.matched_rule_id) }}</span>
                </span>
              </button>
              <div class="rss-item-actions">
                <Button
                  v-if="item.is_downloaded"
                  size="icon-sm"
                  variant="ghost"
                  class="rss-item-badge--downloaded"
                  :title="$t('rss.view-download')"
                  :aria-label="$t('rss.view-download')"
                  @click.stop="openViewer(item)"
                >
                  <Eye :size="14" />
                </Button>
                <Button
                  v-else-if="hasDownloadUrl(item)"
                  size="icon-sm"
                  variant="ghost"
                  :title="$t('rss.download')"
                  :aria-label="$t('rss.download')"
                  @click.stop="handleDownload(item)"
                >
                  <Download :size="14" />
                </Button>
                <Button
                  v-if="item.is_downloaded"
                  size="icon-sm"
                  variant="ghost"
                  class="rss-item-delete"
                  :title="$t('rss.delete-item')"
                  :aria-label="$t('rss.delete-item')"
                  @click.stop="handleClearDownload(item)"
                >
                  <Trash2 :size="14" />
                </Button>
              </div>
            </div>
          </motion-enter>
        </ul>
      </div>
      <footer class="task-pagination">
        <button
          class="task-pagination-btn"
          type="button"
          :disabled="currentPage <= 1"
          @click="onPrevPage"
        >
          {{ $t('task.pagination-prev') }}
        </button>
        <span class="task-pagination-text">{{ currentPage }} / {{ totalPages }}</span>
        <button
          class="task-pagination-btn"
          type="button"
          :disabled="currentPage >= totalPages"
          @click="onNextPage"
        >
          {{ $t('task.pagination-next') }}
        </button>
      </footer>
    </template>
    <Dialog :open="!!previewItem" @update:open="closePreview">
      <DialogContent class="rss-preview-dialog" :show-close-button="true">
        <DialogHeader>
          <DialogTitle class="rss-preview-title">{{ previewItem?.title }}</DialogTitle>
        </DialogHeader>
        <div v-if="previewItem" class="rss-preview-body">
          <div class="rss-preview-meta">
            <span v-if="previewItem.pub_date" class="rss-preview-date">
              {{ formatDate(previewItem.pub_date) }}
            </span>
            <span v-if="previewItem.enclosure_type" class="rss-preview-type">
              {{ previewItem.enclosure_type }}
            </span>
            <span v-if="previewItem.enclosure_length" class="rss-preview-size">
              {{ formatSize(previewItem.enclosure_length) }}
            </span>
          </div>
          <div class="rss-preview-content" v-html="sanitize(previewItem.description)" />
        </div>
        <DialogFooter>
          <span v-if="previewItem?.is_downloaded" class="rss-preview-downloaded">
            <CheckCircle2 :size="14" />
            {{ $t('rss.downloaded') }}
          </span>
          <Button
            v-if="previewItem?.is_downloaded"
            variant="outline"
            size="sm"
            @click="openViewerFromPreview()"
          >
            <Eye :size="14" />
            {{ $t('rss.view-download') }}
          </Button>
          <Button
            v-if="previewItem?.link"
            variant="outline"
            size="sm"
            @click="openLink(previewItem.link)"
          >
            <ExternalLink :size="14" />
            {{ $t('rss.open-link') }}
          </Button>
          <Button
            v-if="previewItem && hasDownloadUrl(previewItem) && !previewItem.is_downloaded"
            size="sm"
            @click="handleDownload(previewItem)"
          >
            <Download :size="14" />
            {{ $t('rss.download') }}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
    <Dialog :open="!!viewerItem" @update:open="closeViewer">
      <DialogContent class="rss-viewer-dialog sm:max-w-[96vw] h-[94vh] flex flex-col" :show-close-button="true">
        <DialogHeader>
          <DialogTitle class="rss-preview-title">{{ viewerItem?.title }}</DialogTitle>
        </DialogHeader>
        <div class="rss-viewer-body">
          <div v-if="viewerLoading" class="rss-viewer-loading">
            {{ $t('rss.loading') }}
          </div>
          <div v-else-if="viewerError" class="rss-viewer-error">
            {{ viewerError }}
          </div>
          <div v-else-if="viewerExternalPath" class="rss-viewer-external">
            <p>{{ $t('rss.binary-file') }}</p>
            <p class="rss-viewer-path">{{ viewerExternalPath }}</p>
            <Button @click="openFile(viewerExternalPath)">
              {{ $t('rss.open-externally') }}
            </Button>
          </div>
	          <iframe
	            v-else
	            ref="viewerFrame"
	            class="rss-viewer-frame"
	            sandbox="allow-popups"
	            :srcdoc="viewerContent"
	          />
        </div>
      </DialogContent>
    </Dialog>
  </div>
</template>

<script lang="ts">
import { CheckCircle2, Download, ExternalLink, Eye, Trash2 } from "@lucide/vue";
import type { RssItem } from "@shared/types/rss";
import { bytesToSize } from "@shared/utils";
import api from "@/api";
import { Button } from "@/components/ui/button";
import { Checkbox } from "@/components/ui/checkbox";
import {
	Dialog,
	DialogContent,
	DialogFooter,
	DialogHeader,
	DialogTitle,
} from "@/components/ui/dialog";
import { useRssStore } from "@/store/rss";
import { openExternalUrl } from "@/utils/external";
import { openItem } from "@/utils/native";

function isHtmlPath(path: string): boolean {
	const lower = path.toLowerCase();
	return (
		lower.endsWith(".html") ||
		lower.endsWith(".htm") ||
		lower.endsWith(".xhtml")
	);
}

function injectBaseTag(html: string, baseUrl: string, isDark: boolean): string {
	const bg = isDark ? "#343434" : "#ffffff";
	const fg = isDark ? "#eeeeee" : "#1a1a1a";
	const link = isDark ? "#7b9bff" : "#1a5ccc";
	const border = isDark ? "#555" : "#ddd";
	const codeBg = isDark ? "#2a2a2a" : "#f5f5f5";
	const muted = isDark ? "#bbb" : "#555";
	const readerStyle = `<style data-risuko-reader>
:root{color-scheme:${isDark ? "dark" : "light"}}
*:not(img):not(video):not(picture):not(svg):not(canvas){background-color:${bg}!important;color:${fg}!important;border-color:${border}!important}
html,body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif!important;line-height:1.7!important;font-size:15px!important;word-wrap:break-word}
body{max-width:960px;margin:0 auto!important;padding:16px!important}
a,a *{color:${link}!important}
a:visited,a:visited *{color:${link}!important}
img,video,picture{max-width:100%!important;height:auto!important;border-radius:4px}
pre,code,pre *,code *{background-color:${codeBg}!important;color:${fg}!important;border-radius:4px;overflow-x:auto}
pre{padding:12px!important}
code{padding:2px 4px!important;font-size:0.9em}
table{border-collapse:collapse!important;width:100%!important}
th,td{border:1px solid ${border}!important;padding:8px!important;text-align:left}
th,th *{background-color:${codeBg}!important}
blockquote,blockquote *{border-color:${border}!important;color:${muted}!important}
blockquote{border-left:3px solid ${border}!important;margin-left:0!important;padding-left:16px!important}
hr{border:none!important;border-top:1px solid ${border}!important}
*{max-width:100%!important;box-sizing:border-box}
</style>`;
	const baseTag = baseUrl ? `<base href="${baseUrl}" target="_blank">` : "";

	let result = html;
	if (baseTag) {
		const headIdx = result.indexOf("<head");
		if (headIdx !== -1) {
			const closeAngle = result.indexOf(">", headIdx);
			if (closeAngle !== -1) {
				result = `${result.slice(0, closeAngle + 1)}${baseTag}${result.slice(closeAngle + 1)}`;
			}
		} else {
			result = `${baseTag}${result}`;
		}
	}

	const bodyCloseIdx = result.lastIndexOf("</body>");
	if (bodyCloseIdx !== -1) {
		result = `${result.slice(0, bodyCloseIdx)}${readerStyle}${result.slice(bodyCloseIdx)}`;
	} else {
		result = `${result}${readerStyle}`;
	}
	return result;
}

const ALLOWED_TAGS = new Set([
	"p",
	"br",
	"b",
	"strong",
	"i",
	"em",
	"a",
	"ul",
	"ol",
	"li",
	"blockquote",
	"code",
	"pre",
	"h1",
	"h2",
	"h3",
	"h4",
	"h5",
	"h6",
	"img",
	"hr",
]);

function sanitizeHtml(html: string): string {
	const parser = new DOMParser();
	const doc = parser.parseFromString(html, "text/html");

	function clean(node: Node): Node | null {
		if (node.nodeType === Node.TEXT_NODE) {
			return node.cloneNode();
		}
		if (node.nodeType !== Node.ELEMENT_NODE) {
			return null;
		}
		const el = node as Element;
		const tag = el.tagName.toLowerCase();
		if (!ALLOWED_TAGS.has(tag)) {
			const fragment = document.createDocumentFragment();
			for (const child of Array.from(el.childNodes)) {
				const cleaned = clean(child);
				if (cleaned) {
					fragment.appendChild(cleaned);
				}
			}
			return fragment;
		}
		const newEl = document.createElement(tag);
		if (tag === "a") {
			const href = el.getAttribute("href");
			if (href && (href.startsWith("http://") || href.startsWith("https://"))) {
				newEl.setAttribute("href", href);
				newEl.setAttribute("target", "_blank");
				newEl.setAttribute("rel", "noopener noreferrer");
			}
		}
		if (tag === "img") {
			const src = el.getAttribute("src");
			if (src && (src.startsWith("http://") || src.startsWith("https://"))) {
				newEl.setAttribute("src", src);
				const alt = el.getAttribute("alt");
				if (alt) {
					newEl.setAttribute("alt", alt);
				}
			}
		}
		for (const child of Array.from(el.childNodes)) {
			const cleaned = clean(child);
			if (cleaned) {
				newEl.appendChild(cleaned);
			}
		}
		return newEl;
	}

	const wrapper = document.createElement("div");
	for (const child of Array.from(doc.body.childNodes)) {
		const cleaned = clean(child);
		if (cleaned) {
			wrapper.appendChild(cleaned);
		}
	}
	return wrapper.innerHTML;
}

export default {
	name: "rss-item-list",
	components: {
		Button,
		CheckCircle2,
		Checkbox,
		Dialog,
		DialogContent,
		DialogFooter,
		DialogHeader,
		DialogTitle,
		Download,
		ExternalLink,
		Eye,
		Trash2,
	},
	data() {
		return {
			previewItem: null as RssItem | null,
			viewerItem: null as RssItem | null,
			viewerContent: "",
			viewerLoading: false,
			viewerError: "",
			viewerExternalPath: "",
		};
	},
	computed: {
		items(): RssItem[] {
			return useRssStore().paginatedItems;
		},
		selectedIdSet(): Set<string> {
			return new Set(useRssStore().selectedItemIds);
		},
		currentPage(): number {
			return useRssStore().currentPage;
		},
		totalPages(): number {
			return useRssStore().totalPages;
		},
		densityMode(): "compact" | "comfortable" {
			return useRssStore().densityMode;
		},
		ruleMap(): Record<string, string> {
			const map: Record<string, string> = {};
			for (const rule of useRssStore().rules) {
				map[rule.id] = rule.name;
			}
			return map;
		},
	},
	methods: {
		itemDelay(index: number): number {
			const MAX_DELAY = 0.6;
			return Math.min((index + 1) * 0.03, MAX_DELAY);
		},
		isSelected(itemId: string): boolean {
			return this.selectedIdSet.has(itemId);
		},
		qualityTagsFor(item: RssItem): string[] {
			return item.parsed_meta?.quality_tags?.slice(0, 3) ?? [];
		},
		ruleNameFor(ruleId: string | null | undefined): string {
			if (!ruleId) {
				return "";
			}
			return this.ruleMap[ruleId] ?? "";
		},
		toggleSelect(itemId: string) {
			useRssStore().toggleItemSelection(itemId);
		},
		onPrevPage() {
			useRssStore().changePage(this.currentPage - 1);
		},
		onNextPage() {
			useRssStore().changePage(this.currentPage + 1);
		},
		openPreview(item: RssItem) {
			this.previewItem = item;
			void useRssStore().markItemRead(item.feed_id, item.id);
		},
		closePreview() {
			this.previewItem = null;
		},
		hasDownloadUrl(item: RssItem): boolean {
			return !!(item.enclosure_url || item.link);
		},
		async handleDownload(item: RssItem) {
			await useRssStore().downloadItem(item.feed_id, item.id);
		},
		async handleClearDownload(item: RssItem) {
			await useRssStore().clearItemDownload(item.feed_id, item.id);
		},
		async openViewer(item: RssItem) {
			void useRssStore().markItemRead(item.feed_id, item.id);
			this.viewerItem = item;
			this.viewerContent = "";
			this.viewerError = "";
			this.viewerExternalPath = "";
			this.viewerLoading = true;
			try {
				const path = item.download_path ?? "";
				if (path && !isHtmlPath(path)) {
					this.viewerExternalPath = path;
					return;
				}
				const raw = await api.readRssDownload(item.feed_id, item.id);
				const baseUrl = item.link || item.enclosure_url || "";
				const isDark = document.documentElement.classList.contains("dark");
				this.viewerContent = injectBaseTag(sanitizeHtml(raw), baseUrl, isDark);
			} catch (e: unknown) {
				this.viewerError = e instanceof Error ? e.message : String(e);
			} finally {
				this.viewerLoading = false;
			}
		},
		openViewerFromPreview() {
			if (!this.previewItem) {
				return;
			}
			const item = this.previewItem;
			this.previewItem = null;
			this.openViewer(item);
		},
		closeViewer() {
			this.viewerItem = null;
			this.viewerContent = "";
			this.viewerError = "";
			this.viewerExternalPath = "";
		},
		async openLink(url: string) {
			if (!(await openExternalUrl(url))) {
				this.$msg.error(this.$t("app.open-url-failed"));
			}
		},
		async openFile(path: string) {
			try {
				if (await openItem(path)) {
					this.$msg.error(this.$t("task.file-not-exist"));
				}
			} catch (err) {
				this.$msg.error(`${err}`);
			}
		},
		formatDate(timestamp: number): string {
			return new Date(timestamp * 1000).toLocaleDateString(undefined, {
				month: "short",
				day: "numeric",
				hour: "2-digit",
				minute: "2-digit",
			});
		},
		formatSize(bytes: number): string {
			return bytesToSize(bytes);
		},
		sanitize(html: string): string {
			return sanitizeHtml(html);
		},
	},
};
</script>

<style scoped>
.rss-item-list-wrap {
  display: flex;
  flex-direction: column;
  height: 100%;
}

.rss-items-empty {
  display: flex;
  align-items: center;
  justify-content: center;
  height: 100%;
  color: var(--text-3);
  font-size: 14px;
}

.rss-item-list {
  flex: 1;
  overflow-y: auto;
  padding: 0 16px 64px;
  box-sizing: border-box;
}

.rss-items {
  list-style: none;
  padding: 0;
  margin: 0;
}

.rss-item {
  border-bottom: 1px solid var(--border);
}

.rss-item--read {
  opacity: 0.7;
}

.rss-item--selected {
  background: var(--primary-soft);
}

.rss-item-header {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 10px 16px;
  cursor: pointer;
  transition: background-color 0.15s;
}

.rss-item-header:hover {
  background: var(--surface-1);
}

.rss-item-checkbox {
  flex-shrink: 0;
}

.rss-item-info {
  display: block;
  flex: 1;
  min-width: 0;
  border: 0;
  padding: 0;
  background: transparent;
  color: inherit;
  font: inherit;
  text-align: left;
  cursor: pointer;
}

.rss-item-info:focus-visible {
  outline: 2px solid var(--primary);
  outline-offset: 2px;
}

.rss-item-title {
  display: block;
  font-size: 13px;
  font-weight: 500;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.rss-item--read .rss-item-title {
  font-weight: 400;
}

.rss-item-meta {
  display: flex;
  gap: 8px;
  font-size: 11px;
  color: var(--text-2);
  margin-top: 2px;
}

.rss-item-actions {
  display: flex;
  align-items: center;
  gap: 2px;
  flex-shrink: 0;
}

.rss-item-badge--downloaded {
  color: var(--color-primary);
}

.rss-item-delete {
  opacity: 0;
  transition: opacity 0.15s;
}

.rss-item-header:hover .rss-item-delete {
  opacity: 1;
}

.rss-preview-dialog {
  max-width: 90vw;
  width: 860px;
  max-height: 85vh;
  display: flex;
  flex-direction: column;
}

.rss-preview-title {
  overflow: hidden;
  text-overflow: ellipsis;
  display: -webkit-box;
  -webkit-line-clamp: 2;
  -webkit-box-orient: vertical;
  line-height: 1.4;
}

.rss-preview-body {
  flex: 1;
  overflow-y: auto;
  min-height: 0;
}

.rss-preview-meta {
  display: flex;
  gap: 8px;
  font-size: 11px;
  color: var(--text-2);
  margin-bottom: 12px;
}

.rss-preview-content {
  font-size: 13px;
  line-height: 1.7;
  color: var(--foreground);
  word-break: break-word;
}

.rss-preview-content :deep(img) {
  max-width: 100%;
  height: auto;
  border-radius: 4px;
  margin: 8px 0;
}

.rss-preview-content :deep(a) {
  color: var(--color-primary);
  text-decoration: underline;
  text-underline-offset: 2px;
}

.rss-preview-content :deep(blockquote) {
  border-left: 3px solid var(--border);
  margin: 8px 0;
  padding: 4px 12px;
  color: var(--text-2);
}

.rss-preview-content :deep(pre) {
  background: var(--muted);
  padding: 8px 12px;
  border-radius: 4px;
  overflow-x: auto;
  font-size: 12px;
}

.rss-preview-content :deep(p) {
  margin: 8px 0;
}

.rss-preview-downloaded {
  display: inline-flex;
  align-items: center;
  gap: 4px;
  font-size: 12px;
  color: var(--color-primary);
  margin-right: auto;
}

.rss-viewer-dialog {
  max-width: 96vw;
  width: 1400px;
  height: 94vh;
  display: flex;
  flex-direction: column;
}

.rss-viewer-body {
  flex: 1;
  min-height: 0;
  overflow: hidden;
  display: flex;
  flex-direction: column;
}

.rss-viewer-frame {
  flex: 1;
  width: 100%;
  min-height: 0;
  border: 1px solid var(--border);
  border-radius: 6px;
  background: var(--background);
}

.rss-viewer-loading,
.rss-viewer-error {
  display: flex;
  align-items: center;
  justify-content: center;
  height: 100%;
  font-size: 13px;
  color: var(--text-2);
}

.rss-viewer-error {
  color: var(--danger);
}

.rss-viewer-external {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: 12px;
  height: 100%;
  font-size: 13px;
  color: var(--text-2);
  padding: 24px;
  text-align: center;
}

.rss-viewer-path {
  font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
  font-size: 11px;
  word-break: break-all;
  opacity: .7;
}

.rss-item-quality {
  font-size: 10px;
  padding: 0 4px;
  border-radius: 2px;
  background: hsl(var(--muted));
  color: hsl(var(--muted-foreground));
  text-transform: uppercase;
}

.rss-item-rule {
  font-size: 10px;
  padding: 0 4px;
  border-radius: 2px;
  background: hsl(var(--primary) / 0.15);
  color: hsl(var(--primary));
  max-width: 120px;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.rss-items--compact .rss-item {
  padding: 4px 8px;
}

.rss-items--compact .rss-item-header {
  gap: 6px;
}

.rss-items--compact .rss-item-title {
  font-size: 12px;
}

.rss-items--compact .rss-item-meta {
  font-size: 10px;
}
</style>
