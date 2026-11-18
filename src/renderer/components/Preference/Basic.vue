<template>
  <div class="content panel panel-layout panel-layout--v">
    <main class="panel-content">
      <form class="form-preference" ref="basicForm" @submit.prevent>
        <div class="settings-section">
          <div class="settings-section-header">
            <div class="section-icon"><Globe :size="16" /></div>
            <div class="section-title">
              <h3>
                {{ $t('preferences.language') }} &
                {{ $t('preferences.startup') }}
              </h3>
            </div>
          </div>
          <div class="settings-section-content">
            <div class="settings-select-group">
              <div class="settings-select-item" data-preference-search-target="preferences.language">
                <label class="settings-select-item-label">{{ $t('preferences.language') }}</label>
                <Select v-model="form.locale" class="settings-select-control">
                  <SelectTrigger>
                    <SelectValue :placeholder="$t('preferences.change-language')" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem v-for="item in locales" :key="item.value" :value="item.value">
                      {{ item.label }}
                    </SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div
                v-if="isMac"
                class="settings-select-item"
                data-preference-search-target="preferences.run-mode"
              >
                <label class="settings-select-item-label">{{ $t('preferences.run-mode') }}</label>
                <Select v-model="form.runMode" class="settings-select-control">
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem v-for="item in runModes" :key="item.value" :value="item.value">
                      {{ item.label }}
                    </SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>
            <div class="settings-row" data-preference-search-target="preferences.open-at-login">
              <div class="settings-row-content">
                <span class="settings-row-title">{{ $t('preferences.open-at-login') }}</span>
              </div>
              <div class="settings-row-action">
                <ui-checkbox
                  :model-value="!!form.openAtLogin"
                  @change="(val) => setBasicBoolean('openAtLogin', val)"
                />
              </div>
            </div>
            <div class="settings-row" data-preference-search-target="preferences.keep-window-state">
              <div class="settings-row-content">
                <span class="settings-row-title">{{ $t('preferences.keep-window-state') }}</span>
              </div>
              <div class="settings-row-action">
                <ui-checkbox
                  :model-value="!!form.keepWindowState"
                  @change="(val) => setBasicBoolean('keepWindowState', val)"
                />
              </div>
            </div>
            <div class="settings-row" data-preference-search-target="preferences.auto-resume-all">
              <div class="settings-row-content">
                <span class="settings-row-title">{{ $t('preferences.auto-resume-all') }}</span>
              </div>
              <div class="settings-row-action">
                <ui-checkbox
                  :model-value="!!form.resumeAllWhenAppLaunched"
                  @change="(val) => setBasicBoolean('resumeAllWhenAppLaunched', val)"
                />
              </div>
            </div>
            <div class="settings-row" data-preference-search-target="preferences.purge-record-on-start">
              <div class="settings-row-content">
                <span class="settings-row-title">{{ $t('preferences.purge-record-on-start') }}</span>
              </div>
              <div class="settings-row-action">
                <ui-checkbox
                  :model-value="!!form.purgeRecordOnStart"
                  @change="(val) => setBasicBoolean('purgeRecordOnStart', val)"
                />
              </div>
            </div>
          </div>
        </div>

        <div
          class="settings-section"
          data-preference-search-target="preferences.default-dir preferences.history-directories preferences.favorite-directory preferences.unfavorite-directory preferences.remove-history-directory"
        >
          <div class="settings-section-header">
            <div class="section-icon"><FolderDown :size="16" /></div>
            <div class="section-title">
              <h3>{{ $t('preferences.default-dir') }}</h3>
            </div>
          </div>
          <div class="settings-section-content">
            <div class="input-group input-group--bordered">
              <span class="input-prepend">
                <history-directory @selected="handleHistoryDirectorySelected" />
              </span>
              <Input
                placeholder=""
                v-model="form.dir"
                readonly
                class="path-indicator-field flex-1 shadow-none rounded-none border-none noinput"
              />
              <span class="input-append" v-if="isRenderer">
                <select-directory @selected="handleNativeDirectorySelected" />
              </span>
            </div>
            <div class="form-info" v-if="isMas">
              {{ $t('preferences.mas-default-dir-tips') }}
            </div>
          </div>
        </div>

        <div
          class="settings-section"
          data-preference-search-target="preferences.file-category-dirs preferences.file-category-music preferences.file-category-video preferences.file-category-image preferences.file-category-document preferences.file-category-compressed preferences.file-category-program preferences.file-category-rss"
        >
          <div class="settings-section-header">
            <div class="section-icon"><FolderDown :size="16" /></div>
            <div class="section-title">
              <h3>{{ $t('preferences.file-category-dirs') }}</h3>
            </div>
          </div>
          <div class="settings-section-content">
            <div class="form-info" style="margin-bottom: 8px">
              {{ $t('preferences.file-category-dirs-tips') }}
            </div>
            <div
              v-for="cat in fileCategories"
              :key="cat.key"
              class="settings-row category-path-row"
              style="margin-bottom: 6px"
            >
              <span class="settings-row-title category-path-label" style="flex: 0 0 80px; min-width: 80px">{{
                cat.label
              }}</span>
              <div class="input-group input-group--bordered category-path-group" style="flex: 1; min-width: 0">
                <Input
                  :model-value="categoryDirectoryValue(cat.key)"
                  readonly
                  class="path-indicator-field flex-1 shadow-none rounded-none border-none noinput"
                />
                <span class="input-append" v-if="isRenderer">
                  <select-directory
                    @selected="(dir) => handleCategoryDirectorySelected(cat.key, dir)"
                  />
                </span>
              </div>
            </div>
          </div>
        </div>

        <div
          class="settings-section"
          data-preference-search-target="preferences.task-routing-rules preferences.task-routing-rule-add"
        >
          <div class="settings-section-header">
            <div class="section-icon"><FolderDown :size="16" /></div>
            <div class="section-title">
              <h3>{{ $t('preferences.task-routing-rules') }}</h3>
            </div>
          </div>
          <div class="settings-section-content">
            <div class="form-info" style="margin-bottom: 8px">
              {{ $t('preferences.task-routing-rules-tips') }}
            </div>
            <div
              v-for="(rule, index) in form.taskRoutingRules"
              :key="rule.id"
              class="settings-row"
              style="margin-bottom: 6px; align-items: flex-start"
            >
              <div style="flex: 1; display: flex; gap: 6px; min-width: 0; flex-wrap: wrap">
                <Input
                  :placeholder="$t('preferences.task-routing-rule-pattern-placeholder')"
                  :model-value="form.taskRoutingRules[index].pattern"
                  @update:model-value="(val) => updateRuleField(index, 'pattern', val)"
                  class="flex-1 shadow-none border-none"
                  style="min-width: 100px"
                />
                <Input
                  :placeholder="$t('preferences.task-routing-rule-label-placeholder')"
                  :model-value="form.taskRoutingRules[index].label"
                  @update:model-value="(val) => updateRuleField(index, 'label', val)"
                  class="flex-1 shadow-none border-none"
                  style="min-width: 80px"
                />
                <div class="input-group input-group--bordered" style="flex: 1; min-width: 160px">
                  <Input
                    :placeholder="$t('preferences.task-routing-rule-dir-placeholder')"
                    :model-value="form.taskRoutingRules[index].dir"
                    @update:model-value="(val) => updateRuleField(index, 'dir', val)"
                    class="path-indicator-field flex-1 shadow-none rounded-none border-none"
                  />
                  <span class="input-append" v-if="isRenderer">
                    <select-directory
                      class="routing-rule-dir-picker"
                      @selected="(dir) => updateRuleField(index, 'dir', dir)"
                    />
                  </span>
                </div>
                <div class="settings-row-action" style="flex: 0 0 auto">
                  <ui-checkbox
                    :model-value="!!form.taskRoutingRules[index].enabled"
                    @change="(val) => updateRuleField(index, 'enabled', !!val)"
                  />
                </div>
                <ui-button
                  size="mini"
                  variant="text"
                  @click="removeRoutingRule(index)"
                  style="padding: 4px 8px"
                >
                  ×
                </ui-button>
              </div>
            </div>
            <div class="settings-row">
              <ui-button size="mini" variant="primary" @click="addRoutingRule">
                + {{ $t('preferences.task-routing-rule-add') }}
              </ui-button>
            </div>
          </div>
        </div>

        <div class="settings-section">
          <div class="settings-section-header">
            <div class="section-icon"><Gauge :size="16" /></div>
            <div class="section-title">
              <h3>{{ $t('preferences.transfer-settings') }}</h3>
            </div>
          </div>
          <div class="settings-section-content">
            <div
              class="settings-row"
              data-preference-search-target="preferences.speed-limit-enabled"
            >
              <div class="settings-row-content">
                <span class="settings-row-title">{{ $t('preferences.speed-limit-enabled') }}</span>
                <div class="settings-row-description">
                  {{ $t('preferences.speed-limit-enabled-description') }}
                </div>
              </div>
              <div class="settings-row-action">
                <ui-switch
                  :model-value="speedLimitEnabled"
                  @change="onSpeedLimitToggle"
                />
              </div>
            </div>
            <div class="settings-select-group">
              <div class="settings-select-item" data-preference-search-target="preferences.transfer-speed-upload">
                <label class="settings-select-item-label"
                  ><ArrowUp :size="12" style="vertical-align: middle; margin-right: 4px" />{{
                    $t('preferences.transfer-speed-upload')
                  }}</label
                >
                <div class="settings-inline-input">
                  <NumberInput
                    v-model="maxOverallUploadLimitParsed"
                    :min="0"
                    :max="65535"
                    :step="1"
                  />
                  <Select :model-value="uploadUnit" @update:model-value="handleUploadChange">
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem v-for="item in speedUnits" :key="item.value" :value="item.value">
                        {{ item.label }}
                      </SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>
              <div class="settings-select-item" data-preference-search-target="preferences.transfer-speed-download">
                <label class="settings-select-item-label"
                  ><ArrowDown :size="12" style="vertical-align: middle; margin-right: 4px" />{{
                    $t('preferences.transfer-speed-download')
                  }}</label
                >
                <div class="settings-inline-input">
                  <NumberInput
                    v-model="maxOverallDownloadLimitParsed"
                    :min="0"
                    :max="65535"
                    :step="1"
                  />
                  <Select :model-value="downloadUnit" @update:model-value="handleDownloadChange">
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem v-for="item in speedUnits" :key="item.value" :value="item.value">
                        {{ item.label }}
                      </SelectItem>
                    </SelectContent>
                  </Select>
                </div>
              </div>
            </div>
          </div>
        </div>

        <div class="settings-section">
          <div class="settings-section-header">
            <div class="section-icon"><Share2 :size="16" /></div>
            <div class="section-title">
              <h3>{{ $t('preferences.bt-settings') }}</h3>
            </div>
          </div>
          <div class="settings-section-content">
            <div class="settings-row" data-preference-search-target="preferences.bt-save-metadata">
              <div class="settings-row-content">
                <span class="settings-row-title">{{ $t('preferences.bt-save-metadata') }}</span>
              </div>
              <div class="settings-row-action">
                <ui-checkbox
                  :model-value="!!form.btSaveMetadata"
                  @change="(val) => setBasicBoolean('btSaveMetadata', val)"
                />
              </div>
            </div>
            <div class="settings-row" data-preference-search-target="preferences.bt-force-encryption">
              <div class="settings-row-content">
                <span class="settings-row-title">{{ $t('preferences.bt-force-encryption') }}</span>
              </div>
              <div class="settings-row-action">
                <ui-checkbox
                  :model-value="!!form.btForceEncryption"
                  @change="(val) => setBasicBoolean('btForceEncryption', val)"
                />
              </div>
            </div>
            <div
              class="settings-row"
              data-preference-search-target="preferences.keep-seeding preferences.seed-ratio preferences.seed-time"
            >
              <div class="settings-row-content">
                <span class="settings-row-title">{{ $t('preferences.keep-seeding') }}</span>
              </div>
              <div class="settings-row-action">
                <ui-checkbox :model-value="!!form.keepSeeding" @change="onKeepSeedingToggle" />
              </div>
            </div>
            <div v-if="form.keepSeeding" class="settings-select-group">
              <div class="settings-select-item" data-preference-search-target="preferences.seed-ratio">
                <label class="settings-select-item-label">{{ $t('preferences.seed-ratio') }}</label>
                <NumberInput v-model="form.seedRatio" :min="0" :max="100" :step="0.1" />
              </div>
              <div class="settings-select-item" data-preference-search-target="preferences.seed-time">
                <label class="settings-select-item-label"
                  >{{ $t('preferences.seed-time') }} ({{ $t('preferences.seed-time-unit') }})</label
                >
                <NumberInput v-model="form.seedTime" :min="0" :max="525600" :step="1" />
              </div>
            </div>
            <div class="settings-select-group">
              <div class="settings-select-item" data-preference-search-target="preferences.bt-upload-rate-limit">
                <label class="settings-select-item-label"
                  ><ArrowUp :size="12" style="vertical-align: middle; margin-right: 4px" />{{
                    $t('preferences.bt-upload-rate-limit')
                  }}</label
                >
                <div class="settings-inline-input">
                  <NumberInput
                    :model-value="speedLimitAmount('btUploadRateLimit')"
                    :min="0"
                    :max="65535"
                    :step="1"
                    @update:model-value="(amount) => setSpeedLimitAmount('btUploadRateLimit', amount)"
                  />
                  <Select
                    :model-value="speedLimitUnit('btUploadRateLimit')"
                    @update:model-value="(unit) => setSpeedLimitUnit('btUploadRateLimit', unit)"
                  >
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem v-for="item in speedUnits" :key="item.value" :value="item.value">
                        {{ item.label }}
                      </SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <div class="settings-row-description">
                  {{ $t('preferences.bt-upload-rate-limit-tips') }}
                </div>
              </div>
            </div>
          </div>
        </div>

        <div class="settings-section">
          <div class="settings-section-header">
            <div class="section-icon"><ListTodo :size="16" /></div>
            <div class="section-title">
              <h3>{{ $t('preferences.task-manage') }}</h3>
            </div>
          </div>
          <div class="settings-section-content">
            <div class="settings-select-group">
              <div
                class="settings-select-item"
                data-preference-search-target="preferences.max-concurrent-downloads"
              >
                <label class="settings-select-item-label">{{
                  $t('preferences.max-concurrent-downloads')
                }}</label>
                <NumberInput
                  v-model="form.maxConcurrentDownloads"
                  :min="1"
                  :max="maxConcurrentDownloads"
                />
              </div>
              <div
                class="settings-select-item"
                data-preference-search-target="preferences.connections-per-task"
              >
                <label class="settings-select-item-label">{{
                  $t('preferences.connections-per-task')
                }}</label>
                <NumberInput v-model="form.split" :min="1" :max="128" />
              </div>
            </div>
            <div
              class="settings-row"
              data-preference-search-target="preferences.auto-retry preferences.auto-retry-strategy preferences.auto-retry-interval preferences.auto-retry-max-attempts preferences.worker-max-retries"
            >
              <div class="settings-row-content">
                <span class="settings-row-title">{{ $t('preferences.auto-retry') }}</span>
              </div>
              <div class="settings-row-action">
                <ui-checkbox
                  :model-value="!!form.autoRetry"
                  @change="(val) => setBasicBoolean('autoRetry', val)"
                />
              </div>
            </div>
            <div v-if="form.autoRetry" class="settings-select-group">
              <div class="settings-select-item">
                <label class="settings-select-item-label">{{
                  $t('preferences.auto-retry-strategy')
                }}</label>
                <Select v-model="form.autoRetryStrategy" class="settings-select-control">
                  <SelectTrigger>
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem
                      v-for="item in retryStrategies"
                      :key="item.value"
                      :value="item.value"
                    >
                      {{ item.label }}
                    </SelectItem>
                  </SelectContent>
                </Select>
              </div>
              <div class="settings-select-item">
                <label class="settings-select-item-label">
                  {{ $t('preferences.auto-retry-interval') }} ({{
                    $t('preferences.auto-retry-interval-unit')
                  }})
                </label>
                <NumberInput v-model="form.autoRetryInterval" :min="1" :max="300" :step="1" />
              </div>
              <div class="settings-select-item">
                <label class="settings-select-item-label">
                  {{ $t('preferences.auto-retry-max-attempts') }}
                </label>
                <NumberInput v-model="form.autoRetryMaxAttempts" :min="0" :max="1000" :step="1" />
              </div>
            </div>
            <div v-if="form.autoRetry" class="settings-select-group">
              <div class="settings-select-item">
                <label class="settings-select-item-label">
                  {{ $t('preferences.worker-max-retries') }}
                </label>
                <NumberInput v-model="form.maxWorkerRetries" :min="1" :max="20" :step="1" />
              </div>
            </div>
            <div
              class="settings-row"
              data-preference-search-target="preferences.auto-detect-low-speed-tasks preferences.low-speed-threshold"
            >
              <div class="settings-row-content">
                <div class="settings-row-title">
                  {{ $t('preferences.auto-detect-low-speed-tasks') }}
                </div>
                <div class="settings-row-description">
                  {{ $t('preferences.auto-detect-low-speed-tasks-tips') }}
                </div>
              </div>
              <div class="settings-row-action">
                <ui-checkbox
                  :model-value="!!form.autoDetectLowSpeedTasks"
                  @change="(val) => setBasicBoolean('autoDetectLowSpeedTasks', val)"
                />
              </div>
            </div>
            <div v-if="form.autoDetectLowSpeedTasks" class="settings-select-group">
              <div class="settings-select-item">
                <label class="settings-select-item-label">
                  {{ $t('preferences.low-speed-threshold') }} ({{
                    $t('preferences.low-speed-threshold-unit')
                  }})
                </label>
                <NumberInput v-model="form.lowSpeedThreshold" :min="1" :max="10240" :step="1" />
              </div>
            </div>
            <div class="settings-row">
              <div class="settings-row-content">
                <span class="settings-row-title">{{
                  $t('preferences.new-task-show-downloading')
                }}</span>
              </div>
              <div class="settings-row-action">
                <ui-checkbox
                  :model-value="!!form.newTaskShowDownloading"
                  @change="(val) => setBasicBoolean('newTaskShowDownloading', val)"
                />
              </div>
            </div>
            <div class="settings-row">
              <div class="settings-row-content">
                <span class="settings-row-title">{{
                  $t('preferences.task-completed-notify')
                }}</span>
              </div>
              <div class="settings-row-action">
                <ui-checkbox
                  :model-value="!!form.taskNotification"
                  @change="(val) => setBasicBoolean('taskNotification', val)"
                />
              </div>
            </div>
            <div v-if="!isAndroid" class="settings-row">
              <div class="settings-row-content">
                <div class="settings-row-title">
                  {{ $t('preferences.clipboard-watch') }}
                </div>
                <div class="settings-row-description">
                  {{ $t('preferences.clipboard-watch-tips') }}
                </div>
              </div>
              <div class="settings-row-action">
                <ui-checkbox
                  :model-value="!!form.clipboardWatch"
                  @change="(val) => setBasicBoolean('clipboardWatch', val)"
                />
              </div>
            </div>
            <div v-if="!isAndroid && form.clipboardWatch" class="settings-row">
              <div class="settings-row-content">
                <div class="settings-row-title">
                  {{ $t('preferences.clipboard-watch-ext') }}
                </div>
                <div class="settings-row-description">
                  {{ $t('preferences.clipboard-watch-ext-tips') }}
                </div>
                <Textarea
                  :rows="3"
                  autocomplete="off"
                  spellcheck="false"
                  :placeholder="$t('preferences.clipboard-watch-ext-placeholder')"
                  v-model="form.clipboardWatchExtensions"
                  style="margin-top: 8px; max-height: 5lh"
                />
              </div>
            </div>
            <div class="settings-row">
              <div class="settings-row-content">
                <div class="settings-row-title">
                  {{ $t('preferences.prevent-sleep-while-downloading') }}
                </div>
                <div class="settings-row-description">
                  {{ $t('preferences.prevent-sleep-while-downloading-tips') }}
                </div>
              </div>
              <div class="settings-row-action">
                <ui-checkbox
                  :model-value="!!form.preventSleepWhileDownloading"
                  @change="(val) => setBasicBoolean('preventSleepWhileDownloading', val)"
                />
              </div>
            </div>
            <div v-if="!isAndroid" class="settings-row">
              <div class="settings-row-content">
                <div class="settings-row-title">
                  {{ $t('preferences.shutdown-when-complete') }}
                </div>
                <div class="settings-row-description">
                  {{ $t('preferences.shutdown-when-complete-tips') }}
                </div>
              </div>
              <div class="settings-row-action">
                <ui-checkbox
                  :model-value="!!form.shutdownWhenComplete"
                  @change="(val) => setBasicBoolean('shutdownWhenComplete', val)"
                />
              </div>
            </div>
            <div class="settings-row">
              <div class="settings-row-content">
                <span class="settings-row-title">{{
                  $t('preferences.no-confirm-before-delete-task')
                }}</span>
              </div>
              <div class="settings-row-action">
                <ui-checkbox
                  :model-value="!!form.noConfirmBeforeDeleteTask"
                  @change="(val) => setBasicBoolean('noConfirmBeforeDeleteTask', val)"
                />
              </div>
            </div>
            <div class="settings-row">
              <div class="settings-row-content">
                <div class="settings-row-title">
                  {{ $t('preferences.use-remote-file-time') }}
                </div>
                <div class="settings-row-description">
                  {{ $t('preferences.use-remote-file-time-tips') }}
                </div>
              </div>
              <div class="settings-row-action">
                <ui-checkbox
                  :model-value="!!form.useRemoteFileTime"
                  @change="(val) => setBasicBoolean('useRemoteFileTime', val)"
                />
              </div>
            </div>
            <div class="settings-row">
              <div class="settings-row-content">
                <div class="settings-row-title">
                  {{ $t('preferences.auto-file-renaming') }}
                </div>
                <div class="settings-row-description">
                  {{ $t('preferences.auto-file-renaming-tips') }}
                </div>
              </div>
              <div class="settings-row-action">
                <ui-checkbox
                  :model-value="!!form.autoFileRenaming"
                  @change="(val) => setBasicBoolean('autoFileRenaming', val)"
                />
              </div>
            </div>
          </div>
        </div>

        <div class="settings-section">
          <div class="settings-section-header"></div>
          <div class="settings-section-content version-section">
            <div class="version-indicator">
              <div class="version-item">
                <span class="version-name">Risuko</span>
                <span class="version-value">{{ appVersion || '--' }}</span>
              </div>
              <div class="version-item">
                <span class="version-name">Engine</span>
                <span class="version-value">{{ engineVersion }}</span>
              </div>
            </div>
          </div>
        </div>
      </form>
      <div class="form-actions">
        <ui-button @click="resetForm('basicForm')">{{ $t('preferences.discard') }}</ui-button>
        <ui-button variant="primary" @click="submitForm('basicForm')">{{
          $t('preferences.save')
        }}</ui-button>
      </div>
    </main>
  </div>
</template>

<script lang="ts">
import {
	ArrowDown,
	ArrowUp,
	FolderDown,
	Gauge,
	Globe,
	ListTodo,
	Share2,
} from "@lucide/vue";
import {
	APP_RUN_MODE,
	DEFAULT_CLIPBOARD_WATCH_EXTENSIONS,
	DOH_PROVIDERS,
	ENGINE_MAX_CONCURRENT_DOWNLOADS,
	ENGINE_RPC_PORT,
	FILE_CATEGORIES,
} from "@shared/constants";
import { availableLanguages } from "@shared/locales";
import { redactProxySettings } from "@shared/types/config";
import {
	changedConfig,
	convertLineToComma,
	diffConfig,
	extractSpeedUnit,
	parseBooleanConfig,
} from "@shared/utils";
import logger from "@shared/utils/logger";
import { formatSpeedLimit } from "@shared/utils/speedLimit";
import { reduceTrackerString } from "@shared/utils/tracker";
import { invoke } from "@tauri-apps/api/core";
import { cloneDeep, isEmpty } from "lodash";
import SelectDirectory from "@/components/Native/SelectDirectory.vue";
import HistoryDirectory from "@/components/Preference/HistoryDirectory.vue";
import UiButton from "@/components/ui/compat/UiButton.vue";
import { confirm } from "@/components/ui/confirm-dialog";
import { Input } from "@/components/ui/input";
import NumberInput from "@/components/ui/NumberInput.vue";
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
import {
	finalizeAdvancedChanges,
	resolveDohProvider,
} from "@/utils/advancedPreferenceSave";
import { normalizeMaxAttempts } from "@/utils/autoRetry";
import { approveCompletionScript, pendingCompletionScript } from "@/utils/task";
import { getRisukoVersion } from "@/utils/version";

const RETRY_STRATEGY_STATIC = "static";
const RETRY_STRATEGY_EXPONENTIAL = "exponential";

const normalizePositiveInt = (
	value,
	fallback,
	min = 1,
	max = Number.MAX_SAFE_INTEGER,
) => {
	const parsed = Number(value);
	if (!Number.isFinite(parsed)) {
		return fallback;
	}
	return Math.min(Math.max(Math.floor(parsed), min), max);
};

const parseExtList = (text) => [
	...new Set(
		String(text || "")
			.split(/[\s,]+/)
			.map((s) => s.trim().replace(/^\.+/, "").toLowerCase())
			.filter(Boolean),
	),
];

const BASIC_BOOLEAN_KEYS = [
	"openAtLogin",
	"keepWindowState",
	"resumeAllWhenAppLaunched",
	"purgeRecordOnStart",
	"btSaveMetadata",
	"btForceEncryption",
	"clipboardWatch",
	"keepSeeding",
	"autoRetry",
	"autoDetectLowSpeedTasks",
	"newTaskShowDownloading",
	"preventSleepWhileDownloading",
	"shutdownWhenComplete",
	"taskNotification",
	"noConfirmBeforeDeleteTask",
	"useRemoteFileTime",
	"autoFileRenaming",
];

const normalizeBasicConfig = (data) => {
	for (const key of BASIC_BOOLEAN_KEYS) {
		if (key in data) {
			data[key] = !!data[key];
		}
	}

	if (data.btTracker) {
		data.btTracker = reduceTrackerString(convertLineToComma(data.btTracker));
	}

	if ("clipboardWatchExtensions" in data) {
		data.clipboardWatchExtensions = parseExtList(data.clipboardWatchExtensions);
	}

	if (data.rpcListenPort === "") {
		data.rpcListenPort = ENGINE_RPC_PORT;
	}

	if ("autoRetryInterval" in data) {
		data.autoRetryInterval = normalizePositiveInt(
			data.autoRetryInterval,
			5,
			1,
			300,
		);
	}

	if ("autoRetryMaxAttempts" in data) {
		data.autoRetryMaxAttempts = normalizeMaxAttempts(data.autoRetryMaxAttempts);
	}

	if ("split" in data) {
		data.split = normalizePositiveInt(data.split, 16, 1, 128);
	}

	if ("maxConcurrentDownloads" in data) {
		data.maxConcurrentDownloads = normalizePositiveInt(
			data.maxConcurrentDownloads,
			5,
			1,
			ENGINE_MAX_CONCURRENT_DOWNLOADS,
		);
	}

	if ("lowSpeedThreshold" in data) {
		data.lowSpeedThreshold = normalizePositiveInt(
			data.lowSpeedThreshold,
			20,
			1,
			10240,
		);
	}

	if ("autoRetryStrategy" in data) {
		data.autoRetryStrategy =
			data.autoRetryStrategy === RETRY_STRATEGY_EXPONENTIAL
				? RETRY_STRATEGY_EXPONENTIAL
				: RETRY_STRATEGY_STATIC;
	}

	if ("maxWorkerRetries" in data) {
		data.maxWorkerRetries = normalizePositiveInt(
			data.maxWorkerRetries,
			5,
			1,
			20,
		);
	}

	return data;
};

const initForm = (config) => {
	const {
		autoDetectLowSpeedTasks,
		autoFileRenaming,
		autoRetry,
		autoRetryInterval,
		autoRetryMaxAttempts,
		autoRetryStrategy,
		maxWorkerRetries,
		btForceEncryption,
		btSaveMetadata,
		btUploadRateLimit,
		clipboardWatch,
		clipboardWatchExtensions,
		dir,
		fileCategoryDirs,
		keepSeeding,
		keepWindowState,
		locale,
		maxConcurrentDownloads,
		maxOverallDownloadLimit,
		maxOverallUploadLimit,
		newTaskShowDownloading,
		noConfirmBeforeDeleteTask,
		openAtLogin,
		preventSleepWhileDownloading,
		shutdownWhenComplete,
		purgeRecordOnStart,
		resumeAllWhenAppLaunched,
		runMode,
		seedRatio,
		seedTime,
		taskNotification,
		split,
		useRemoteFileTime,
		lowSpeedThreshold,
	} = config;

	const result = {
		autoDetectLowSpeedTasks: parseBooleanConfig(autoDetectLowSpeedTasks),
		autoFileRenaming: parseBooleanConfig(autoFileRenaming, true),
		autoRetry: parseBooleanConfig(autoRetry),
		autoRetryInterval: normalizePositiveInt(autoRetryInterval, 5, 1, 300),
		autoRetryMaxAttempts: normalizeMaxAttempts(autoRetryMaxAttempts),
		autoRetryStrategy:
			autoRetryStrategy === RETRY_STRATEGY_EXPONENTIAL
				? RETRY_STRATEGY_EXPONENTIAL
				: RETRY_STRATEGY_STATIC,
		maxWorkerRetries: normalizePositiveInt(maxWorkerRetries, 5, 1, 20),
		btForceEncryption: parseBooleanConfig(btForceEncryption),
		btSaveMetadata: parseBooleanConfig(btSaveMetadata),
		btUploadRateLimit,
		clipboardWatch: parseBooleanConfig(clipboardWatch, true),
		clipboardWatchExtensions: (Array.isArray(clipboardWatchExtensions)
			? clipboardWatchExtensions
			: DEFAULT_CLIPBOARD_WATCH_EXTENSIONS
		).join(" "),
		dir,
		fileCategoryDirs: {
			music: "",
			video: "",
			image: "",
			document: "",
			compressed: "",
			program: "",
			rss: "",
			...(fileCategoryDirs || {}),
		},
		taskRoutingRules: (config.taskRoutingRules || []).map((rule) => ({
			...rule,
			id: rule.id || crypto.randomUUID(),
		})),
		keepSeeding: parseBooleanConfig(keepSeeding),
		keepWindowState: parseBooleanConfig(keepWindowState),
		locale,
		lowSpeedThreshold: normalizePositiveInt(lowSpeedThreshold, 20, 1, 10240),
		maxConcurrentDownloads: normalizePositiveInt(
			maxConcurrentDownloads,
			5,
			1,
			ENGINE_MAX_CONCURRENT_DOWNLOADS,
		),
		split: normalizePositiveInt(split, 16, 1, 128),
		maxOverallDownloadLimit,
		maxOverallUploadLimit,
		newTaskShowDownloading: parseBooleanConfig(newTaskShowDownloading),
		noConfirmBeforeDeleteTask: parseBooleanConfig(noConfirmBeforeDeleteTask),
		openAtLogin: parseBooleanConfig(openAtLogin),
		preventSleepWhileDownloading: parseBooleanConfig(
			preventSleepWhileDownloading,
			true,
		),
		shutdownWhenComplete: parseBooleanConfig(shutdownWhenComplete),
		purgeRecordOnStart: parseBooleanConfig(purgeRecordOnStart),
		resumeAllWhenAppLaunched: parseBooleanConfig(resumeAllWhenAppLaunched),
		runMode,
		seedRatio,
		seedTime,
		taskNotification: parseBooleanConfig(taskNotification),
		useRemoteFileTime: parseBooleanConfig(useRemoteFileTime),
	};
	return result;
};

export default {
	name: "preference-basic",
	components: {
		[HistoryDirectory.name]: HistoryDirectory,
		[SelectDirectory.name]: SelectDirectory,
		[UiButton.name]: UiButton,
		NumberInput,
		Input,
		Select,
		SelectContent,
		SelectItem,
		SelectTrigger,
		SelectValue,
		Textarea,
		Globe,
		FolderDown,
		Gauge,
		Share2,
		ListTodo,
		ArrowUp,
		ArrowDown,
	},
	data() {
		const preferenceStore = usePreferenceStore();
		const formOriginal = initForm(preferenceStore.config);
		const form = isEmpty(changedConfig.basic)
			? cloneDeep(formOriginal)
			: initForm({ ...preferenceStore.config, ...changedConfig.basic });

		return {
			appVersion: "",
			form,
			formOriginal,
			locales: availableLanguages,
		};
	},
	created() {
		getRisukoVersion().then((v) => {
			this.appVersion = v;
		});

		const currentEngineVersion = this.engineInfo?.version;
		if (!currentEngineVersion) {
			useAppStore().fetchEngineInfo();
		}
	},
	computed: {
		isRenderer: () => is.renderer(),
		isMac: () => is.macOS(),
		isMas: () => is.mas(),
		isAndroid: () => is.android(),
		speedLimitEnabled() {
			return usePreferenceStore().engineMode === "LIMIT";
		},
		maxConcurrentDownloads() {
			return ENGINE_MAX_CONCURRENT_DOWNLOADS;
		},
		fileCategories() {
			return Object.values(FILE_CATEGORIES).map((key) => ({
				key,
				label: this.$t(`preferences.file-category-${key}`),
			}));
		},
		maxOverallDownloadLimitParsed: {
			get() {
				return parseInt(this.form.maxOverallDownloadLimit, 10);
			},
			set(value) {
				const limit = value > 0 ? `${value}${this.downloadUnit}` : 0;
				this.form.maxOverallDownloadLimit = limit;
			},
		},
		maxOverallUploadLimitParsed: {
			get() {
				return parseInt(this.form.maxOverallUploadLimit, 10);
			},
			set(value) {
				const limit = value > 0 ? `${value}${this.uploadUnit}` : 0;
				this.form.maxOverallUploadLimit = limit;
			},
		},
		downloadUnit() {
			return extractSpeedUnit(this.form.maxOverallDownloadLimit);
		},
		uploadUnit() {
			return extractSpeedUnit(this.form.maxOverallUploadLimit);
		},
		runModes() {
			const result = [
				{
					label: this.$t("preferences.run-mode-standard"),
					value: APP_RUN_MODE.STANDARD,
				},
				{
					label: this.$t("preferences.run-mode-tray"),
					value: APP_RUN_MODE.TRAY,
				},
			];
			return result;
		},
		speedUnits() {
			return [
				{
					label: "KB/s",
					value: "K",
				},
				{
					label: "MB/s",
					value: "M",
				},
			];
		},
		retryStrategies() {
			return [
				{
					label: this.$t("preferences.auto-retry-strategy-static"),
					value: RETRY_STRATEGY_STATIC,
				},
				{
					label: this.$t("preferences.auto-retry-strategy-exponential"),
					value: RETRY_STRATEGY_EXPONENTIAL,
				},
			];
		},
		engineVersion() {
			const engineVersion = this.engineInfo?.version;
			return engineVersion ? `${engineVersion}` : "--";
		},
		engineInfo() {
			return useAppStore().engineInfo;
		},
	},
	methods: {
		setBasicBoolean(key, enable) {
			this.form[key] = !!enable;
		},
		categoryDirectoryValue(category) {
			return this.form.fileCategoryDirs?.[category] || this.form.dir || "";
		},
		handleCategoryDirectorySelected(category, dir) {
			this.form.fileCategoryDirs = {
				...this.form.fileCategoryDirs,
				[category]: dir,
			};
		},
		addRoutingRule() {
			const rules = [...(this.form.taskRoutingRules || [])];
			rules.push({
				id: crypto.randomUUID(),
				label: "",
				pattern: "",
				dir: "",
				enabled: true,
			});
			this.form.taskRoutingRules = rules;
		},
		removeRoutingRule(index) {
			const rules = [...(this.form.taskRoutingRules || [])];
			rules.splice(index, 1);
			this.form.taskRoutingRules = rules;
		},
		updateRuleField(index, field, value) {
			const rules = [...this.form.taskRoutingRules];
			rules[index] = { ...rules[index], [field]: value };
			this.form.taskRoutingRules = rules;
		},
		onSpeedLimitToggle(enabled) {
			usePreferenceStore().setEngineMode(enabled ? "LIMIT" : "MAX");
		},
		handleDownloadChange(value) {
			const speedLimit = parseInt(this.form.maxOverallDownloadLimit, 10);
			const limit = speedLimit > 0 ? `${speedLimit}${value}` : 0;
			this.form.maxOverallDownloadLimit = limit;
		},
		handleUploadChange(value) {
			const speedLimit = parseInt(this.form.maxOverallUploadLimit, 10);
			const limit = speedLimit > 0 ? `${speedLimit}${value}` : 0;
			this.form.maxOverallUploadLimit = limit;
		},
		speedLimitAmount(key) {
			return parseInt(this.form[key], 10);
		},
		speedLimitUnit(key) {
			return extractSpeedUnit(this.form[key]);
		},
		setSpeedLimitAmount(key, amount) {
			this.form[key] = formatSpeedLimit(amount, this.speedLimitUnit(key));
		},
		setSpeedLimitUnit(key, unit) {
			this.form[key] = formatSpeedLimit(this.speedLimitAmount(key), unit);
		},
		onKeepSeedingToggle(enable) {
			this.form.keepSeeding = !!enable;
			if (!enable) {
				this.form.seedRatio = 0;
			}
			this.form.seedTime = enable ? 525600 : 0;
		},
		handleHistoryDirectorySelected(dir) {
			this.form.dir = dir;
		},
		handleNativeDirectorySelected(dir) {
			this.form.dir = dir;
			usePreferenceStore().recordHistoryDirectory(dir);
		},
		syncFormConfig() {
			usePreferenceStore()
				.fetchPreference()
				.then((config) => {
					this.form = initForm(config);
					this.formOriginal = cloneDeep(this.form);
				});
		},
		async submitForm(_formName) {
			const advanced: Record<string, unknown> = { ...changedConfig.advanced };
			if (!isEmpty(advanced)) {
				const saved = usePreferenceStore().config;
				const pending = { ...saved, ...advanced };
				const finalized = finalizeAdvancedChanges(
					advanced,
					{
						engineOverridesText: advanced.engineOverridesText,
						dohProvider:
							advanced.dohProvider ??
							resolveDohProvider(
								saved.dohProvider,
								saved.dohUrl,
								DOH_PROVIDERS,
							),
						dohEnable: parseBooleanConfig(pending.dohEnable, false),
						dohUrl: pending.dohUrl,
						dohBootstrap: pending.dohBootstrap,
					},
					DOH_PROVIDERS,
				);
				if (finalized.error) {
					this.$msg.error(this.$t(finalized.error));
					return;
				}
				if (finalized.droppedOverrideKeys.length) {
					this.$msg.warning(
						this.$t("preferences.engine-overrides-reserved-keys", {
							keys: finalized.droppedOverrideKeys.join(", "),
						}),
					);
				}
			}
			const data = {
				...normalizeBasicConfig(diffConfig(this.formOriginal, this.form)),
				...advanced,
			};
			const proxy = data.proxy;
			const p2pProxyChanged =
				proxy &&
				typeof proxy === "object" &&
				(proxy as Record<string, unknown>)["p2p-profile-explicit"] === true;
			if (p2pProxyChanged) {
				const { confirmed } = await confirm({
					title: this.$t("preferences.proxy-p2p-restart-title"),
					message: this.$t("preferences.proxy-p2p-restart-confirm"),
					kind: "warning",
					confirmText: this.$t("app.yes"),
					cancelText: this.$t("app.no"),
				});
				if (!confirmed) {
					return;
				}
			}

			logger.log(
				"[Risuko] preference changed data:",
				redactProxySettings(data as Record<string, unknown>),
			);

			const script = pendingCompletionScript(
				advanced,
				usePreferenceStore().config,
			);
			if (script && !(await approveCompletionScript(script))) {
				this.$msg.error(this.$t("preferences.completion-script-not-approved"));
				return;
			}

			usePreferenceStore()
				.save(data)
				.then(() => {
					if ("clipboardWatch" in data) {
						invoke(
							data.clipboardWatch
								? "start_clipboard_watch"
								: "stop_clipboard_watch",
						).catch(() => {});
					}
					this.syncFormConfig();
					this.$msg.success(this.$t("preferences.save-success-message"));
					changedConfig.basic = {};
					changedConfig.advanced = {};
				})
				.catch(() => {
					this.$msg.error(this.$t("preferences.save-fail-message"));
				});
		},
		resetForm(_formName) {
			this.syncFormConfig();
		},
	},
	async beforeRouteLeave(to, _from) {
		changedConfig.basic = normalizeBasicConfig(
			diffConfig(this.formOriginal, this.form),
		);
		if (to.path === "/preference/advanced") {
			return true;
		}
		if (isEmpty(changedConfig.basic) && isEmpty(changedConfig.advanced)) {
			return true;
		}
		const { confirmed } = await confirm({
			message: this.$t("preferences.not-saved-confirm"),
			title: this.$t("preferences.not-saved"),
			kind: "warning",
			confirmText: this.$t("app.yes"),
			cancelText: this.$t("app.no"),
		});
		if (confirmed) {
			changedConfig.basic = {};
			changedConfig.advanced = {};
			return true;
		}
		return false;
	},
};
</script>
