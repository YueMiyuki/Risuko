<template>
  <div class="content panel panel-layout panel-layout--v">
    <main class="panel-content">
      <div class="form-preference">
        <div class="settings-section" data-preference-search-target="cloudSinks.sinks">
          <div class="settings-section-header">
            <div class="section-icon"><Cloud :size="16" /></div>
            <div class="section-title">
              <h3>{{ $t('cloudSinks.sinks') }}</h3>
              <p class="section-desc">{{ $t('cloudSinks.sinksHint') }}</p>
            </div>
            <Button size="sm" @click="openCreate">
              <Plus :size="14" class="mr-1" />
              {{ $t('cloudSinks.addSink') }}
            </Button>
          </div>
          <div class="settings-section-content">
            <div v-if="store.sinks.length === 0" class="empty-state">
              <CloudOff :size="28" class="empty-state-icon" />
              <p class="empty-state-title">{{ $t('cloudSinks.empty') }}</p>
              <p class="empty-state-hint">{{ $t('cloudSinks.emptyHint') }}</p>
            </div>
            <div v-else class="sink-list">
              <div
                v-for="sink in store.sinks"
                :key="sink.id"
                class="sink-card"
                :class="{ 'sink-card--default': store.defaultSinkId === sink.id }"
              >
                <div class="sink-card-icon">
                  <component :is="protocolIcon(sink.config.kind)" :size="18" />
                </div>
                <div class="sink-card-main">
                  <div class="sink-card-line">
                    <span class="sink-card-label">{{ sink.label || sink.id }}</span>
                    <span class="sink-card-kind">{{ sink.config.kind.toUpperCase() }}</span>
                    <span v-if="store.defaultSinkId === sink.id" class="sink-card-badge">
                      <Star :size="10" fill="currentColor" />
                      {{ $t('cloudSinks.defaultBadge') }}
                    </span>
                  </div>
                  <div class="sink-card-meta" :title="sinkMeta(sink)">
                    {{ sinkMeta(sink) }}
                  </div>
                </div>
                <div class="sink-card-actions">
                  <Button
                    size="sm"
                    variant="ghost"
                    :disabled="testingId === sink.id"
                    @click="onTest(sink.id)"
                  >
                    <Loader2 v-if="testingId === sink.id" :size="13" class="animate-spin" />
                    <Plug v-else :size="13" />
                    <span class="ml-1">{{ $t('cloudSinks.test') }}</span>
                  </Button>
                  <Button
                    v-if="store.defaultSinkId !== sink.id"
                    size="sm"
                    variant="ghost"
                    @click="store.setDefaultSink(sink.id)"
                  >
                    <Star :size="13" />
                    <span class="ml-1">{{ $t('cloudSinks.makeDefault') }}</span>
                  </Button>
                  <Button size="sm" variant="ghost" @click="openEdit(sink)">
                    <Pencil :size="13" />
                    <span class="ml-1">{{ $t('cloudSinks.edit') }}</span>
                  </Button>
                  <Button
                    size="sm"
                    variant="ghost"
                    class="action-danger"
                    :aria-label="$t('cloudSinks.delete')"
                    :title="`${$t('cloudSinks.delete')} — ${sink.label}`"
                    @click="removingSink = sink"
                  >
                    <Trash2 :size="13" />
                  </Button>
                </div>
              </div>
            </div>
          </div>
        </div>

        <div class="settings-section" data-preference-search-target="cloudSinks.rules">
          <div class="settings-section-header">
            <div class="section-icon"><Filter :size="16" /></div>
            <div class="section-title">
              <h3>{{ $t('cloudSinks.rules') }}</h3>
              <p class="section-desc">{{ $t('cloudSinks.rulesHint') }}</p>
            </div>
            <Button
              size="sm"
              :disabled="store.sinks.length === 0"
              @click="openRuleCreate"
            >
              <Plus :size="14" class="mr-1" />
              {{ $t('cloudSinks.addRule') }}
            </Button>
          </div>
          <div class="settings-section-content">
            <div
              v-if="store.sinks.length === 0"
              class="empty-state empty-state--small"
            >
              <Filter :size="22" class="empty-state-icon" />
              <p class="empty-state-hint">
                {{ $t('cloudSinks.rulesNeedSink') }}
              </p>
            </div>
            <div
              v-else-if="store.rules.length === 0"
              class="empty-state empty-state--small"
            >
              <Filter :size="22" class="empty-state-icon" />
              <p class="empty-state-hint">{{ $t('cloudSinks.rulesEmpty') }}</p>
            </div>
            <div v-else class="sink-list">
              <div
                v-for="rule in store.rules"
                :key="rule.id"
                class="sink-card"
                :class="{ 'sink-card--disabled': !rule.enabled }"
              >
                <div class="sink-card-icon">
                  <Filter :size="16" />
                </div>
                <div class="sink-card-main">
                  <div class="sink-card-line">
                    <span class="sink-card-label">{{ rule.label || $t('cloudSinks.unnamedRule') }}</span>
                    <span class="sink-card-kind">→ {{ ruleSinkLabel(rule.sinkId) }}</span>
                    <span v-if="!rule.enabled" class="sink-card-badge">
                      {{ $t('cloudSinks.disabled') }}
                    </span>
                  </div>
                  <div class="sink-card-meta" :title="ruleMeta(rule)">
                    {{ ruleMeta(rule) }}
                  </div>
                </div>
                <div class="sink-card-actions">
                  <Button
                    size="sm"
                    variant="ghost"
                    @click="toggleRuleEnabled(rule)"
                  >
                    {{ rule.enabled ? $t('cloudSinks.disable') : $t('cloudSinks.enable') }}
                  </Button>
                  <Button size="sm" variant="ghost" @click="openRuleEdit(rule)">
                    <Pencil :size="13" />
                    <span class="ml-1">{{ $t('cloudSinks.edit') }}</span>
                  </Button>
                  <Button
                    size="sm"
                    variant="ghost"
                    class="action-danger"
                    :aria-label="$t('cloudSinks.deleteRule')"
                    :title="`${$t('cloudSinks.deleteRule')} — ${rule.label || $t('cloudSinks.unnamedRule')}`"
                    @click="removingRule = rule"
                  >
                    <Trash2 :size="13" />
                  </Button>
                </div>
              </div>
            </div>
          </div>
        </div>

        <div class="settings-section" data-preference-search-target="cloudSinks.recentJobs">
          <div class="settings-section-header">
            <div class="section-icon"><Activity :size="16" /></div>
            <div class="section-title">
              <h3>{{ $t('cloudSinks.recentJobs') }}</h3>
              <p class="section-desc">{{ $t('cloudSinks.recentJobsHint') }}</p>
            </div>
            <Button
              size="sm"
              variant="outline"
              :disabled="store.jobs.length === 0"
              @click="store.clearHistory()"
            >
              {{ $t('cloudSinks.clearHistory') }}
            </Button>
          </div>
          <div class="settings-section-content">
            <div v-if="store.jobs.length === 0" class="empty-state empty-state--small">
              <Inbox :size="22" class="empty-state-icon" />
              <p class="empty-state-hint">{{ $t('cloudSinks.noJobs') }}</p>
            </div>
            <div v-else class="job-list">
              <div
                v-for="job in store.jobs.slice(0, 20)"
                :key="job.id"
                class="job-row"
              >
                <span class="job-pill" :data-status="job.status">{{
                  jobStatusLabel(job.status)
                }}</span>
                <div class="job-meta">
                  <div class="job-name" :title="jobName(job)">{{ jobName(job) }}</div>
                  <Progress :model-value="jobPercent(job)" class="job-progress" />
                </div>
                <div class="job-stats">
                  <span class="job-percent">{{ jobPercent(job) }}%</span>
                  <span class="job-size">{{ jobSize(job) }}</span>
                </div>
                <div class="job-actions">
                  <Button
                    v-if="job.status === 'active' || job.status === 'queued'"
                    size="sm"
                    variant="ghost"
                    :aria-label="$t('cloudSinks.cancelJob')"
                    :title="`${$t('cloudSinks.cancelJob')} — ${jobName(job)}`"
                    @click="store.cancelJob(job.id)"
                  >
                    <X :size="13" />
                  </Button>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </main>

    <Dialog
      :open="dialogOpen"
      @update:open="(v: boolean) => { if (!v) dialogOpen = false }"
    >
      <DialogContent class="cloud-sink-dialog" :show-close-button="true">
        <DialogHeader>
          <DialogTitle>{{ dialogTitle }}</DialogTitle>
          <p class="dialog-subtitle">{{ $t('cloudSinks.sinksHint') }}</p>
        </DialogHeader>

        <div class="dialog-form">
          <div class="grid-2">
            <div class="dialog-field">
              <Label for="cs-label">{{ $t('cloudSinks.label') }}</Label>
              <Input
                id="cs-label"
                v-model="form.label"
                :placeholder="$t('cloudSinks.labelPlaceholder')"
                autocomplete="off"
              />
            </div>
            <div class="dialog-field">
              <Label for="cs-protocol">{{ $t('cloudSinks.protocol') }}</Label>
              <Select v-model="form.kind">
                <SelectTrigger id="cs-protocol">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="webdav">WebDAV</SelectItem>
                  <SelectItem value="s3">S3</SelectItem>
                  <SelectItem value="sftp">SFTP</SelectItem>
                  <SelectItem value="ftp">FTP / FTPS</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>

          <template v-if="form.kind === 'webdav'">
            <div class="dialog-field">
              <Label for="cs-endpoint">{{ $t('cloudSinks.endpoint') }}</Label>
              <Input
                id="cs-endpoint"
                v-model="form.webdav.endpoint"
                placeholder="https://dav.example.com"
                autocomplete="off"
              />
            </div>
            <div class="dialog-field">
              <Label for="cs-base">{{ $t('cloudSinks.basePath') }}</Label>
              <Input
                id="cs-base"
                v-model="form.webdav.basePath"
                placeholder="/risuko"
                autocomplete="off"
              />
              <p class="dialog-hint">{{ $t('cloudSinks.basePathHint') }}</p>
            </div>
            <div class="grid-2">
              <div class="dialog-field">
                <Label for="cs-user">{{ $t('cloudSinks.username') }}</Label>
                <Input id="cs-user" v-model="form.webdav.username" autocomplete="off" />
              </div>
              <div class="dialog-field">
                <Label for="cs-pass">{{ $t('cloudSinks.password') }}</Label>
                <Input
                  id="cs-pass"
                  v-model="form.webdav.password"
                  type="password"
                  autocomplete="new-password"
                />
              </div>
            </div>
            <div class="dialog-field-toggle">
              <ui-checkbox v-model="form.webdav.insecure">
                <span class="toggle-title">{{ $t('cloudSinks.allowInsecure') }}</span>
                <span class="toggle-hint">{{ $t('cloudSinks.allowInsecureHint') }}</span>
              </ui-checkbox>
            </div>
          </template>

          <template v-else-if="form.kind === 's3'">
            <div class="dialog-field">
              <Label>{{ $t('cloudSinks.s3Endpoint') }}</Label>
              <Input
                v-model="form.s3.endpoint"
                :placeholder="$t('cloudSinks.s3EndpointPlaceholder')"
                autocomplete="off"
              />
            </div>
            <div class="grid-2">
              <div class="dialog-field">
                <Label>{{ $t('cloudSinks.s3Region') }}</Label>
                <Input
                  v-model="form.s3.region"
                  :placeholder="$t('cloudSinks.s3RegionPlaceholder')"
                  autocomplete="off"
                />
              </div>
              <div class="dialog-field">
                <Label>{{ $t('cloudSinks.s3Bucket') }}</Label>
                <Input v-model="form.s3.bucket" autocomplete="off" />
              </div>
            </div>
            <div class="grid-2">
              <div class="dialog-field">
                <Label>{{ $t('cloudSinks.s3AccessKey') }}</Label>
                <Input v-model="form.s3.accessKeyId" autocomplete="off" />
              </div>
              <div class="dialog-field">
                <Label>{{ $t('cloudSinks.s3SecretKey') }}</Label>
                <Input
                  v-model="form.s3.secretAccessKey"
                  type="password"
                  autocomplete="new-password"
                />
              </div>
            </div>
            <div class="dialog-field">
              <Label>{{ $t('cloudSinks.s3Prefix') }}</Label>
              <Input v-model="form.s3.prefix" placeholder="uploads/" autocomplete="off" />
              <p class="dialog-hint">{{ $t('cloudSinks.s3PrefixHint') }}</p>
            </div>
            <div class="dialog-field-toggle">
              <ui-checkbox v-model="form.s3.forcePathStyle">
                <span class="toggle-title">{{ $t('cloudSinks.s3PathStyle') }}</span>
                <span class="toggle-hint">{{ $t('cloudSinks.s3PathStyleHint') }}</span>
              </ui-checkbox>
            </div>
          </template>

          <template v-else-if="form.kind === 'sftp'">
            <div class="grid-host-port">
              <div class="dialog-field">
                <Label>{{ $t('cloudSinks.host') }}</Label>
                <Input v-model="form.sftp.host" placeholder="sftp.example.com" autocomplete="off" />
              </div>
              <div class="dialog-field">
                <Label>{{ $t('cloudSinks.port') }}</Label>
                <Input
                  v-model.number="form.sftp.port"
                  type="number"
                  min="1"
                  max="65535"
                  placeholder="22"
                />
              </div>
            </div>
            <div class="grid-2">
              <div class="dialog-field">
                <Label>{{ $t('cloudSinks.username') }}</Label>
                <Input v-model="form.sftp.username" autocomplete="off" />
              </div>
              <div class="dialog-field">
                <Label>{{ $t('cloudSinks.password') }}</Label>
                <Input
                  v-model="form.sftp.password"
                  type="password"
                  autocomplete="new-password"
                />
              </div>
            </div>
            <div class="dialog-field">
              <Label>{{ $t('cloudSinks.sftpBasePath') }}</Label>
              <Input
                v-model="form.sftp.basePath"
                :placeholder="$t('cloudSinks.sftpBasePathPlaceholder')"
                autocomplete="off"
              />
            </div>
            <div class="dialog-field">
              <Label>{{ $t('cloudSinks.privateKey') }}</Label>
              <textarea
                v-model="form.sftp.privateKey"
                class="dialog-textarea"
                rows="3"
                placeholder="-----BEGIN OPENSSH PRIVATE KEY-----"
                autocomplete="off"
                spellcheck="false"
              />
              <p class="dialog-hint">{{ $t('cloudSinks.privateKeyHint') }}</p>
            </div>
          </template>

          <template v-else-if="form.kind === 'ftp'">
            <div class="grid-host-port">
              <div class="dialog-field">
                <Label>{{ $t('cloudSinks.host') }}</Label>
                <Input v-model="form.ftp.host" placeholder="ftp.example.com" autocomplete="off" />
              </div>
              <div class="dialog-field">
                <Label>{{ $t('cloudSinks.port') }}</Label>
                <Input
                  v-model.number="form.ftp.port"
                  type="number"
                  min="1"
                  max="65535"
                  placeholder="21"
                />
              </div>
            </div>
            <div class="grid-2">
              <div class="dialog-field">
                <Label>{{ $t('cloudSinks.username') }}</Label>
                <Input v-model="form.ftp.username" autocomplete="off" />
              </div>
              <div class="dialog-field">
                <Label>{{ $t('cloudSinks.password') }}</Label>
                <Input
                  v-model="form.ftp.password"
                  type="password"
                  autocomplete="new-password"
                />
              </div>
            </div>
            <div class="dialog-field">
              <Label>{{ $t('cloudSinks.basePath') }}</Label>
              <Input v-model="form.ftp.basePath" placeholder="/uploads" autocomplete="off" />
              <p class="dialog-hint">{{ $t('cloudSinks.ftpBasePathHint') }}</p>
            </div>
            <div class="dialog-field-toggle">
              <ui-checkbox v-model="form.ftp.secure">
                <span class="toggle-title">{{ $t('cloudSinks.ftpSecure') }}</span>
                <span class="toggle-hint">{{ $t('cloudSinks.ftpSecureHint') }}</span>
              </ui-checkbox>
            </div>
            <div v-if="form.ftp.secure" class="dialog-field-toggle">
              <ui-checkbox v-model="form.ftp.insecure">
                <span class="toggle-title">{{ $t('cloudSinks.allowInsecure') }}</span>
                <span class="toggle-hint">{{ $t('cloudSinks.allowInsecureHint') }}</span>
              </ui-checkbox>
            </div>
          </template>

          <hr class="dialog-sep" />

          <div class="grid-2">
            <div class="dialog-field">
              <Label>{{ $t('cloudSinks.postAction') }}</Label>
              <Select v-model="form.postAction">
                <SelectTrigger>
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="keep">{{ $t('cloudSinks.postKeep') }}</SelectItem>
                  <SelectItem value="trash">{{ $t('cloudSinks.postTrash') }}</SelectItem>
                  <SelectItem value="move">{{ $t('cloudSinks.postMove') }}</SelectItem>
                </SelectContent>
              </Select>
            </div>
            <div v-if="form.postAction === 'move'" class="dialog-field">
              <Label>{{ $t('cloudSinks.moveTarget') }}</Label>
              <Input
                v-model="form.moveTarget"
                :placeholder="$t('cloudSinks.moveTargetPlaceholder')"
              />
            </div>
          </div>

          <div v-if="dialogError" class="dialog-error">
            <AlertCircle :size="14" class="shrink-0" />
            <span>{{ dialogError }}</span>
          </div>
        </div>

        <DialogFooter>
          <Button variant="outline" :disabled="saving" @click="dialogOpen = false">
            {{ $t('cloudSinks.cancel') }}
          </Button>
          <Button :disabled="saving" @click="onSave">
            <Loader2 v-if="saving" :size="13" class="animate-spin mr-1" />
            {{ saving ? $t('cloudSinks.saving') : $t('cloudSinks.save') }}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>

    <Dialog
      :open="!!removingSink"
      @update:open="(v: boolean) => { if (!v) removingSink = null }"
    >
      <DialogContent class="cloud-sink-confirm" :show-close-button="false">
        <DialogHeader>
          <DialogTitle>{{ $t('cloudSinks.confirmRemoveTitle') }}</DialogTitle>
          <p class="dialog-subtitle">{{ $t('cloudSinks.confirmRemove') }}</p>
        </DialogHeader>
        <div v-if="removingSink" class="confirm-target">
          <strong>{{ removingSink.label || removingSink.id }}</strong>
          <span class="confirm-target-meta">{{ sinkMeta(removingSink) }}</span>
        </div>
        <DialogFooter>
          <Button variant="outline" @click="removingSink = null">
            {{ $t('cloudSinks.cancel') }}
          </Button>
          <Button variant="destructive" @click="onRemoveConfirmed">
            {{ $t('cloudSinks.delete') }}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>

    <Dialog
      :open="ruleDialogOpen"
      @update:open="(v: boolean) => { if (!v) ruleDialogOpen = false }"
    >
      <DialogContent class="cloud-sink-dialog" :show-close-button="true">
        <DialogHeader>
          <DialogTitle>{{ ruleDialogTitle }}</DialogTitle>
          <p class="dialog-subtitle">{{ $t('cloudSinks.rulesHint') }}</p>
        </DialogHeader>

        <div class="dialog-form">
          <div class="grid-2">
            <div class="dialog-field">
              <Label for="rule-label">{{ $t('cloudSinks.label') }}</Label>
              <Input
                id="rule-label"
                v-model="ruleForm.label"
                :placeholder="$t('cloudSinks.ruleLabelPlaceholder')"
                autocomplete="off"
              />
            </div>
            <div class="dialog-field">
              <Label for="rule-target">{{ $t('cloudSinks.ruleTarget') }}</Label>
              <Select v-model="ruleForm.sinkId">
                <SelectTrigger id="rule-target">
                  <SelectValue
                    :placeholder="$t('cloudSinks.ruleTargetPlaceholder')"
                  />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem
                    v-for="s in store.sinks"
                    :key="s.id"
                    :value="s.id"
                  >
                    {{ s.label || s.id }}
                  </SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>
          <p
            v-if="store.sinks.length === 0"
            class="dialog-hint dialog-hint--warn"
          >
            {{ $t('cloudSinks.rulesNeedSink') }}
          </p>

          <hr class="dialog-sep" />

          <div class="dialog-field">
            <Label>{{ $t('cloudSinks.ruleCategories') }}</Label>
            <DropdownMenu>
              <DropdownMenuTrigger as-child>
                <button type="button" class="multi-select">
                  <div class="multi-select__body">
                    <template v-if="ruleForm.categories.length">
                      <span
                        v-for="c in ruleForm.categories"
                        :key="c"
                        class="multi-select__tag"
                      >
                        {{ $t(`cloudSinks.cat_${c}`) }}
                        <X
                          :size="11"
                          class="multi-select__tag-x"
                          @click.stop="toggleSelection(ruleForm.categories, c)"
                        />
                      </span>
                    </template>
                    <span v-else class="multi-select__placeholder">
                      {{ $t('cloudSinks.selectCategories') }}
                    </span>
                  </div>
                  <ChevronDown :size="14" class="multi-select__chevron" />
                </button>
              </DropdownMenuTrigger>
              <DropdownMenuContent align="start" class="multi-select__menu">
                <DropdownMenuCheckboxItem
                  v-for="c in CATEGORY_OPTIONS"
                  :key="c"
                  :model-value="ruleForm.categories.includes(c)"
                  @select="(e: Event) => e.preventDefault()"
                  @update:model-value="toggleSelection(ruleForm.categories, c)"
                >
                  {{ $t(`cloudSinks.cat_${c}`) }}
                </DropdownMenuCheckboxItem>
              </DropdownMenuContent>
            </DropdownMenu>
          </div>

          <div class="dialog-field">
            <Label>{{ $t('cloudSinks.ruleTaskKinds') }}</Label>
            <DropdownMenu>
              <DropdownMenuTrigger as-child>
                <button type="button" class="multi-select">
                  <div class="multi-select__body">
                    <template v-if="ruleForm.taskKinds.length">
                      <span
                        v-for="k in ruleForm.taskKinds"
                        :key="k"
                        class="multi-select__tag"
                      >
                        {{ k.toUpperCase() }}
                        <X
                          :size="11"
                          class="multi-select__tag-x"
                          @click.stop="toggleSelection(ruleForm.taskKinds, k)"
                        />
                      </span>
                    </template>
                    <span v-else class="multi-select__placeholder">
                      {{ $t('cloudSinks.selectTaskKinds') }}
                    </span>
                  </div>
                  <ChevronDown :size="14" class="multi-select__chevron" />
                </button>
              </DropdownMenuTrigger>
              <DropdownMenuContent align="start" class="multi-select__menu">
                <DropdownMenuCheckboxItem
                  v-for="k in TASK_KIND_OPTIONS"
                  :key="k"
                  :model-value="ruleForm.taskKinds.includes(k)"
                  @select="(e: Event) => e.preventDefault()"
                  @update:model-value="toggleSelection(ruleForm.taskKinds, k)"
                >
                  {{ k.toUpperCase() }}
                </DropdownMenuCheckboxItem>
              </DropdownMenuContent>
            </DropdownMenu>
          </div>

          <div class="grid-2">
            <div class="dialog-field">
              <Label for="rule-ext">{{ $t('cloudSinks.ruleExtensions') }}</Label>
              <Input
                id="rule-ext"
                v-model="ruleForm.extensionsText"
                placeholder="mp4, mkv, iso"
                autocomplete="off"
                spellcheck="false"
              />
              <div v-if="parsedExtensions.length" class="ext-tags">
                <span
                  v-for="e in parsedExtensions"
                  :key="e"
                  class="ext-tag"
                >.{{ e }}</span>
              </div>
            </div>
            <div class="dialog-field">
              <div class="field-row">
                <Label>{{ $t('cloudSinks.ruleSize') }}</Label>
                <span class="field-row__count">{{ sizeRangeSummary }}</span>
              </div>
              <div class="size-range">
                <div class="input-suffix">
                  <Input
                    id="rule-min"
                    v-model="ruleForm.minSizeMb"
                    type="number"
                    min="0"
                    :placeholder="$t('cloudSinks.ruleAnySize')"
                    autocomplete="off"
                  />
                  <span class="input-suffix__unit">MB</span>
                </div>
                <span class="size-range__sep">→</span>
                <div class="input-suffix">
                  <Input
                    id="rule-max"
                    v-model="ruleForm.maxSizeMb"
                    type="number"
                    min="0"
                    :placeholder="$t('cloudSinks.ruleAnySize')"
                    autocomplete="off"
                  />
                  <span class="input-suffix__unit">MB</span>
                </div>
              </div>
            </div>
          </div>

          <p class="dialog-hint">{{ $t('cloudSinks.ruleConditionsHint') }}</p>

          <hr class="dialog-sep" />

          <div class="dialog-field-toggle">
            <ui-checkbox v-model="ruleForm.enabled">
              <span class="toggle-title">{{ $t('cloudSinks.ruleEnabled') }}</span>
              <span class="toggle-hint">{{ $t('cloudSinks.ruleEnabledHint') }}</span>
            </ui-checkbox>
          </div>

          <div v-if="ruleDialogError" class="dialog-error">
            <AlertCircle :size="14" class="shrink-0" />
            <span>{{ ruleDialogError }}</span>
          </div>
        </div>

        <DialogFooter>
          <Button
            variant="outline"
            :disabled="ruleSaving"
            @click="ruleDialogOpen = false"
          >
            {{ $t('cloudSinks.cancel') }}
          </Button>
          <Button
            :disabled="ruleSaving || store.sinks.length === 0"
            @click="onSaveRule"
          >
            <Loader2 v-if="ruleSaving" :size="13" class="animate-spin mr-1" />
            {{ ruleSaving ? $t('cloudSinks.saving') : $t('cloudSinks.save') }}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>

    <Dialog
      :open="!!removingRule"
      @update:open="(v: boolean) => { if (!v) removingRule = null }"
    >
      <DialogContent class="cloud-sink-confirm" :show-close-button="false">
        <DialogHeader>
          <DialogTitle>{{ $t('cloudSinks.confirmRemoveRuleTitle') }}</DialogTitle>
          <p class="dialog-subtitle">
            {{ $t('cloudSinks.confirmRemoveRule') }}
          </p>
        </DialogHeader>
        <div v-if="removingRule" class="confirm-target">
          <strong>{{ removingRule.label || removingRule.id }}</strong>
        </div>
        <DialogFooter>
          <Button variant="outline" @click="removingRule = null">
            {{ $t('cloudSinks.cancel') }}
          </Button>
          <Button variant="destructive" @click="onRemoveRuleConfirmed">
            {{ $t('cloudSinks.delete') }}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  </div>
</template>

<script lang="ts">
import {
	Activity,
	AlertCircle,
	ChevronDown,
	Cloud,
	CloudOff,
	Database,
	Filter,
	HardDrive,
	Inbox,
	Loader2,
	Pencil,
	Plug,
	Plus,
	Server,
	Star,
	Terminal,
	Trash2,
	X,
} from "@lucide/vue";
import type { SavedCredential } from "@shared/types/credential";
import type {
	SinkConfig,
	UploadJob,
	UploadJobStatus,
	UploadProtocol,
	UploadRule,
	UploadSinkRecord,
} from "@shared/types/upload";
import { bytesToSize } from "@shared/utils";
import logger from "@shared/utils/logger";
import { defineComponent } from "vue";
import { toast } from "vue-sonner";
import { Button } from "@/components/ui/button";
import {
	Dialog,
	DialogContent,
	DialogFooter,
	DialogHeader,
	DialogTitle,
} from "@/components/ui/dialog";
import {
	DropdownMenu,
	DropdownMenuCheckboxItem,
	DropdownMenuContent,
	DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Progress } from "@/components/ui/progress";
import {
	Select,
	SelectContent,
	SelectItem,
	SelectTrigger,
	SelectValue,
} from "@/components/ui/select";
import { usePreferenceStore } from "@/store/preference";
import { useUploadSinkStore } from "@/store/uploadSink";

interface FormState {
	id: string;
	label: string;
	kind: UploadProtocol;
	webdav: {
		endpoint: string;
		basePath: string;
		username: string;
		password: string;
		insecure: boolean;
	};
	s3: {
		endpoint: string;
		region: string;
		bucket: string;
		accessKeyId: string;
		secretAccessKey: string;
		prefix: string;
		forcePathStyle: boolean;
	};
	sftp: {
		host: string;
		port: number;
		username: string;
		password: string;
		privateKey: string;
		basePath: string;
	};
	ftp: {
		host: string;
		port: number;
		username: string;
		password: string;
		basePath: string;
		secure: boolean;
		insecure: boolean;
	};
	postAction: "keep" | "trash" | "move";
	moveTarget: string;
}

interface RuleFormState {
	id: string;
	label: string;
	sinkId: string;
	enabled: boolean;
	categories: string[];
	taskKinds: string[];
	extensionsText: string;
	minSizeMb: string;
	maxSizeMb: string;
}

const CATEGORY_OPTIONS = [
	"music",
	"video",
	"image",
	"document",
	"compressed",
	"program",
] as const;

const TASK_KIND_OPTIONS = ["http", "torrent", "m3u8", "ftp", "ed2k"] as const;

function emptyRuleForm(): RuleFormState {
	return {
		id: "",
		label: "",
		sinkId: "",
		enabled: true,
		categories: [],
		taskKinds: [],
		extensionsText: "",
		minSizeMb: "",
		maxSizeMb: "",
	};
}

function emptyForm(): FormState {
	return {
		id: "",
		label: "",
		kind: "webdav",
		webdav: {
			endpoint: "",
			basePath: "",
			username: "",
			password: "",
			insecure: false,
		},
		s3: {
			endpoint: "",
			region: "us-east-1",
			bucket: "",
			accessKeyId: "",
			secretAccessKey: "",
			prefix: "",
			forcePathStyle: false,
		},
		sftp: {
			host: "",
			port: 22,
			username: "",
			password: "",
			privateKey: "",
			basePath: "",
		},
		ftp: {
			host: "",
			port: 21,
			username: "",
			password: "",
			basePath: "",
			secure: false,
			insecure: false,
		},
		postAction: "keep",
		moveTarget: "",
	};
}

function sameEndpoint(a: string, b: string): boolean {
	try {
		return (
			new URL(a.trim().replace(/\/+$/, "")).href ===
			new URL(b.trim().replace(/\/+$/, "")).href
		);
	} catch {
		return false;
	}
}

function sameHost(a: string, b: string): boolean {
	return a.trim().toLowerCase() === b.trim().toLowerCase();
}

function sameSinkServer(a: SinkConfig, b: SinkConfig): boolean {
	if (a.kind === "webdav" && b.kind === "webdav") {
		return sameEndpoint(a.endpoint, b.endpoint);
	}
	if (a.kind === "s3" && b.kind === "s3") {
		return (
			sameEndpoint(a.endpoint, b.endpoint) &&
			a.bucket.trim() === b.bucket.trim()
		);
	}
	if (a.kind === "sftp" && b.kind === "sftp") {
		return sameHost(a.host, b.host) && a.port === b.port;
	}
	if (a.kind === "ftp" && b.kind === "ftp") {
		return (
			sameHost(a.host, b.host) && a.port === b.port && !!a.secure === !!b.secure
		);
	}
	return false;
}

function secretMissing(config: SinkConfig): boolean {
	switch (config.kind) {
		case "webdav":
			return !!config.username && !config.password;
		case "s3":
			return !config.secretAccessKey;
		case "sftp":
			return !config.password && !config.privateKey;
		case "ftp":
			return !!config.username && !config.password;
	}
}

export default defineComponent({
	name: "preference-cloud-sinks",
	components: {
		Activity,
		AlertCircle,
		Button,
		ChevronDown,
		Cloud,
		CloudOff,
		Dialog,
		DialogContent,
		DialogFooter,
		DialogHeader,
		DialogTitle,
		DropdownMenu,
		DropdownMenuCheckboxItem,
		DropdownMenuContent,
		DropdownMenuTrigger,
		Filter,
		Inbox,
		Input,
		Label,
		Loader2,
		Pencil,
		Plug,
		Plus,
		Progress,
		Select,
		SelectContent,
		SelectItem,
		SelectTrigger,
		SelectValue,
		Star,
		Trash2,
		X,
	},
	data() {
		return {
			dialogOpen: false,
			editingId: null as string | null,
			form: emptyForm(),
			saving: false,
			dialogError: "" as string,
			testingId: null as string | null,
			removingSink: null as UploadSinkRecord | null,
			ruleDialogOpen: false,
			ruleEditingId: null as string | null,
			ruleForm: emptyRuleForm(),
			ruleSaving: false,
			ruleDialogError: "" as string,
			removingRule: null as UploadRule | null,
			CATEGORY_OPTIONS,
			TASK_KIND_OPTIONS,
		};
	},
	computed: {
		store() {
			return useUploadSinkStore();
		},
		dialogTitle(): string {
			return this.editingId
				? (this.$t("cloudSinks.editSink") as string)
				: (this.$t("cloudSinks.addSink") as string);
		},
		ruleDialogTitle(): string {
			return this.ruleEditingId
				? (this.$t("cloudSinks.editRule") as string)
				: (this.$t("cloudSinks.addRule") as string);
		},
		parsedExtensions(): string[] {
			return this.ruleForm.extensionsText
				.split(/[,\s]+/)
				.map((e) => e.trim().toLowerCase().replace(/^\./, ""))
				.filter(Boolean);
		},
		sizeRangeSummary(): string {
			const minMb = Number(this.ruleForm.minSizeMb);
			const maxMb = Number(this.ruleForm.maxSizeMb);
			const hasMin = Number.isFinite(minMb) && minMb > 0;
			const hasMax = Number.isFinite(maxMb) && maxMb > 0;
			if (hasMin && hasMax) {
				return `${minMb}–${maxMb} MB`;
			}
			if (hasMin) {
				return `≥ ${minMb} MB`;
			}
			if (hasMax) {
				return `≤ ${maxMb} MB`;
			}
			return this.$t("cloudSinks.any") as string;
		},
	},
	async mounted() {
		try {
			await this.store.fetchAll();
		} catch (err) {
			logger.warn("CloudSinks: fetchAll failed", err);
		} finally {
			await this.store.initEventListeners();
		}
	},
	beforeUnmount() {
		this.store.cleanupEventListeners();
	},
	methods: {
		protocolIcon(kind: string) {
			switch (kind) {
				case "webdav":
					return Server;
				case "s3":
					return Database;
				case "sftp":
					return Terminal;
				case "ftp":
					return HardDrive;
				default:
					return Cloud;
			}
		},
		sinkMeta(sink: UploadSinkRecord): string {
			const c = sink.config;
			const joinPath = (head: string, tail?: string | null) => {
				const t = (tail ?? "").trim();
				if (!t) {
					return head;
				}
				return `${head.replace(/\/+$/, "")}/${t.replace(/^\/+/, "")}`;
			};
			switch (c.kind) {
				case "webdav":
					return joinPath(c.endpoint, c.basePath);
				case "s3":
					return joinPath(joinPath(c.endpoint, c.bucket), c.prefix);
				case "sftp":
					return joinPath(
						`sftp://${c.username}@${c.host}:${c.port}`,
						c.basePath,
					);
				case "ftp":
					return joinPath(
						`${c.secure ? "ftps" : "ftp"}://${c.username || "anonymous"}@${c.host}:${c.port}`,
						c.basePath,
					);
				default:
					return "";
			}
		},
		openCreate() {
			this.form = emptyForm();
			this.editingId = null;
			this.dialogError = "";
			this.dialogOpen = true;
		},
		openEdit(sink: UploadSinkRecord) {
			const f = emptyForm();
			f.id = sink.id;
			f.label = sink.label;
			f.postAction = sink.postAction;
			f.moveTarget = sink.moveTarget ?? "";
			f.kind = sink.config.kind;
			const c = sink.config;
			if (c.kind === "webdav") {
				f.webdav.endpoint = c.endpoint;
				f.webdav.basePath = c.basePath ?? "";
				f.webdav.username = c.username ?? "";
				f.webdav.password = c.password ?? "";
				f.webdav.insecure = !!c.insecure;
			} else if (c.kind === "s3") {
				f.s3.endpoint = c.endpoint;
				f.s3.region = c.region;
				f.s3.bucket = c.bucket;
				f.s3.accessKeyId = c.accessKeyId;
				f.s3.secretAccessKey = c.secretAccessKey ?? "";
				f.s3.prefix = c.prefix ?? "";
				f.s3.forcePathStyle = !!c.forcePathStyle;
			} else if (c.kind === "sftp") {
				f.sftp.host = c.host;
				f.sftp.port = c.port || 22;
				f.sftp.username = c.username;
				f.sftp.password = c.password ?? "";
				f.sftp.privateKey = c.privateKey ?? "";
				f.sftp.basePath = c.basePath ?? "";
			} else if (c.kind === "ftp") {
				f.ftp.host = c.host;
				f.ftp.port = c.port || 21;
				f.ftp.username = c.username ?? "";
				f.ftp.password = c.password ?? "";
				f.ftp.basePath = c.basePath ?? "";
				f.ftp.secure = !!c.secure;
				f.ftp.insecure = !!c.insecure;
			}
			this.form = f;
			this.editingId = sink.id;
			this.dialogError = "";
			this.dialogOpen = true;
		},
		buildConfig(): SinkConfig {
			switch (this.form.kind) {
				case "webdav":
					return {
						kind: "webdav",
						endpoint: this.form.webdav.endpoint.trim(),
						basePath: this.form.webdav.basePath.trim(),
						username: this.form.webdav.username || "",
						password: this.form.webdav.password || "",
						insecure: this.form.webdav.insecure,
					};
				case "s3": {
					const secret = this.form.s3.secretAccessKey.trim();
					return {
						kind: "s3",
						endpoint: this.form.s3.endpoint.trim(),
						region: this.form.s3.region.trim(),
						bucket: this.form.s3.bucket.trim(),
						accessKeyId: this.form.s3.accessKeyId.trim(),
						...(secret ? { secretAccessKey: secret } : {}),
						prefix: this.form.s3.prefix.trim(),
						forcePathStyle: this.form.s3.forcePathStyle,
					};
				}
				case "sftp":
					return {
						kind: "sftp",
						host: this.form.sftp.host.trim(),
						port: Number(this.form.sftp.port) || 22,
						username: this.form.sftp.username.trim(),
						password: this.form.sftp.password || "",
						privateKey: this.form.sftp.privateKey || "",
						basePath: this.form.sftp.basePath.trim(),
					};
				case "ftp":
					return {
						kind: "ftp",
						host: this.form.ftp.host.trim(),
						port: Number(this.form.ftp.port) || 21,
						username: this.form.ftp.username || "",
						password: this.form.ftp.password || "",
						basePath: this.form.ftp.basePath.trim(),
						secure: this.form.ftp.secure,
						insecure: this.form.ftp.insecure,
					};
			}
		},
		secretNeededForNewServer(): boolean {
			const saved = this.editingId
				? this.store.sinks.find((s) => s.id === this.editingId)
				: undefined;
			if (!saved) {
				return false;
			}
			const config = this.buildConfig();
			return !sameSinkServer(saved.config, config) && secretMissing(config);
		},
		validate(): string | null {
			if (!this.form.label.trim()) {
				return this.$t("cloudSinks.errLabelRequired") as string;
			}
			switch (this.form.kind) {
				case "webdav":
					if (!this.form.webdav.endpoint.trim()) {
						return this.$t("cloudSinks.errEndpointRequired") as string;
					}
					break;
				case "s3":
					if (!this.form.s3.endpoint.trim()) {
						return this.$t("cloudSinks.errS3EndpointRequired") as string;
					}
					if (!this.form.s3.bucket.trim()) {
						return this.$t("cloudSinks.errS3BucketRequired") as string;
					}
					if (!this.form.s3.accessKeyId.trim()) {
						return this.$t("cloudSinks.errS3AccessKeyRequired") as string;
					}
					if (!this.editingId && !this.form.s3.secretAccessKey.trim()) {
						return this.$t("cloudSinks.errS3SecretKeyRequired") as string;
					}
					break;
				case "sftp":
					if (!this.form.sftp.host.trim()) {
						return this.$t("cloudSinks.errSftpHostRequired") as string;
					}
					if (!this.form.sftp.username.trim()) {
						return this.$t("cloudSinks.errSftpUsernameRequired") as string;
					}
					if (
						!this.editingId &&
						!this.form.sftp.password &&
						!this.form.sftp.privateKey
					) {
						return this.$t("cloudSinks.errSftpAuthRequired") as string;
					}
					break;
				case "ftp":
					if (!this.form.ftp.host.trim()) {
						return this.$t("cloudSinks.errFtpHostRequired") as string;
					}
					break;
			}
			if (this.secretNeededForNewServer()) {
				return this.$t("cloudSinks.errSecretForNewServer") as string;
			}
			if (this.form.postAction === "move" && !this.form.moveTarget.trim()) {
				return this.$t("cloudSinks.errMoveTargetRequired") as string;
			}
			return null;
		},
		async onSave() {
			this.dialogError = "";
			const err = this.validate();
			if (err) {
				this.dialogError = err;
				return;
			}
			this.saving = true;
			let savedSink: UploadSinkRecord | null = null;
			try {
				const config = this.buildConfig();
				if (this.editingId) {
					const existing = this.store.sinks.find(
						(s) => s.id === this.editingId,
					);
					if (!existing) {
						throw new Error("Sink no longer exists");
					}
					const updated: UploadSinkRecord = {
						...existing,
						label: this.form.label.trim(),
						config,
						postAction: this.form.postAction,
						moveTarget: this.form.moveTarget || null,
					};
					await this.store.updateSink(updated);
					savedSink = updated;
				} else {
					const created = await this.store.addSink({
						label: this.form.label.trim(),
						config,
						postAction: this.form.postAction,
						moveTarget: this.form.moveTarget || null,
					});
					savedSink = created;
				}
				this.dialogOpen = false;
			} catch (e) {
				this.dialogError = String((e as Error)?.message || e);
				this.saving = false;
				return;
			}

			if (savedSink) {
				try {
					await this.syncCredential(savedSink);
				} catch (e) {
					this.$msg.error(
						this.$t("cloudSinks.credentialSyncFailed") +
							": " +
							String((e as Error)?.message || e),
					);
				}
			}
			this.saving = false;
		},
		async onRemoveConfirmed() {
			if (!this.removingSink) {
				return;
			}
			const id = this.removingSink.id;
			const label = this.removingSink.label || id;
			this.removingSink = null;
			try {
				await this.store.removeSink(id);
			} catch (e) {
				toast.error(String((e as Error)?.message || e));
				return;
			}
			toast.success(this.$t("cloudSinks.removed", { label }) as string);
			try {
				await usePreferenceStore().removeCredential(id);
			} catch (e) {
				const msg = String((e as Error)?.message || e);
				toast.error(msg);
				logger.warn("[Risuko] Failed to remove credential for sink", id, msg);
			}
		},
		async onTest(id: string) {
			this.testingId = id;
			try {
				await this.store.testSink(id);
				toast.success(this.$t("cloudSinks.testOk") as string);
			} catch (e) {
				toast.error(
					`${this.$t("cloudSinks.testFail")}: ${(e as Error)?.message || e}`,
				);
			} finally {
				this.testingId = null;
			}
		},
		mapSinkToCredential(record: UploadSinkRecord): SavedCredential | null {
			const base: Partial<SavedCredential> = {
				id: record.id,
				label: record.label,
			};
			const c = record.config;
			if (c.kind === "sftp") {
				return {
					...base,
					host: c.host,
					protocol: "sftp",
					ftpUser: c.username,
					ftpPasswd: c.password ?? "",
					sftpPrivateKey: c.privateKey ?? "",
					sftpPrivateKeyContent: c.privateKey ?? "",
				} as SavedCredential;
			}
			if (c.kind === "ftp") {
				return {
					...base,
					host: c.host,
					protocol: "ftp",
					ftpUser: c.username || "",
					ftpPasswd: c.password ?? "",
				} as SavedCredential;
			}
			return null;
		},
		async syncCredential(record: UploadSinkRecord) {
			const preferenceStore = usePreferenceStore();
			const mapped = this.mapSinkToCredential(record);
			if (!mapped) {
				await preferenceStore.removeCredential(record.id);
				return;
			}
			const existing = preferenceStore
				.getSavedCredentials()
				.find((c: SavedCredential) => c.id === record.id);
			if (existing) {
				const credential: SavedCredential = {
					...existing,
					...mapped,
					createdAt: existing.createdAt,
					lastUsedAt: existing.lastUsedAt,
				};
				await preferenceStore.saveCredential(credential);
			} else {
				const now = Date.now();
				await preferenceStore.saveCredential({
					...mapped,
					createdAt: now,
					lastUsedAt: now,
				});
			}
		},
		jobStatusLabel(status: UploadJobStatus): string {
			const map: Record<UploadJobStatus, string> = {
				queued: "cloudSinks.jobStatusQueued",
				active: "cloudSinks.jobStatusActive",
				complete: "cloudSinks.jobStatusComplete",
				failed: "cloudSinks.jobStatusFailed",
				cancelled: "cloudSinks.jobStatusCancelled",
			};
			return this.$t(map[status]) as string;
		},
		jobName(job: UploadJob): string {
			return job.remoteRelative || job.localPath;
		},
		jobPercent(job: UploadJob): number {
			if (job.status === "complete") {
				return 100;
			}
			if (!job.size) {
				return 0;
			}
			return Math.min(100, Math.round((job.uploaded / job.size) * 100));
		},
		jobSize(job: UploadJob): string {
			if (!job.size) {
				return "—";
			}
			if (
				job.status === "complete" ||
				job.status === "failed" ||
				job.status === "cancelled"
			) {
				return bytesToSize(job.size) as string;
			}
			return `${bytesToSize(job.uploaded)} / ${bytesToSize(job.size)}`;
		},
		toggleSelection(arr: string[], value: string) {
			const idx = arr.indexOf(value);
			if (idx >= 0) {
				arr.splice(idx, 1);
			} else {
				arr.push(value);
			}
		},
		ruleSinkLabel(sinkId: string): string {
			const s = this.store.sinks.find((x) => x.id === sinkId);
			return s?.label || s?.id || sinkId;
		},
		ruleMeta(rule: UploadRule): string {
			const m = rule.match;
			const parts: string[] = [];
			if (m.taskKinds.length) {
				parts.push(m.taskKinds.map((k) => k.toUpperCase()).join("/"));
			}
			if (m.categories.length) {
				parts.push(
					m.categories
						.map((c) => {
							const key = `cloudSinks.cat_${c}`;
							const t = this.$t(key) as string;
							return t === key ? c : t;
						})
						.join(", "),
				);
			}
			if (m.extensions.length) {
				parts.push(m.extensions.map((e) => `.${e}`).join(" "));
			}
			if (m.minSize != null && m.minSize > 0) {
				parts.push(`≥ ${bytesToSize(m.minSize)}`);
			}
			if (m.maxSize != null && m.maxSize > 0) {
				parts.push(`≤ ${bytesToSize(m.maxSize)}`);
			}
			return parts.length
				? parts.join(" · ")
				: (this.$t("cloudSinks.ruleMatchAny") as string);
		},
		openRuleCreate() {
			this.ruleForm = emptyRuleForm();
			this.ruleForm.sinkId =
				this.store.defaultSinkId ?? this.store.sinks[0]?.id ?? "";
			this.ruleEditingId = null;
			this.ruleDialogError = "";
			this.ruleDialogOpen = true;
		},
		openRuleEdit(rule: UploadRule) {
			const f = emptyRuleForm();
			f.id = rule.id;
			f.label = rule.label;
			f.sinkId = rule.sinkId;
			f.enabled = rule.enabled;
			f.categories = [...rule.match.categories];
			f.taskKinds = [...rule.match.taskKinds];
			f.extensionsText = rule.match.extensions.join(", ");
			f.minSizeMb =
				rule.match.minSize != null && rule.match.minSize > 0
					? String(Math.round(rule.match.minSize / (1024 * 1024)))
					: "";
			f.maxSizeMb =
				rule.match.maxSize != null && rule.match.maxSize > 0
					? String(Math.round(rule.match.maxSize / (1024 * 1024)))
					: "";
			this.ruleForm = f;
			this.ruleEditingId = rule.id;
			this.ruleDialogError = "";
			this.ruleDialogOpen = true;
		},
		buildRule(): UploadRule {
			const exts = this.parsedExtensions;
			const minMb = Number(this.ruleForm.minSizeMb);
			const maxMb = Number(this.ruleForm.maxSizeMb);
			return {
				id: this.ruleForm.id,
				label: this.ruleForm.label.trim(),
				sinkId: this.ruleForm.sinkId,
				enabled: this.ruleForm.enabled,
				match: {
					categories: [...this.ruleForm.categories],
					extensions: exts,
					taskKinds: [...this.ruleForm.taskKinds],
					minSize:
						Number.isFinite(minMb) && minMb > 0
							? Math.round(minMb * 1024 * 1024)
							: null,
					maxSize:
						Number.isFinite(maxMb) && maxMb > 0
							? Math.round(maxMb * 1024 * 1024)
							: null,
				},
			};
		},
		validateRule(): string | null {
			if (!this.ruleForm.label.trim()) {
				return this.$t("cloudSinks.errLabelRequired") as string;
			}
			if (!this.ruleForm.sinkId) {
				return this.$t("cloudSinks.errRuleSinkRequired") as string;
			}
			const minMb = Number(this.ruleForm.minSizeMb);
			const maxMb = Number(this.ruleForm.maxSizeMb);
			const hasMin = Number.isFinite(minMb) && minMb > 0;
			const hasMax = Number.isFinite(maxMb) && maxMb > 0;
			if (hasMin && hasMax && minMb > maxMb) {
				return this.$t("cloudSinks.errRuleSizeRange") as string;
			}
			return null;
		},
		async onSaveRule() {
			this.ruleDialogError = "";
			const err = this.validateRule();
			if (err) {
				this.ruleDialogError = err;
				return;
			}
			this.ruleSaving = true;
			try {
				const rule = this.buildRule();
				if (this.ruleEditingId) {
					await this.store.updateRule(rule);
				} else {
					await this.store.addRule({
						label: rule.label,
						sinkId: rule.sinkId,
						enabled: rule.enabled,
						match: rule.match,
					});
				}
				this.ruleDialogOpen = false;
			} catch (e) {
				this.ruleDialogError = String((e as Error)?.message || e);
			} finally {
				this.ruleSaving = false;
			}
		},
		async toggleRuleEnabled(rule: UploadRule) {
			try {
				await this.store.updateRule({
					...rule,
					enabled: !rule.enabled,
				});
			} catch (e) {
				toast.error(String((e as Error)?.message || e));
			}
		},
		async onRemoveRuleConfirmed() {
			if (!this.removingRule) {
				return;
			}
			const id = this.removingRule.id;
			this.removingRule = null;
			try {
				await this.store.removeRule(id);
			} catch (e) {
				toast.error(String((e as Error)?.message || e));
			}
		},
	},
});
</script>

<style scoped>
.section-desc {
	margin: 2px 0 0;
	font-size: 12px;
	color: hsl(var(--muted-foreground));
}

.sink-list {
	display: flex;
	flex-direction: column;
	gap: 8px;
}

.sink-card {
	display: flex;
	align-items: center;
	gap: 12px;
	padding: 12px 14px;
	border: 1px solid hsl(var(--border));
	border-radius: 10px;
	background: hsl(var(--card));
	transition:
		border-color 0.15s ease,
		background-color 0.15s ease,
		box-shadow 0.15s ease;
}

.sink-card:hover {
	border-color: hsl(var(--ring) / 0.5);
	background: hsl(var(--accent) / 0.4);
}

.sink-card--default {
	border-color: hsl(var(--primary) / 0.4);
	box-shadow: 0 0 0 1px hsl(var(--primary) / 0.15);
}

.sink-card-icon {
	width: 36px;
	height: 36px;
	flex: 0 0 36px;
	display: flex;
	align-items: center;
	justify-content: center;
	border-radius: 8px;
	background: hsl(var(--muted));
	color: hsl(var(--muted-foreground));
}

.sink-card-main {
	flex: 1;
	min-width: 0;
}

.sink-card-line {
	display: flex;
	align-items: center;
	gap: 8px;
	flex-wrap: wrap;
}

.sink-card-label {
	font-size: 14px;
	font-weight: 600;
}

.sink-card-kind {
	font-size: 10px;
	letter-spacing: 0.06em;
	font-weight: 600;
	padding: 2px 6px;
	border-radius: 4px;
	background: hsl(var(--muted));
	color: hsl(var(--muted-foreground));
}

.sink-card-badge {
	display: inline-flex;
	align-items: center;
	gap: 3px;
	font-size: 10px;
	font-weight: 600;
	letter-spacing: 0.04em;
	padding: 2px 7px;
	border-radius: 4px;
	background: hsl(var(--primary));
	color: hsl(var(--primary-foreground));
}

.sink-card-meta {
	margin-top: 3px;
	font-size: 12px;
	color: hsl(var(--muted-foreground));
	overflow: hidden;
	text-overflow: ellipsis;
	white-space: nowrap;
	font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
}

.sink-card-actions {
	display: flex;
	align-items: center;
	gap: 2px;
	flex-shrink: 0;
}

.action-danger {
	color: hsl(var(--destructive));
}

.action-danger:hover {
	background: hsl(var(--destructive) / 0.1);
	color: hsl(var(--destructive));
}

.empty-state {
	display: flex;
	flex-direction: column;
	align-items: center;
	justify-content: center;
	gap: 6px;
	padding: 36px 16px;
	border: 1px dashed hsl(var(--border));
	border-radius: 10px;
	color: hsl(var(--muted-foreground));
}

.empty-state--small {
	padding: 20px 16px;
	gap: 4px;
}

.empty-state-icon {
	color: hsl(var(--muted-foreground) / 0.7);
	margin-bottom: 4px;
}

.empty-state-title {
	font-size: 13px;
	font-weight: 500;
	color: hsl(var(--foreground));
	margin: 0;
}

.empty-state-hint {
	font-size: 12px;
	margin: 0;
}

.job-list {
	display: flex;
	flex-direction: column;
	gap: 6px;
}

.job-row {
	display: grid;
	grid-template-columns: 80px 1fr auto auto;
	align-items: center;
	gap: 12px;
	padding: 8px 12px;
	border: 1px solid hsl(var(--border));
	border-radius: 8px;
	background: hsl(var(--card));
}

.job-pill {
	font-size: 10px;
	font-weight: 600;
	letter-spacing: 0.04em;
	text-align: center;
	padding: 3px 6px;
	border-radius: 4px;
	background: hsl(var(--muted));
	color: hsl(var(--muted-foreground));
}

.job-pill[data-status="active"] {
	background: hsl(217 91% 60% / 0.15);
	color: hsl(217 91% 60%);
}

.job-pill[data-status="complete"] {
	background: hsl(142 71% 45% / 0.15);
	color: hsl(142 71% 45%);
}

.job-pill[data-status="failed"] {
	background: hsl(var(--destructive) / 0.15);
	color: hsl(var(--destructive));
}

.job-pill[data-status="cancelled"] {
	background: hsl(var(--muted));
	color: hsl(var(--muted-foreground));
}

.job-meta {
	min-width: 0;
}

.job-name {
	font-size: 13px;
	overflow: hidden;
	text-overflow: ellipsis;
	white-space: nowrap;
	margin-bottom: 4px;
}

.job-progress {
	height: 4px;
}

.job-stats {
	display: flex;
	flex-direction: column;
	align-items: flex-end;
	font-variant-numeric: tabular-nums;
	font-size: 11px;
	color: hsl(var(--muted-foreground));
	min-width: 110px;
}

.job-percent {
	font-size: 12px;
	font-weight: 600;
	color: hsl(var(--foreground));
}

.job-actions {
	display: flex;
}

.cloud-sink-dialog {
	max-width: 720px;
	width: calc(100vw - 48px);
}

.dialog-subtitle {
	font-size: 12px;
	color: hsl(var(--muted-foreground));
	margin: 4px 0 0;
}

.dialog-form {
	display: flex;
	flex-direction: column;
	gap: 12px;
	padding: 4px 0;
	max-height: 65vh;
	overflow-y: auto;
	padding-right: 4px;
	scrollbar-width: none;
	-ms-overflow-style: none;
}

.dialog-form::-webkit-scrollbar {
	display: none;
}

.dialog-field {
	display: flex;
	flex-direction: column;
	gap: 6px;
	min-width: 0;
}

.grid-2 {
	display: grid;
	grid-template-columns: 1fr 1fr;
	gap: 12px;
}

.grid-host-port {
	display: grid;
	grid-template-columns: 1fr 100px;
	gap: 12px;
}

.dialog-hint {
	font-size: 11px;
	color: hsl(var(--muted-foreground));
	margin: 0;
}

.dialog-sep {
	border: 0;
	border-top: 1px solid hsl(var(--border));
	margin: 4px 0;
}

.dialog-textarea {
	font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
	font-size: 12px;
	padding: 8px 10px;
	border: 1px solid hsl(var(--border));
	border-radius: 6px;
	background: hsl(var(--background));
	color: hsl(var(--foreground));
	resize: vertical;
	min-height: 60px;
	outline: none;
	transition: border-color 0.15s ease;
}

.dialog-textarea:focus {
	border-color: hsl(var(--ring));
	box-shadow: 0 0 0 2px hsl(var(--ring) / 0.2);
}

.dialog-field-toggle {
	display: flex;
	align-items: center;
	gap: 12px;
	padding: 10px 12px;
	border: 1px solid hsl(var(--border));
	border-radius: 8px;
	background: hsl(var(--muted) / 0.3);
}

.dialog-field-toggle .ui-checkbox {
	width: 100%;
	align-items: flex-start;
}

.dialog-field-toggle :deep(.ui-checkbox__label) {
	display: flex;
	flex-direction: column;
	min-width: 0;
}

.toggle-title {
	font-size: 13px;
	font-weight: 500;
	line-height: 1.2;
}

.toggle-hint {
	font-size: 11px;
	color: hsl(var(--muted-foreground));
	margin-top: 2px;
}

.dialog-error {
	display: flex;
	align-items: center;
	gap: 6px;
	font-size: 12px;
	color: hsl(var(--destructive));
	background: hsl(var(--destructive) / 0.1);
	border: 1px solid hsl(var(--destructive) / 0.3);
	border-radius: 6px;
	padding: 8px 10px;
}

.field-row {
	display: flex;
	align-items: baseline;
	justify-content: space-between;
	gap: 8px;
	margin-bottom: 4px;
}

.field-row__count {
	font-size: 11px;
	font-weight: 500;
	color: hsl(var(--muted-foreground));
	font-variant-numeric: tabular-nums;
}

.multi-select {
	display: flex;
	align-items: center;
	gap: 6px;
	width: 100%;
	min-height: 34px;
	padding: 4px 8px 4px 10px;
	background: transparent;
	border: 1px solid hsl(var(--input));
	border-radius: 8px;
	color: hsl(var(--foreground));
	font-size: 13px;
	cursor: pointer;
	transition:
		border-color 0.15s ease,
		box-shadow 0.15s ease;
}

.multi-select:hover {
	border-color: hsl(var(--ring) / 0.5);
}

.multi-select[data-state="open"],
.multi-select:focus-visible {
	outline: none;
	border-color: hsl(var(--ring));
	box-shadow: 0 0 0 3px hsl(var(--ring) / 0.18);
}

.multi-select__body {
	flex: 1;
	display: flex;
	flex-wrap: wrap;
	gap: 4px;
	min-width: 0;
	text-align: left;
}

.multi-select__placeholder {
	color: hsl(var(--muted-foreground));
	padding: 2px 0;
}

.multi-select__tag {
	display: inline-flex;
	align-items: center;
	gap: 3px;
	padding: 2px 4px 2px 7px;
	font-size: 11px;
	font-weight: 500;
	border-radius: 4px;
	background: hsl(var(--primary) / 0.12);
	color: hsl(var(--primary));
	line-height: 1.4;
}

.multi-select__tag-x {
	cursor: pointer;
	border-radius: 3px;
	padding: 1px;
	opacity: 0.7;
	transition: opacity 0.12s ease, background-color 0.12s ease;
}

.multi-select__tag-x:hover {
	opacity: 1;
	background: hsl(var(--primary) / 0.2);
}

.multi-select__chevron {
	color: hsl(var(--muted-foreground));
	flex-shrink: 0;
}

:global(.multi-select__menu) {
	min-width: var(--reka-dropdown-menu-trigger-width, 200px);
}

.size-range {
	display: flex;
	align-items: center;
	gap: 6px;
}

.size-range .input-suffix {
	flex: 1;
	min-width: 0;
}

.size-range__sep {
	color: hsl(var(--muted-foreground));
	font-size: 13px;
	flex-shrink: 0;
}

.input-suffix {
	position: relative;
	display: flex;
	align-items: center;
}

.input-suffix :deep(input) {
	padding-right: 38px;
}

.input-suffix__unit {
	position: absolute;
	right: 10px;
	font-size: 11px;
	font-weight: 500;
	color: hsl(var(--muted-foreground));
	pointer-events: none;
}

.ext-tags {
	display: flex;
	flex-wrap: wrap;
	gap: 4px;
	margin-top: 6px;
}

.ext-tag {
	display: inline-flex;
	padding: 2px 7px;
	font-size: 11px;
	font-weight: 500;
	font-family: var(--font-mono, ui-monospace, monospace);
	border-radius: 4px;
	background: hsl(var(--muted));
	color: hsl(var(--foreground));
}

.dialog-hint--warn {
	color: hsl(var(--destructive));
}

.sink-card--disabled {
	opacity: 0.55;
}

.cloud-sink-confirm {
	max-width: 420px;
}

.confirm-target {
	display: flex;
	flex-direction: column;
	gap: 2px;
	padding: 10px 12px;
	background: hsl(var(--muted) / 0.5);
	border-radius: 6px;
	font-size: 13px;
}

.confirm-target-meta {
	font-size: 11px;
	color: hsl(var(--muted-foreground));
	font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
}

@media (max-width: 600px) {
	.grid-2,
	.grid-host-port {
		grid-template-columns: 1fr;
	}
}
</style>
