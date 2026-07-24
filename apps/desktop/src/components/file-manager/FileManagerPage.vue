<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from "vue";
import { useI18n } from "vue-i18n";
import { AlertTriangle, CheckCircle2, ChevronDown, ChevronRight, File, Folder, Loader2, Pencil, Plus, RefreshCcw, Server, Trash2, X, XCircle } from "@lucide/vue";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import PasswordInput from "@/components/ui/PasswordInput.vue";
import { useToast } from "@/composables/useToast";
import * as api from "@/lib/backend/api";
import type { FileConnection, FileConnectionInput, FileConnectionTestResult, FileEntryStat, FileManagerEntry } from "@/lib/backend/tauri";

const LIST_PAGE_SIZE = 200;

const { t } = useI18n();
const { toast } = useToast();
const text = computed(() => ({
  title: t("fileManager.title"),
  add: t("fileManager.add"),
  emptyConnections: t("fileManager.emptyConnections"),
  emptyDirectory: t("fileManager.emptyDirectory"),
  name: t("fileManager.name"),
  endpoint: t("fileManager.endpoint"),
  root: t("fileManager.root"),
  username: t("fileManager.username"),
  password: t("fileManager.password"),
  keepPassword: t("fileManager.keepPassword"),
  clearPassword: t("fileManager.clearPassword"),
  security: t("fileManager.security"),
  test: t("fileManager.test"),
  save: t("common.save"),
  cancel: t("common.cancel"),
  edit: t("fileManager.edit"),
  delete: t("fileManager.delete"),
  deleteConfirm: t("fileManager.deleteConfirm"),
  loadError: t("fileManager.loadError"),
  testSuccess: t("fileManager.testSuccess"),
  refresh: t("fileManager.refresh"),
  stage: {
    configuration: t("fileManager.stageConfiguration"),
    dns: "DNS",
    tcp: "TCP",
    authentication: t("fileManager.stageAuthentication"),
    root: t("fileManager.root"),
  },
  type: t("fileManager.type"),
  size: t("fileManager.size"),
  modified: t("fileManager.modified"),
  loadMore: t("fileManager.loadMore"),
  cursorExpired: t("fileManager.cursorExpired"),
  metadata: t("fileManager.metadata"),
  contentType: t("fileManager.contentType"),
  contentEncoding: t("fileManager.contentEncoding"),
  contentDisposition: t("fileManager.contentDisposition"),
  cacheControl: t("fileManager.cacheControl"),
  contentMd5: t("fileManager.contentMd5"),
  etag: "ETag",
  version: t("fileManager.version"),
  loadedCount: t("fileManager.loadedCount"),
}));

const connections = ref<FileConnection[]>([]);
const selectedId = ref<string | null>(null);
const entries = ref<FileManagerEntry[]>([]);
const currentPath = ref("");
const listCursor = ref<string | null>(null);
const selectedEntry = ref<FileManagerEntry | null>(null);
const entryStat = ref<FileEntryStat | null>(null);
const statError = ref<string | null>(null);
const rootError = ref<string | null>(null);
const loadingConnections = ref(false);
const loadingEntries = ref(false);
const loadingMore = ref(false);
const loadingStat = ref(false);
const editorOpen = ref(false);
const deleteOpen = ref(false);
const saving = ref(false);
const testing = ref(false);
const deleting = ref(false);
const testResult = ref<FileConnectionTestResult | null>(null);
const editingId = ref<string | null>(null);
const clearPassword = ref(false);
const form = ref({ name: "", endpoint: "ftp://localhost:21", root: "/", username: "", password: "" });
let connectionsGeneration = 0;
let rootGeneration = 0;
let statGeneration = 0;
let navigationGeneration = 0;

const selectedConnection = computed(() => connections.value.find((connection) => connection.id === selectedId.value));
const canSubmit = computed(() => !!form.value.name.trim() && !!form.value.endpoint.trim() && form.value.root.startsWith("/"));
const breadcrumbs = computed(() => {
  const result = [{ label: "/", path: "" }];
  const segments = currentPath.value.split("/").filter(Boolean);
  let path = "";
  for (const segment of segments) {
    path = path ? `${path}/${segment}` : segment;
    result.push({ label: segment, path });
  }
  return result;
});

function inputFromForm(): FileConnectionInput {
  return {
    id: editingId.value,
    expectedRevision: editingId.value ? selectedConnection.value?.revision : undefined,
    name: form.value.name.trim(),
    config: {
      type: "ftp",
      endpoint: form.value.endpoint.trim(),
      root: form.value.root.trim(),
      username: form.value.username.trim(),
    },
    secrets: clearPassword.value ? { password: null, clearPassword: true } : form.value.password ? { password: form.value.password } : undefined,
  };
}

async function loadConnections(preferredId?: string) {
  const generation = ++connectionsGeneration;
  const navigation = ++navigationGeneration;
  rootGeneration += 1;
  const closePromise = closeActiveCursor();
  loadingConnections.value = true;
  try {
    const [loaded] = await Promise.all([api.listFileConnections(), closePromise]);
    if (generation !== connectionsGeneration || navigation !== navigationGeneration) return;
    connections.value = loaded;
    const nextId = preferredId && connections.value.some((connection) => connection.id === preferredId) ? preferredId : selectedId.value;
    selectedId.value = nextId && connections.value.some((connection) => connection.id === nextId) ? nextId : (connections.value[0]?.id ?? null);
    currentPath.value = "";
    await loadDirectory();
  } catch (error) {
    if (generation === connectionsGeneration) toast(`${text.value.loadError}: ${String(error)}`, 5000);
  } finally {
    if (generation === connectionsGeneration) loadingConnections.value = false;
  }
}

async function closeActiveCursor() {
  const cursor = listCursor.value;
  const connectionId = selectedId.value;
  listCursor.value = null;
  if (!cursor || !connectionId) return;
  await closeCursor(connectionId, cursor);
}

async function closeCursor(connectionId: string, cursor: string) {
  try {
    await api.closeFileListCursor(connectionId, cursor);
  } catch {
    // Closing is best effort; a missing/expired cursor is already unusable.
  }
}

function clearSelection() {
  statGeneration += 1;
  selectedEntry.value = null;
  entryStat.value = null;
  statError.value = null;
  loadingStat.value = false;
}

async function loadDirectory() {
  const generation = ++rootGeneration;
  entries.value = [];
  clearSelection();
  rootError.value = null;
  if (!selectedId.value) {
    loadingEntries.value = false;
    return;
  }
  const connectionId = selectedId.value;
  const path = currentPath.value;
  loadingEntries.value = true;
  try {
    const page = await api.listFileEntries(connectionId, path, { pageSize: LIST_PAGE_SIZE });
    if (generation === rootGeneration && selectedId.value === connectionId && currentPath.value === path) {
      entries.value = page.entries;
      listCursor.value = page.cursor ?? null;
    } else if (page.cursor) {
      await closeCursor(connectionId, page.cursor);
    }
  } catch (error) {
    if (generation === rootGeneration) {
      rootError.value = String(error);
      toast(rootError.value, 5000);
    }
  } finally {
    if (generation === rootGeneration) loadingEntries.value = false;
  }
}

async function refreshDirectory() {
  const navigation = ++navigationGeneration;
  rootGeneration += 1;
  const closePromise = closeActiveCursor();
  await closePromise;
  if (navigation !== navigationGeneration) return;
  await loadDirectory();
}

async function loadMore() {
  const cursor = listCursor.value;
  const connectionId = selectedId.value;
  const path = currentPath.value;
  if (!cursor || !connectionId || loadingMore.value) return;
  loadingMore.value = true;
  try {
    const page = await api.listFileEntriesNext(connectionId, cursor, path, { pageSize: LIST_PAGE_SIZE });
    if (listCursor.value === cursor && selectedId.value === connectionId && currentPath.value === path) {
      entries.value.push(...page.entries);
      listCursor.value = page.cursor ?? null;
    } else if (page.cursor) {
      await closeCursor(connectionId, page.cursor);
    }
  } catch (error) {
    if (listCursor.value === cursor) {
      listCursor.value = null;
      const message = String(error);
      rootError.value = message.includes("CursorExpired") ? text.value.cursorExpired : message;
      toast(rootError.value, 5000);
    }
  } finally {
    loadingMore.value = false;
  }
}

async function selectConnection(id: string) {
  if (selectedId.value === id) return;
  const navigation = ++navigationGeneration;
  rootGeneration += 1;
  const closePromise = closeActiveCursor();
  selectedId.value = id;
  currentPath.value = "";
  await closePromise;
  if (navigation !== navigationGeneration) return;
  await loadDirectory();
}

async function openDirectory(path: string) {
  if (path === currentPath.value) return;
  const navigation = ++navigationGeneration;
  rootGeneration += 1;
  const closePromise = closeActiveCursor();
  currentPath.value = path;
  await closePromise;
  if (navigation !== navigationGeneration) return;
  await loadDirectory();
}

async function selectEntry(entry: FileManagerEntry) {
  selectedEntry.value = entry;
  entryStat.value = null;
  statError.value = null;
  const generation = ++statGeneration;
  const connectionId = selectedId.value;
  if (!connectionId) return;
  loadingStat.value = true;
  try {
    const metadata = await api.statFileEntry(connectionId, entry.path);
    if (generation === statGeneration && selectedId.value === connectionId && selectedEntry.value?.path === entry.path) {
      entryStat.value = metadata;
    }
  } catch (error) {
    if (generation === statGeneration) statError.value = String(error);
  } finally {
    if (generation === statGeneration) loadingStat.value = false;
  }
}

function openCreate() {
  editingId.value = null;
  form.value = { name: "", endpoint: "ftp://localhost:21", root: "/", username: "", password: "" };
  testResult.value = null;
  clearPassword.value = false;
  editorOpen.value = true;
}

function openEdit() {
  const connection = selectedConnection.value;
  if (!connection) return;
  editingId.value = connection.id;
  form.value = {
    name: connection.name,
    endpoint: connection.config.endpoint,
    root: connection.config.root,
    username: connection.config.username,
    password: "",
  };
  testResult.value = null;
  clearPassword.value = false;
  editorOpen.value = true;
}

async function testConnection() {
  testing.value = true;
  testResult.value = null;
  try {
    testResult.value = await api.testFileConnection(inputFromForm());
    if (testResult.value.success) toast(text.value.testSuccess, 2000);
  } catch (error) {
    toast(String(error), 5000);
  } finally {
    testing.value = false;
  }
}

async function saveConnection() {
  if (!canSubmit.value) return;
  saving.value = true;
  try {
    const saved = await api.saveFileConnection(inputFromForm());
    editorOpen.value = false;
    await loadConnections(saved.id);
  } catch (error) {
    toast(String(error), 5000);
  } finally {
    saving.value = false;
  }
}

async function deleteConnection() {
  if (!selectedId.value || deleting.value) return;
  const deletedId = selectedId.value;
  deleting.value = true;
  try {
    await api.deleteFileConnection(deletedId);
    deleteOpen.value = false;
    selectedId.value = null;
    await loadConnections();
  } catch (error) {
    toast(String(error), 5000);
  } finally {
    deleting.value = false;
  }
}

function formatSize(bytes: number): string {
  if (!bytes) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const exponent = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
  return `${(bytes / 1024 ** exponent).toFixed(exponent ? 1 : 0)} ${units[exponent]}`;
}

onMounted(() => void loadConnections());
onBeforeUnmount(() => {
  navigationGeneration += 1;
  rootGeneration += 1;
  statGeneration += 1;
  void closeActiveCursor();
});
</script>

<template>
  <div class="flex h-full min-h-0 bg-background">
    <aside class="flex w-64 shrink-0 flex-col border-r bg-muted/10">
      <div class="flex h-10 items-center justify-between border-b px-3">
        <div class="flex min-w-0 items-center gap-2 text-sm font-medium">
          <Server class="h-4 w-4 shrink-0" />
          <span class="truncate">{{ text.title }}</span>
        </div>
        <Button size="icon" variant="ghost" class="h-7 w-7" :title="text.add" :aria-label="text.add" @click="openCreate">
          <Plus class="h-4 w-4" />
        </Button>
      </div>
      <div class="min-h-0 flex-1 overflow-auto p-1.5">
        <div v-if="loadingConnections" class="flex justify-center py-6"><Loader2 class="h-4 w-4 animate-spin text-muted-foreground" /></div>
        <button
          v-for="connection in connections"
          :key="connection.id"
          type="button"
          class="mb-0.5 flex w-full min-w-0 items-center gap-2 rounded px-2 py-2 text-left text-sm hover:bg-muted"
          :class="selectedId === connection.id ? 'bg-accent text-accent-foreground' : ''"
          @click="void selectConnection(connection.id)"
        >
          <Server class="h-4 w-4 shrink-0 text-muted-foreground" />
          <span class="min-w-0 flex-1 truncate">{{ connection.name }}</span>
          <span class="text-[10px] uppercase text-muted-foreground">FTP</span>
        </button>
        <div v-if="!loadingConnections && !connections.length" class="px-3 py-8 text-center text-xs text-muted-foreground">{{ text.emptyConnections }}</div>
      </div>
    </aside>

    <section class="flex min-w-0 flex-1 flex-col">
      <div class="flex h-10 items-center gap-1 border-b px-2">
        <span class="min-w-0 flex-1 truncate px-1 text-sm font-medium">{{ selectedConnection?.name ?? text.title }}</span>
        <Button variant="ghost" size="icon" class="h-7 w-7" :disabled="!selectedConnection || loadingEntries" :title="text.refresh" :aria-label="text.refresh" @click="void refreshDirectory()">
          <RefreshCcw class="h-3.5 w-3.5" :class="{ 'animate-spin': loadingEntries }" />
        </Button>
        <Button variant="ghost" size="icon" class="h-7 w-7" :disabled="!selectedConnection" :title="text.edit" :aria-label="text.edit" @click="openEdit">
          <Pencil class="h-3.5 w-3.5" />
        </Button>
        <Button variant="ghost" size="icon" class="h-7 w-7 text-destructive hover:text-destructive" :disabled="!selectedConnection || deleting" :title="text.delete" :aria-label="text.delete" @click="deleteOpen = true">
          <Trash2 class="h-3.5 w-3.5" />
        </Button>
      </div>

      <div v-if="selectedConnection" data-file-manager-breadcrumb class="flex h-9 shrink-0 items-center gap-0.5 overflow-x-auto border-b px-3 text-xs">
        <template v-for="(crumb, index) in breadcrumbs" :key="crumb.path">
          <ChevronRight v-if="index" class="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
          <button type="button" class="max-w-48 truncate px-1 py-1 hover:text-foreground" :class="crumb.path === currentPath ? 'font-medium text-foreground' : 'text-muted-foreground'" :title="crumb.path || '/'" @click="void openDirectory(crumb.path)">
            {{ crumb.label }}
          </button>
        </template>
        <span class="ml-auto shrink-0 pl-3 text-muted-foreground">{{ text.loadedCount.replace("{count}", String(entries.length)) }}</span>
      </div>

      <div class="flex min-h-0 flex-1">
        <div class="min-w-0 flex-1 overflow-auto">
          <table v-if="selectedConnection" class="w-full table-fixed text-sm">
            <thead class="sticky top-0 z-10 border-b bg-background text-left text-xs text-muted-foreground">
              <tr>
                <th class="w-[48%] px-3 py-2 font-medium">{{ text.name }}</th>
                <th class="w-24 px-3 py-2 font-medium">{{ text.type }}</th>
                <th class="w-28 px-3 py-2 text-right font-medium">{{ text.size }}</th>
                <th class="w-48 px-3 py-2 font-medium">{{ text.modified }}</th>
              </tr>
            </thead>
            <tbody>
              <tr
                v-for="entry in entries"
                :key="entry.path"
                tabindex="0"
                class="border-b border-border/50 outline-none hover:bg-muted/40 focus-visible:bg-muted/60"
                :class="selectedEntry?.path === entry.path ? 'bg-accent/70' : ''"
                @click="void selectEntry(entry)"
                @dblclick="entry.kind === 'directory' && void openDirectory(entry.path)"
                @keydown.enter="entry.kind === 'directory' ? void openDirectory(entry.path) : void selectEntry(entry)"
              >
                <td class="px-3 py-2">
                  <div class="flex min-w-0 items-center gap-2">
                    <Folder v-if="entry.kind === 'directory'" class="h-4 w-4 shrink-0 text-amber-500" />
                    <File v-else class="h-4 w-4 shrink-0 text-muted-foreground" />
                    <span class="truncate" :title="entry.path">{{ entry.name }}</span>
                  </div>
                </td>
                <td class="px-3 py-2 text-xs text-muted-foreground">{{ entry.kind }}</td>
                <td class="px-3 py-2 text-right font-mono text-xs text-muted-foreground">{{ entry.kind === "file" ? formatSize(entry.size) : "" }}</td>
                <td class="truncate px-3 py-2 text-xs text-muted-foreground">{{ entry.lastModified ? new Date(entry.lastModified).toLocaleString() : "" }}</td>
              </tr>
            </tbody>
          </table>
          <div v-if="loadingEntries" class="flex justify-center py-12"><Loader2 class="h-5 w-5 animate-spin text-muted-foreground" /></div>
          <div v-else-if="rootError" role="alert" class="mx-auto max-w-xl px-6 py-6 text-center text-sm text-destructive">{{ rootError }}</div>
          <div v-else-if="selectedConnection && !entries.length" class="py-12 text-center text-sm text-muted-foreground">{{ text.emptyDirectory }}</div>
          <div v-if="listCursor && !rootError" class="flex justify-center border-t px-3 py-3">
            <Button data-file-manager-load-more variant="outline" size="sm" :disabled="loadingMore" @click="void loadMore()">
              <Loader2 v-if="loadingMore" class="mr-1.5 h-3.5 w-3.5 animate-spin" />
              <ChevronDown v-else class="mr-1.5 h-3.5 w-3.5" />
              {{ text.loadMore }}
            </Button>
          </div>
        </div>

        <aside v-if="selectedEntry" data-file-manager-metadata class="w-72 shrink-0 overflow-auto border-l bg-muted/5">
          <div class="flex h-9 items-center border-b px-3">
            <span class="min-w-0 flex-1 truncate text-xs font-medium">{{ text.metadata }}</span>
            <Button variant="ghost" size="icon" class="h-6 w-6" :aria-label="text.cancel" @click="clearSelection">
              <X class="h-3.5 w-3.5" />
            </Button>
          </div>
          <div v-if="loadingStat" class="flex justify-center py-8"><Loader2 class="h-4 w-4 animate-spin text-muted-foreground" /></div>
          <div v-else-if="statError" role="alert" class="break-words px-3 py-4 text-xs text-destructive">{{ statError }}</div>
          <dl v-else-if="entryStat" class="grid grid-cols-[6.5rem_minmax(0,1fr)] gap-x-2 gap-y-2 px-3 py-3 text-xs">
            <dt class="text-muted-foreground">{{ text.name }}</dt>
            <dd class="break-all">{{ entryStat.name }}</dd>
            <dt class="text-muted-foreground">{{ text.type }}</dt>
            <dd>{{ entryStat.kind }}</dd>
            <dt class="text-muted-foreground">{{ text.size }}</dt>
            <dd>{{ entryStat.kind === "file" ? formatSize(entryStat.size) : "" }}</dd>
            <dt class="text-muted-foreground">{{ text.modified }}</dt>
            <dd>{{ entryStat.lastModified ? new Date(entryStat.lastModified).toLocaleString() : "" }}</dd>
            <template v-if="entryStat.etag"
              ><dt class="text-muted-foreground">{{ text.etag }}</dt>
              <dd class="break-all font-mono">{{ entryStat.etag }}</dd></template
            >
            <template v-if="entryStat.version"
              ><dt class="text-muted-foreground">{{ text.version }}</dt>
              <dd class="break-all font-mono">{{ entryStat.version }}</dd></template
            >
            <template v-if="entryStat.contentType"
              ><dt class="text-muted-foreground">{{ text.contentType }}</dt>
              <dd class="break-all">{{ entryStat.contentType }}</dd></template
            >
            <template v-if="entryStat.contentEncoding"
              ><dt class="text-muted-foreground">{{ text.contentEncoding }}</dt>
              <dd class="break-all">{{ entryStat.contentEncoding }}</dd></template
            >
            <template v-if="entryStat.contentDisposition"
              ><dt class="text-muted-foreground">{{ text.contentDisposition }}</dt>
              <dd class="break-all">{{ entryStat.contentDisposition }}</dd></template
            >
            <template v-if="entryStat.cacheControl"
              ><dt class="text-muted-foreground">{{ text.cacheControl }}</dt>
              <dd class="break-all">{{ entryStat.cacheControl }}</dd></template
            >
            <template v-if="entryStat.contentMd5"
              ><dt class="text-muted-foreground">{{ text.contentMd5 }}</dt>
              <dd class="break-all font-mono">{{ entryStat.contentMd5 }}</dd></template
            >
            <template v-for="(value, key) in entryStat.userMetadata" :key="key"
              ><dt class="break-all text-muted-foreground">{{ key }}</dt>
              <dd class="break-all">{{ value }}</dd></template
            >
          </dl>
        </aside>
      </div>
    </section>
  </div>

  <Dialog v-model:open="editorOpen">
    <DialogContent class="sm:max-w-lg">
      <DialogHeader
        ><DialogTitle>{{ editingId ? text.edit : text.add }}</DialogTitle></DialogHeader
      >
      <div class="rounded border border-amber-500/40 bg-amber-500/10 px-3 py-2 text-xs text-amber-800 dark:text-amber-200">
        <div class="flex gap-2">
          <AlertTriangle class="mt-0.5 h-4 w-4 shrink-0" />
          <span>{{ text.security }}</span>
        </div>
      </div>
      <div class="grid gap-3 py-1">
        <div class="grid gap-1.5">
          <Label for="file-connection-name">{{ text.name }}</Label
          ><Input id="file-connection-name" v-model="form.name" />
        </div>
        <div class="grid gap-1.5">
          <Label for="file-connection-endpoint">{{ text.endpoint }}</Label
          ><Input id="file-connection-endpoint" v-model="form.endpoint" placeholder="ftp://host:21" />
        </div>
        <div class="grid grid-cols-2 gap-3">
          <div class="grid gap-1.5">
            <Label for="file-connection-root">{{ text.root }}</Label
            ><Input id="file-connection-root" v-model="form.root" placeholder="/" />
          </div>
          <div class="grid gap-1.5">
            <Label for="file-connection-username">{{ text.username }}</Label
            ><Input id="file-connection-username" v-model="form.username" />
          </div>
        </div>
        <div class="grid gap-1.5">
          <Label for="file-connection-password">{{ text.password }}</Label>
          <PasswordInput id="file-connection-password" v-model="form.password" :disabled="clearPassword" :placeholder="editingId ? text.keepPassword : ''" />
          <label v-if="editingId && selectedConnection?.hasPassword" class="flex items-center gap-2 text-xs text-muted-foreground">
            <input v-model="clearPassword" type="checkbox" class="h-3.5 w-3.5 accent-primary" @change="clearPassword && (form.password = '')" />
            <span>{{ text.clearPassword }}</span>
          </label>
        </div>
      </div>
      <div v-if="testResult" class="space-y-1 rounded border p-2">
        <div v-for="stage in testResult.stages" :key="stage.stage" class="flex min-w-0 items-start gap-2 text-xs">
          <CheckCircle2 v-if="stage.status === 'passed'" class="h-3.5 w-3.5 shrink-0 text-green-600" />
          <XCircle v-else-if="stage.status === 'failed'" class="h-3.5 w-3.5 shrink-0 text-destructive" />
          <span v-else class="h-3.5 w-3.5 shrink-0 rounded-full border" />
          <span class="w-24 shrink-0 font-medium">{{ text.stage[stage.stage] }}</span>
          <span class="min-w-0 break-words text-muted-foreground">{{ stage.message }}</span>
        </div>
      </div>
      <DialogFooter>
        <Button variant="outline" :disabled="saving || testing" @click="editorOpen = false">{{ text.cancel }}</Button>
        <Button variant="outline" :disabled="!canSubmit || saving || testing" @click="void testConnection()"> <Loader2 v-if="testing" class="mr-1.5 h-3.5 w-3.5 animate-spin" />{{ text.test }} </Button>
        <Button :disabled="!canSubmit || saving || testing" @click="void saveConnection()"> <Loader2 v-if="saving" class="mr-1.5 h-3.5 w-3.5 animate-spin" />{{ text.save }} </Button>
      </DialogFooter>
    </DialogContent>
  </Dialog>

  <Dialog v-model:open="deleteOpen">
    <DialogContent class="sm:max-w-sm">
      <DialogHeader
        ><DialogTitle>{{ text.delete }}</DialogTitle></DialogHeader
      >
      <p class="text-sm text-muted-foreground">{{ text.deleteConfirm }}</p>
      <DialogFooter>
        <Button variant="outline" :disabled="deleting" @click="deleteOpen = false">{{ text.cancel }}</Button>
        <Button variant="destructive" :disabled="deleting" @click="void deleteConnection()">
          <Loader2 v-if="deleting" class="mr-1.5 h-3.5 w-3.5 animate-spin" />
          {{ text.delete }}
        </Button>
      </DialogFooter>
    </DialogContent>
  </Dialog>
</template>
