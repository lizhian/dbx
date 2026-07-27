<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from "vue";
import { useI18n } from "vue-i18n";
import { ArrowLeft, ChevronDown, ChevronLeft, ChevronRight, Copy, Download, File as FileIcon, FilePenLine, FileQuestion, Folder, FolderOpen, FolderPlus, Loader2, RefreshCw, Trash2, Upload } from "@lucide/vue";
import { Button } from "@/components/ui/button";
import CustomContextMenu, { type ContextMenuItem } from "@/components/ui/CustomContextMenu.vue";
import { Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { useConnectionStore } from "@/stores/connectionStore";
import { useToast } from "@/composables/useToast";
import { formatError } from "@/lib/backend/errorUtils";
import * as api from "@/lib/backend/api";
import { copyToClipboard } from "@/lib/common/clipboard";
import { executeWithProductionContextGuard } from "@/lib/database/productionExecutionGuard";
import { treeItemPaddingLeft } from "@/lib/sidebar/sidebarTreeItemLayout";
import type { ConnectionConfig } from "@/types/database";
import type { FileConnection, FileCreateDirectoryRequest, FileEntry, FileRemoteOperationRequest, FileTransferRequest } from "@/types/fileManager";
import FileDownloadList from "./FileDownloadList.vue";
import type { FileDownloadTask } from "./fileDownload";
import { childFilePath, displayFilePath, formatFileSize, parentFilePath } from "./filePath";
import { flattenVisibleFileTree, normalizeFileListing } from "./fileTree";

const emit = defineEmits<{
  close: [];
}>();
const props = defineProps<{
  connectionId?: string;
}>();
const { t } = useI18n();
const { toast } = useToast();
const connectionStore = useConnectionStore();
const runtimeConnections = ref(new Map<string, FileConnection>());
let runtimeConnectionsRefreshGeneration = 0;
const fileConnections = computed(() => connectionStore.connections.filter((connection) => connection.db_type === "file"));
const activeConnection = ref<FileConnection>();
const downloadTasks = ref<FileDownloadTask[]>([]);
const activeConnectionDownloadTasks = computed(() => downloadTasks.value.filter((task) => task.connectionId === activeConnection.value?.id));
const currentPath = ref("");
const entries = ref<FileEntry[]>([]);
const expandedDirectoryPaths = ref(new Set<string>());
const directoryChildren = ref(new Map<string, FileEntry[]>());
const loadingDirectoryPaths = ref(new Set<string>());
const fileTreeRows = computed(() => flattenVisibleFileTree(entries.value, expandedDirectoryPaths.value, directoryChildren.value));
const browsing = ref(false);
const browseError = ref("");
let browseGeneration = 0;
const visiblePath = computed(() => displayFilePath(currentPath.value));
const uploadDialogOpen = ref(false);
const uploadLocalPath = ref("");
const uploadRemotePath = ref("");
const createDirectoryDialogOpen = ref(false);
const createDirectoryName = ref("");
const operationActive = ref("");
const deleteEntryTarget = ref<FileEntry>();
const remoteOperation = ref<{ operation: "copy" | "rename"; entry: FileEntry; destinationPath: string }>();
const replaceRequest = ref<{ operation: "upload"; request: FileTransferRequest } | { operation: "download"; request: FileTransferRequest; downloadTaskId: string } | { operation: "copy" | "rename"; request: FileRemoteOperationRequest; sourceKind: FileEntry["kind"] }>();
const fileContextMenuItems = ref<ContextMenuItem[]>([]);
const ENTRY_SINGLE_CLICK_DELAY_MS = 180;
let entryClickTimer: ReturnType<typeof setTimeout> | undefined;
const replaceDestination = computed(() => {
  const pending = replaceRequest.value;
  return pending ? ("remotePath" in pending.request ? pending.request.remotePath : pending.request.destinationPath) : "";
});
const remoteDestinationPath = computed({
  get: () => remoteOperation.value?.destinationPath ?? "",
  set: (value: string) => {
    if (remoteOperation.value) remoteOperation.value.destinationPath = value;
  },
});

function effectiveRuntimeConnection(config: ConnectionConfig): FileConnection | undefined {
  const runtime = runtimeConnections.value.get(config.id);
  if (!runtime) return undefined;
  return {
    ...runtime,
    name: config.name,
    capabilities: config.read_only
      ? {
          ...runtime.capabilities,
          write: false,
          delete: false,
          copy: false,
          rename: false,
        }
      : runtime.capabilities,
  };
}

function syncActiveConnection() {
  const activeId = activeConnection.value?.id;
  if (!activeId) return;
  const config = fileConnections.value.find((connection) => connection.id === activeId);
  const connection = config ? effectiveRuntimeConnection(config) : undefined;
  if (connection) activeConnection.value = connection;
  else closeBrowser();
}

async function refreshRuntimeConnections() {
  const generation = ++runtimeConnectionsRefreshGeneration;
  try {
    const runtime = await api.listFileConnections();
    if (generation !== runtimeConnectionsRefreshGeneration) return;
    runtimeConnections.value = new Map(runtime.map((connection) => [connection.id, connection]));
    syncActiveConnection();
  } catch (error) {
    if (generation !== runtimeConnectionsRefreshGeneration) return;
    toast(formatError(error), 4000);
  }
}

onMounted(async () => {
  try {
    await connectionStore.initFromDisk();
    await refreshRuntimeConnections();
    if (props.connectionId) await openConnectionById(props.connectionId);
  } catch (error) {
    toast(formatError(error), 4000);
    if (props.connectionId) emit("close");
  }
});

watch(
  () => fileConnections.value.map((connection) => `${connection.id}:${connection.name}:${connection.driver_profile}:${JSON.stringify(connection.external_config)}:${connection.read_only}`).join("|"),
  async () => {
    const activeId = activeConnection.value?.id;
    await refreshRuntimeConnections();
    if (activeId && activeConnection.value?.id === activeId) await refreshDirectory();
  },
);

async function openConnection(config: ConnectionConfig) {
  let connection = effectiveRuntimeConnection(config);
  if (!connection) {
    await refreshRuntimeConnections();
    connection = effectiveRuntimeConnection(config);
  }
  if (!connection) throw new Error(t("fileManager.connectionNotFound"));
  activeConnection.value = connection;
  await navigateToDirectory("");
}

async function openConnectionById(connectionId: string) {
  await connectionStore.initFromDisk();
  const connection = fileConnections.value.find((candidate) => candidate.id === connectionId);
  if (!connection) throw new Error(t("fileManager.connectionNotFound"));
  await openConnection(connection);
}

async function openEntry(entry: FileEntry) {
  if (entry.kind !== "directory") return;
  await navigateToDirectory(entry.path);
}

async function goUp() {
  if (!currentPath.value) return;
  await navigateToDirectory(parentFilePath(currentPath.value));
}

function resetFileTree() {
  browseGeneration += 1;
  expandedDirectoryPaths.value = new Set();
  directoryChildren.value = new Map();
  loadingDirectoryPaths.value = new Set();
}

async function navigateToDirectory(path: string) {
  currentPath.value = path;
  resetFileTree();
  await refreshDirectory();
}

async function refreshDirectory() {
  const connection = activeConnection.value;
  if (!connection?.capabilities.list) return;
  const generation = ++browseGeneration;
  const listedPath = currentPath.value;
  const expandedPaths = [...expandedDirectoryPaths.value];
  browsing.value = true;
  browseError.value = "";
  try {
    const rootEntries = normalizeFileListing(await api.listFilePath(connection.id, listedPath), listedPath);
    if (generation !== browseGeneration || activeConnection.value?.id !== connection.id || currentPath.value !== listedPath) return;
    entries.value = rootEntries;

    const nextChildren = new Map<string, FileEntry[]>();
    const nextExpanded = new Set<string>();
    for (const path of expandedPaths) {
      if (generation !== browseGeneration) return;
      try {
        nextChildren.set(path, normalizeFileListing(await api.listFilePath(connection.id, path), path));
        nextExpanded.add(path);
      } catch {
        // A concurrently removed or renamed branch simply collapses on refresh.
      }
    }
    if (generation !== browseGeneration) return;
    directoryChildren.value = nextChildren;
    expandedDirectoryPaths.value = nextExpanded;
  } catch (error) {
    if (generation !== browseGeneration) return;
    browseError.value = formatError(error);
  } finally {
    if (generation === browseGeneration) browsing.value = false;
  }
}

async function toggleDirectory(entry: FileEntry) {
  if (entry.kind !== "directory") return;
  const expanded = new Set(expandedDirectoryPaths.value);
  if (expanded.has(entry.path)) {
    expanded.delete(entry.path);
    expandedDirectoryPaths.value = expanded;
    return;
  }

  const connection = activeConnection.value;
  if (!connection) return;
  if (!directoryChildren.value.has(entry.path)) {
    const generation = browseGeneration;
    loadingDirectoryPaths.value = new Set([...loadingDirectoryPaths.value, entry.path]);
    try {
      const children = normalizeFileListing(await api.listFilePath(connection.id, entry.path), entry.path);
      if (generation !== browseGeneration || activeConnection.value?.id !== connection.id) return;
      directoryChildren.value = new Map(directoryChildren.value).set(entry.path, children);
    } catch (error) {
      toast(formatError(error), 4000);
      return;
    } finally {
      const loading = new Set(loadingDirectoryPaths.value);
      loading.delete(entry.path);
      loadingDirectoryPaths.value = loading;
    }
  }

  if (!directoryChildren.value.has(entry.path)) return;
  expanded.add(entry.path);
  expandedDirectoryPaths.value = expanded;
}

async function selectUploadFile() {
  if (!activeConnection.value?.capabilities.write) return;
  try {
    const { open } = await import("@tauri-apps/plugin-dialog");
    const selected = await open({ multiple: false, directory: false, title: t("fileManager.selectUpload") });
    if (!selected || Array.isArray(selected)) return;
    uploadLocalPath.value = selected;
    const name = selected.split(/[/\\]/).pop() ?? "";
    uploadRemotePath.value = childFilePath(currentPath.value, name);
    uploadDialogOpen.value = true;
  } catch (error) {
    toast(formatError(error), 4000);
  }
}

function showCreateDirectoryDialog() {
  if (!activeConnection.value?.capabilities.write) return;
  createDirectoryName.value = "";
  createDirectoryDialogOpen.value = true;
}

async function startCreateDirectory() {
  const connection = activeConnection.value;
  const name = createDirectoryName.value.trim();
  if (!connection || !name) return;
  const request: FileCreateDirectoryRequest = {
    connectionId: connection.id,
    path: childFilePath(currentPath.value, name),
  };
  if (!request.path) return;
  createDirectoryDialogOpen.value = false;
  operationActive.value = `create-directory:${request.path}`;
  try {
    const executed = await executeWithProductionContextGuard({
      connection: connectionStore.getConfig(request.connectionId),
      reviewText: `CREATE DIRECTORY ${request.path}`,
      source: t("fileManager.title"),
      execute: async () => {
        await api.createFileDirectory(request);
        return true;
      },
    });
    if (!executed) return;
    toast(t("fileManager.createDirectorySucceeded"));
    await refreshDirectory();
  } catch (error) {
    toast(formatError(error), 4000);
  } finally {
    operationActive.value = "";
  }
}

async function startUpload() {
  const connection = activeConnection.value;
  if (!connection || !uploadRemotePath.value.trim()) return;
  uploadDialogOpen.value = false;
  await runUpload({
    connectionId: connection.id,
    remotePath: uploadRemotePath.value.trim(),
    localPath: uploadLocalPath.value,
    replace: false,
  });
}

async function startDownload(entry: FileEntry) {
  const connection = activeConnection.value;
  if (!connection?.capabilities.read || entry.kind !== "file") return;
  try {
    const { save } = await import("@tauri-apps/plugin-dialog");
    const localPath = await save({ defaultPath: entry.name, title: t("fileManager.selectDownload") });
    if (!localPath) return;
    const task: FileDownloadTask = {
      id: crypto.randomUUID(),
      connectionId: connection.id,
      remotePath: entry.path,
      fileName: entry.name,
      localPath,
      bytesTransferred: 0,
      totalBytes: entry.size,
      status: "downloading",
    };
    downloadTasks.value.push(task);
    await runDownload(task.id, {
      connectionId: connection.id,
      remotePath: entry.path,
      localPath,
      replace: false,
    });
  } catch (error) {
    toast(formatError(error), 4000);
  }
}

function cancelPendingEntryClick() {
  if (!entryClickTimer) return;
  clearTimeout(entryClickTimer);
  entryClickTimer = undefined;
}

function handleEntryClick(entry: FileEntry, event: MouseEvent) {
  if (event.detail > 1 || entry.kind === "unknown") return;
  cancelPendingEntryClick();
  entryClickTimer = setTimeout(() => {
    entryClickTimer = undefined;
    if (entry.kind === "directory") void toggleDirectory(entry);
    else void startDownload(entry);
  }, ENTRY_SINGLE_CLICK_DELAY_MS);
}

function handleEntryDoubleClick(entry: FileEntry) {
  cancelPendingEntryClick();
  if (entry.kind === "directory") void openEntry(entry);
}

onBeforeUnmount(cancelPendingEntryClick);

async function runUpload(request: FileTransferRequest) {
  operationActive.value = `upload:${request.remotePath}`;
  try {
    const bytes = await executeWithProductionContextGuard({
      connection: connectionStore.getConfig(request.connectionId),
      reviewText: `UPLOAD ${request.localPath} -> ${request.remotePath}${request.replace ? " (replace)" : ""}`,
      source: t("fileManager.title"),
      execute: () => api.uploadFile(request),
    });
    if (bytes === undefined) return;
    toast(t("fileManager.uploadSucceeded", { size: formatFileSize(bytes) }));
    await refreshDirectory();
  } catch (error) {
    if (typeof error === "object" && error && "code" in error && error.code === "already_exists") {
      replaceRequest.value = { operation: "upload", request: { ...request, replace: true } };
    } else {
      toast(formatError(error), 4000);
    }
  } finally {
    operationActive.value = "";
  }
}

async function runDownload(taskId: string, request: FileTransferRequest) {
  const task = downloadTasks.value.find((candidate) => candidate.id === taskId);
  if (!task) return;
  task.status = "downloading";
  task.error = undefined;
  if (request.replace) task.bytesTransferred = 0;
  try {
    const bytes = await api.downloadFile(request, (progress) => {
      const activeTask = downloadTasks.value.find((candidate) => candidate.id === taskId);
      if (!activeTask || activeTask.status !== "downloading") return;
      activeTask.bytesTransferred = progress.bytesTransferred;
      activeTask.totalBytes = progress.totalBytes;
    });
    task.bytesTransferred = bytes;
    task.totalBytes ||= bytes;
    task.status = "completed";
    toast(t("fileManager.downloadSucceeded", { size: formatFileSize(bytes) }));
  } catch (error) {
    if (typeof error === "object" && error && "code" in error && error.code === "already_exists") {
      task.status = "waiting";
      replaceRequest.value = { operation: "download", request: { ...request, replace: true }, downloadTaskId: taskId };
      return;
    }
    task.status = "failed";
    task.error = formatError(error);
    toast(task.error, 4000);
  }
}

async function openDownloadedFile(task: FileDownloadTask) {
  if (task.status !== "completed") return;
  try {
    const { open } = await import("@tauri-apps/plugin-shell");
    await open(task.localPath);
  } catch (error) {
    toast(t("fileManager.openDownloadedFileFailed", { message: formatError(error) }), 5000);
  }
}

async function openDownloadFolder(task: FileDownloadTask) {
  if (task.status !== "completed") return;
  try {
    await api.revealPathInFileManager(task.localPath);
  } catch (error) {
    toast(t("fileManager.openDownloadFolderFailed", { message: formatError(error) }), 5000);
  }
}

function startRemoteOperation(entry: FileEntry, operation: "copy" | "rename") {
  if (entry.kind === "unknown" || (operation === "copy" && entry.kind !== "file")) return;
  remoteOperation.value = {
    operation,
    entry,
    destinationPath: operation === "copy" ? childFilePath(currentPath.value, `${entry.name}.copy`) : entry.path,
  };
}

async function confirmRemoteOperation() {
  const connection = activeConnection.value;
  const pending = remoteOperation.value;
  if (!connection || !pending || !pending.destinationPath.trim()) return;
  remoteOperation.value = undefined;
  await runRemoteOperation(
    pending.operation,
    {
      connectionId: connection.id,
      sourcePath: pending.entry.path,
      destinationPath: pending.destinationPath.trim(),
      replace: false,
    },
    pending.entry.kind,
  );
}

async function runRemoteOperation(operation: "copy" | "rename", request: FileRemoteOperationRequest, sourceKind: FileEntry["kind"] = "file") {
  operationActive.value = `${operation}:${request.sourcePath}`;
  try {
    const executed = await executeWithProductionContextGuard({
      connection: connectionStore.getConfig(request.connectionId),
      reviewText: `${operation.toUpperCase()} ${request.sourcePath} -> ${request.destinationPath}${request.replace ? " (replace)" : ""}`,
      source: t("fileManager.title"),
      execute: async () => {
        if (operation === "copy") await api.copyFilePath(request);
        else await api.renameFilePath(request);
        return true;
      },
    });
    if (!executed) return;
    toast(t(operation === "copy" ? "fileManager.copySucceeded" : "fileManager.renameSucceeded"));
    await refreshDirectory();
  } catch (error) {
    if (sourceKind !== "directory" && typeof error === "object" && error && "code" in error && error.code === "already_exists") {
      replaceRequest.value = { operation, request: { ...request, replace: true }, sourceKind };
    } else {
      toast(formatFileOperationError(error), 6000);
    }
  } finally {
    operationActive.value = "";
  }
}

function formatFileOperationError(error: unknown): string {
  const message = formatError(error);
  if (typeof error === "object" && error && "recovery" in error && typeof error.recovery === "string") {
    return `${message} ${error.recovery}`;
  }
  return message;
}

async function confirmReplace() {
  const pending = replaceRequest.value;
  replaceRequest.value = undefined;
  if (!pending) return;
  switch (pending.operation) {
    case "upload":
      await runUpload(pending.request);
      break;
    case "download":
      await runDownload(pending.downloadTaskId, pending.request);
      break;
    case "copy":
      await runRemoteOperation("copy", pending.request, pending.sourceKind);
      break;
    case "rename":
      await runRemoteOperation("rename", pending.request, pending.sourceKind);
      break;
  }
}

function dismissReplaceRequest() {
  const pending = replaceRequest.value;
  if (pending?.operation === "download") {
    const task = downloadTasks.value.find((candidate) => candidate.id === pending.downloadTaskId);
    if (task?.status === "waiting") task.status = "cancelled";
  }
  replaceRequest.value = undefined;
}

async function confirmDeleteEntry() {
  const connection = activeConnection.value;
  const entry = deleteEntryTarget.value;
  if (!connection || !entry) return;
  deleteEntryTarget.value = undefined;
  operationActive.value = `delete:${entry.path}`;
  try {
    const executed = await executeWithProductionContextGuard({
      connection: connectionStore.getConfig(connection.id),
      reviewText: `DELETE ${entry.path}`,
      source: t("fileManager.title"),
      execute: async () => {
        await api.deleteFilePath(connection.id, entry.path);
        return true;
      },
    });
    if (!executed) return;
    toast(t("fileManager.deleteSucceeded"));
    await refreshDirectory();
  } catch (error) {
    toast(formatError(error), 4000);
  } finally {
    operationActive.value = "";
  }
}

function closeBrowser() {
  activeConnection.value = undefined;
  currentPath.value = "";
  entries.value = [];
  resetFileTree();
  browseError.value = "";
  emit("close");
}

function copyEntryText(value: string) {
  void copyToClipboard(value)
    .then(() => toast(t("fileManager.copied"), 2000))
    .catch((error) => toast(formatError(error), 4000));
}

function buildFileContextMenu(entry: FileEntry): ContextMenuItem[] {
  const connection = activeConnection.value;
  if (!connection) return [];
  const items: ContextMenuItem[] = [];

  if (entry.kind === "directory") {
    items.push({ label: t("fileManager.open"), icon: FolderOpen, action: () => void openEntry(entry) });
    items.push({
      label: t(expandedDirectoryPaths.value.has(entry.path) ? "fileManager.collapseFolder" : "fileManager.expandFolder"),
      icon: expandedDirectoryPaths.value.has(entry.path) ? ChevronDown : ChevronRight,
      action: () => void toggleDirectory(entry),
    });
  } else if (entry.kind === "file" && connection.capabilities.read) {
    items.push({ label: t("fileManager.download"), icon: Download, disabled: !!operationActive.value, action: () => void startDownload(entry) });
  }

  if (entry.kind === "file" && connection.capabilities.copy) {
    items.push({ label: t("fileManager.copy"), icon: Copy, disabled: !!operationActive.value, action: () => startRemoteOperation(entry, "copy") });
  }
  if (entry.kind !== "unknown" && connection.capabilities.rename) {
    items.push({ label: t("fileManager.rename"), icon: FilePenLine, disabled: !!operationActive.value, action: () => startRemoteOperation(entry, "rename") });
  }
  items.push({ label: "", separator: true });
  items.push({ label: t("contextMenu.copyName"), icon: Copy, action: () => copyEntryText(entry.name) });
  items.push({ label: t("fileManager.copyPath"), icon: Copy, action: () => copyEntryText(entry.path) });
  if (connection.capabilities.delete) {
    items.push({ label: "", separator: true });
    items.push({ label: t("common.delete"), icon: Trash2, variant: "destructive", disabled: !!operationActive.value, action: () => (deleteEntryTarget.value = entry) });
  }
  return items;
}

function openFileContextMenu(event: MouseEvent, entry: FileEntry, openContextMenu: (event: MouseEvent, itemsOverride?: ContextMenuItem[]) => void) {
  const items = buildFileContextMenu(entry);
  fileContextMenuItems.value = items;
  openContextMenu(event, items);
}

defineExpose({ openConnectionById });
</script>

<template>
  <div class="flex h-full min-h-0 flex-1 flex-col">
    <section class="flex h-full min-h-0 flex-col bg-background">
      <header v-if="activeConnection" data-file-manager-toolbar class="flex h-11 shrink-0 items-center gap-1 border-b px-2">
        <div class="flex min-w-0 shrink-0 items-center gap-2">
          <Button variant="ghost" size="icon" class="h-7 w-7 shrink-0" :title="t('common.close')" @click="closeBrowser">
            <ArrowLeft class="h-4 w-4" />
          </Button>
          <h1 class="max-w-48 truncate text-sm font-semibold" :title="activeConnection.name">{{ activeConnection.name }}</h1>
        </div>
        <div class="mx-1 h-5 border-l" />
        <Button variant="ghost" size="icon" class="h-7 w-7 shrink-0" :disabled="!currentPath || browsing" :title="t('fileManager.up')" @click="goUp">
          <ChevronLeft class="h-4 w-4" />
        </Button>
        <Button variant="ghost" size="icon" class="h-7 w-7 shrink-0" :disabled="browsing" :title="t('fileManager.refresh')" @click="refreshDirectory">
          <RefreshCw class="h-4 w-4" :class="{ 'animate-spin': browsing }" />
        </Button>
        <span class="min-w-0 flex-1 truncate px-2 font-mono text-xs" :title="visiblePath">{{ visiblePath }}</span>
        <Button v-if="activeConnection.capabilities.write" variant="outline" size="sm" class="h-7 shrink-0" :disabled="!!operationActive" @click="showCreateDirectoryDialog">
          <Loader2 v-if="operationActive.startsWith('create-directory:')" class="h-4 w-4 animate-spin" />
          <FolderPlus v-else class="h-4 w-4" />
          {{ t("fileManager.newFolder") }}
        </Button>
        <Button v-if="activeConnection.capabilities.write" variant="outline" size="sm" class="h-7 shrink-0" :disabled="!!operationActive" @click="selectUploadFile">
          <Loader2 v-if="operationActive.startsWith('upload:')" class="h-4 w-4 animate-spin" />
          <Upload v-else class="h-4 w-4" />
          {{ t("fileManager.upload") }}
        </Button>
        <FileDownloadList :tasks="activeConnectionDownloadTasks" @open-file="openDownloadedFile" @open-folder="openDownloadFolder" />
        <span v-if="operationActive" role="status" class="sr-only">{{ t("fileManager.transferring") }}</span>
      </header>

      <template v-if="activeConnection">
        <div v-if="browseError" role="alert" class="border-b px-3 py-2 text-sm text-destructive">{{ browseError }}</div>
        <CustomContextMenu :items="fileContextMenuItems" v-slot="contextMenuSlot">
          <div class="min-h-0 flex-1 overflow-auto">
            <table class="w-full table-fixed text-sm">
              <thead class="sticky top-0 z-[1] bg-muted/70 text-left text-xs text-muted-foreground">
                <tr>
                  <th class="px-3 py-2 font-medium">{{ t("fileManager.fileName") }}</th>
                  <th class="w-28 px-3 py-2 text-right font-medium">{{ t("fileManager.size") }}</th>
                  <th class="w-48 px-3 py-2 font-medium">{{ t("fileManager.modified") }}</th>
                  <th class="w-36 px-3 py-2 text-right font-medium">{{ t("fileManager.actions") }}</th>
                </tr>
              </thead>
              <tbody>
                <tr v-if="currentPath" data-file-parent-row class="cursor-pointer border-b hover:bg-muted/50" @click="goUp">
                  <td class="px-3 py-2">
                    <span class="flex min-w-0 items-center gap-2">
                      <ChevronLeft class="h-4 w-4 shrink-0 text-muted-foreground" />
                      <FolderOpen class="h-4 w-4 shrink-0 text-amber-500" />
                      <span class="truncate">../</span>
                    </span>
                  </td>
                  <td class="px-3 py-2" />
                  <td class="px-3 py-2" />
                  <td class="px-3 py-2" />
                </tr>
                <tr
                  v-for="row in fileTreeRows"
                  :key="row.entry.path"
                  :data-file-entry-path="row.entry.path"
                  :data-file-entry-kind="row.entry.kind"
                  class="border-b hover:bg-muted/50"
                  :class="{ 'cursor-pointer': row.entry.kind !== 'unknown' }"
                  @click="(event) => handleEntryClick(row.entry, event)"
                  @dblclick="handleEntryDoubleClick(row.entry)"
                  @contextmenu="(event) => openFileContextMenu(event, row.entry, contextMenuSlot.onContextMenu)"
                >
                  <td class="py-2 pr-3" :style="{ paddingLeft: treeItemPaddingLeft(row.depth) }">
                    <span class="flex min-w-0 items-center gap-1.5">
                      <button
                        v-if="row.entry.kind === 'directory'"
                        type="button"
                        class="flex h-5 w-5 shrink-0 items-center justify-center text-muted-foreground hover:text-foreground"
                        :title="t(expandedDirectoryPaths.has(row.entry.path) ? 'fileManager.collapseFolder' : 'fileManager.expandFolder')"
                        :aria-expanded="expandedDirectoryPaths.has(row.entry.path)"
                        @click.stop="toggleDirectory(row.entry)"
                      >
                        <Loader2 v-if="loadingDirectoryPaths.has(row.entry.path)" class="h-3.5 w-3.5 animate-spin" />
                        <ChevronDown v-else-if="expandedDirectoryPaths.has(row.entry.path)" class="h-3.5 w-3.5" />
                        <ChevronRight v-else class="h-3.5 w-3.5" />
                      </button>
                      <span v-else class="h-5 w-5 shrink-0" />
                      <button v-if="row.entry.kind === 'directory'" type="button" class="flex min-w-0 items-center gap-2 text-left">
                        <FolderOpen v-if="expandedDirectoryPaths.has(row.entry.path)" class="h-4 w-4 shrink-0 text-amber-500" />
                        <Folder v-else class="h-4 w-4 shrink-0 text-amber-500" />
                        <span class="truncate">{{ row.entry.name }}</span>
                      </button>
                      <button v-else type="button" class="flex min-w-0 items-center gap-2 text-left">
                        <FileIcon v-if="row.entry.kind === 'file'" class="h-4 w-4 shrink-0 text-muted-foreground" />
                        <FileQuestion v-else class="h-4 w-4 shrink-0 text-muted-foreground" />
                        <span class="truncate">{{ row.entry.name }}</span>
                      </button>
                    </span>
                  </td>
                  <td class="px-3 py-2 text-right tabular-nums text-muted-foreground">{{ row.entry.kind === "file" ? formatFileSize(row.entry.size) : "—" }}</td>
                  <td class="truncate px-3 py-2 text-muted-foreground">{{ row.entry.modifiedAt ? new Date(row.entry.modifiedAt).toLocaleString() : "—" }}</td>
                  <td class="px-3 py-1 text-right">
                    <Button v-if="row.entry.kind === 'file' && activeConnection.capabilities.copy" variant="ghost" size="icon" class="h-7 w-7" :disabled="!!operationActive" :title="t('fileManager.copy')" @click.stop="startRemoteOperation(row.entry, 'copy')">
                      <Loader2 v-if="operationActive === `copy:${row.entry.path}`" class="h-4 w-4 animate-spin" />
                      <Copy v-else class="h-4 w-4" />
                    </Button>
                    <Button v-if="row.entry.kind !== 'unknown' && activeConnection.capabilities.rename" variant="ghost" size="icon" class="h-7 w-7" :disabled="!!operationActive" :title="t('fileManager.rename')" @click.stop="startRemoteOperation(row.entry, 'rename')">
                      <Loader2 v-if="operationActive === `rename:${row.entry.path}`" class="h-4 w-4 animate-spin" />
                      <FilePenLine v-else class="h-4 w-4" />
                    </Button>
                    <Button v-if="row.entry.kind === 'file' && activeConnection.capabilities.read" variant="ghost" size="icon" class="h-7 w-7" :disabled="!!operationActive" :title="t('fileManager.download')" @click.stop="startDownload(row.entry)">
                      <Loader2 v-if="operationActive === `download:${row.entry.path}`" class="h-4 w-4 animate-spin" />
                      <Download v-else class="h-4 w-4" />
                    </Button>
                    <Button v-if="activeConnection.capabilities.delete" variant="ghost" size="icon" class="h-7 w-7 text-destructive" :disabled="!!operationActive" :title="t('common.delete')" @click.stop="deleteEntryTarget = row.entry">
                      <Loader2 v-if="operationActive === `delete:${row.entry.path}`" class="h-4 w-4 animate-spin" />
                      <Trash2 v-else class="h-4 w-4" />
                    </Button>
                  </td>
                </tr>
              </tbody>
            </table>
            <div v-if="browsing && entries.length === 0" class="flex h-36 items-center justify-center text-muted-foreground">
              <Loader2 class="h-5 w-5 animate-spin" />
            </div>
            <p v-else-if="!browseError && entries.length === 0" class="p-6 text-center text-sm text-muted-foreground">{{ t("fileManager.emptyDirectory") }}</p>
          </div>
        </CustomContextMenu>
      </template>

      <div v-else data-file-manager-loading class="flex flex-1 items-center justify-center text-muted-foreground">
        <Loader2 class="h-5 w-5 animate-spin" />
      </div>
    </section>

    <Dialog v-model:open="createDirectoryDialogOpen">
      <DialogContent class="sm:max-w-[420px]">
        <DialogHeader>
          <DialogTitle>{{ t("fileManager.newFolder") }}</DialogTitle>
        </DialogHeader>
        <div class="grid gap-1.5">
          <Label for="file-create-directory-name">{{ t("fileManager.folderName") }}</Label>
          <Input id="file-create-directory-name" v-model="createDirectoryName" autofocus @keydown.enter.prevent="startCreateDirectory" />
        </div>
        <DialogFooter>
          <Button variant="outline" @click="createDirectoryDialogOpen = false">{{ t("common.cancel") }}</Button>
          <Button :disabled="!createDirectoryName.trim()" @click="startCreateDirectory">{{ t("fileManager.create") }}</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>

    <Dialog v-model:open="uploadDialogOpen">
      <DialogContent class="sm:max-w-[480px]">
        <DialogHeader>
          <DialogTitle>{{ t("fileManager.upload") }}</DialogTitle>
        </DialogHeader>
        <div class="grid gap-4">
          <div class="grid gap-1.5">
            <Label>{{ t("fileManager.localFile") }}</Label>
            <Input :model-value="uploadLocalPath" disabled />
          </div>
          <div class="grid gap-1.5">
            <Label for="file-upload-remote-path">{{ t("fileManager.remotePath") }}</Label>
            <Input id="file-upload-remote-path" v-model="uploadRemotePath" />
          </div>
        </div>
        <DialogFooter>
          <Button variant="outline" @click="uploadDialogOpen = false">{{ t("common.cancel") }}</Button>
          <Button :disabled="!uploadRemotePath.trim()" @click="startUpload">{{ t("fileManager.upload") }}</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>

    <Dialog :open="!!replaceRequest" @update:open="(open) => !open && dismissReplaceRequest()">
      <DialogContent class="sm:max-w-[420px]">
        <DialogHeader>
          <DialogTitle>{{ t("fileManager.replaceTitle") }}</DialogTitle>
        </DialogHeader>
        <p class="text-sm text-muted-foreground">{{ t("fileManager.replaceMessage") }}</p>
        <p class="truncate font-mono text-xs">{{ replaceDestination }}</p>
        <DialogFooter>
          <Button variant="outline" @click="dismissReplaceRequest">{{ t("common.cancel") }}</Button>
          <Button variant="destructive" @click="confirmReplace">{{ t("fileManager.replace") }}</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>

    <Dialog :open="!!remoteOperation" @update:open="(open) => !open && (remoteOperation = undefined)">
      <DialogContent class="sm:max-w-[500px]">
        <DialogHeader>
          <DialogTitle>{{ t(remoteOperation?.operation === "rename" ? "fileManager.rename" : "fileManager.copy") }}</DialogTitle>
        </DialogHeader>
        <div class="grid gap-4">
          <div class="grid gap-1.5">
            <Label>{{ t("fileManager.sourcePath") }}</Label>
            <Input :model-value="remoteOperation?.entry.path" disabled />
          </div>
          <div class="grid gap-1.5">
            <Label for="file-operation-destination-path">{{ t("fileManager.destinationPath") }}</Label>
            <Input id="file-operation-destination-path" v-model="remoteDestinationPath" />
          </div>
          <div class="border-l-2 border-amber-500 bg-amber-500/10 px-3 py-2 text-xs text-muted-foreground">
            <p v-if="remoteOperation?.operation === 'copy' && activeConnection?.capabilities.copyMode === 'stream_relay'">{{ t("fileManager.streamRelayNotice") }}</p>
            <p v-if="remoteOperation?.operation === 'rename' && remoteOperation.entry.kind === 'directory'">{{ t("fileManager.directoryRenameNotice") }}</p>
            <p v-else-if="remoteOperation?.operation === 'rename' && activeConnection?.capabilities.renameMode === 'copy_delete'">{{ t("fileManager.nonAtomicRenameNotice") }}</p>
            <p v-if="!activeConnection?.capabilities.atomicNoClobber">{{ t("fileManager.bestEffortNoClobberNotice") }}</p>
          </div>
        </div>
        <DialogFooter>
          <Button variant="outline" @click="remoteOperation = undefined">{{ t("common.cancel") }}</Button>
          <Button :disabled="!remoteDestinationPath.trim() || remoteDestinationPath.trim() === remoteOperation?.entry.path" @click="confirmRemoteOperation">
            {{ t(remoteOperation?.operation === "rename" ? "fileManager.rename" : "fileManager.copy") }}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>

    <Dialog :open="!!deleteEntryTarget" @update:open="(open) => !open && (deleteEntryTarget = undefined)">
      <DialogContent class="sm:max-w-[420px]">
        <DialogHeader>
          <DialogTitle>{{ t("fileManager.deleteEntryTitle") }}</DialogTitle>
        </DialogHeader>
        <p class="text-sm text-muted-foreground">{{ t("fileManager.deleteEntryMessage", { name: deleteEntryTarget?.name }) }}</p>
        <DialogFooter>
          <Button variant="outline" @click="deleteEntryTarget = undefined">{{ t("common.cancel") }}</Button>
          <Button variant="destructive" @click="confirmDeleteEntry">{{ t("common.delete") }}</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  </div>
</template>
