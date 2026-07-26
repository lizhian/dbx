<script setup lang="ts">
import { computed, onMounted, ref } from "vue";
import { useI18n } from "vue-i18n";
import { ArrowLeft, ChevronLeft, Copy, Download, FilePenLine, Folder, FolderOpen, Loader2, Pencil, Plus, RefreshCw, Trash2, Unplug, Upload } from "@lucide/vue";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { useFileConnectionStore } from "@/stores/fileConnectionStore";
import { useToast } from "@/composables/useToast";
import { formatError } from "@/lib/backend/errorUtils";
import * as api from "@/lib/backend/api";
import type { FileConnection, FileEntry, FileRemoteOperationRequest, FileTransferRequest } from "@/types/fileManager";
import FileConnectionDialog from "./FileConnectionDialog.vue";
import { childFilePath, displayFilePath, formatFileSize, parentFilePath } from "./filePath";

const { t } = useI18n();
const { toast } = useToast();
const store = useFileConnectionStore();
const dialogOpen = ref(false);
const editing = ref<FileConnection>();
const deleting = ref<FileConnection>();
const deleteActive = ref(false);
const loadError = ref("");
const activeConnection = ref<FileConnection>();
const currentPath = ref("");
const entries = ref<FileEntry[]>([]);
const browsing = ref(false);
const browseError = ref("");
const visiblePath = computed(() => displayFilePath(currentPath.value));
const uploadDialogOpen = ref(false);
const uploadLocalPath = ref("");
const uploadRemotePath = ref("");
const operationActive = ref("");
const deleteEntryTarget = ref<FileEntry>();
const remoteOperation = ref<{ operation: "copy" | "rename"; entry: FileEntry; destinationPath: string }>();
const replaceRequest = ref<{ operation: "upload" | "download"; request: FileTransferRequest } | { operation: "copy" | "rename"; request: FileRemoteOperationRequest }>();
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

onMounted(async () => {
  try {
    await store.load();
  } catch (error) {
    loadError.value = formatError(error);
  }
});

function createConnection() {
  editing.value = undefined;
  dialogOpen.value = true;
}

function editConnection(connection: FileConnection) {
  editing.value = connection;
  dialogOpen.value = true;
}

async function openConnection(connection: FileConnection) {
  activeConnection.value = connection;
  currentPath.value = "";
  await refreshDirectory();
}

async function openEntry(entry: FileEntry) {
  if (entry.kind !== "directory") return;
  currentPath.value = entry.path;
  await refreshDirectory();
}

async function goUp() {
  if (!currentPath.value) return;
  currentPath.value = parentFilePath(currentPath.value);
  await refreshDirectory();
}

async function refreshDirectory() {
  const connection = activeConnection.value;
  if (!connection?.capabilities.list) return;
  browsing.value = true;
  browseError.value = "";
  try {
    entries.value = await api.listFilePath(connection.id, currentPath.value);
  } catch (error) {
    browseError.value = formatError(error);
  } finally {
    browsing.value = false;
  }
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

async function startUpload() {
  const connection = activeConnection.value;
  if (!connection || !uploadRemotePath.value.trim()) return;
  uploadDialogOpen.value = false;
  await runTransfer("upload", {
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
    await runTransfer("download", {
      connectionId: connection.id,
      remotePath: entry.path,
      localPath,
      replace: false,
    });
  } catch (error) {
    toast(formatError(error), 4000);
  }
}

async function runTransfer(operation: "upload" | "download", request: FileTransferRequest) {
  operationActive.value = `${operation}:${request.remotePath}`;
  try {
    const bytes = operation === "upload" ? await api.uploadFile(request) : await api.downloadFile(request);
    toast(t(operation === "upload" ? "fileManager.uploadSucceeded" : "fileManager.downloadSucceeded", { size: formatFileSize(bytes) }));
    if (operation === "upload") await refreshDirectory();
  } catch (error) {
    if (typeof error === "object" && error && "code" in error && error.code === "already_exists") {
      replaceRequest.value = { operation, request: { ...request, replace: true } };
    } else {
      toast(formatError(error), 4000);
    }
  } finally {
    operationActive.value = "";
  }
}

function startRemoteOperation(entry: FileEntry, operation: "copy" | "rename") {
  if (entry.kind !== "file") return;
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
  await runRemoteOperation(pending.operation, {
    connectionId: connection.id,
    sourcePath: pending.entry.path,
    destinationPath: pending.destinationPath.trim(),
    replace: false,
  });
}

async function runRemoteOperation(operation: "copy" | "rename", request: FileRemoteOperationRequest) {
  operationActive.value = `${operation}:${request.sourcePath}`;
  try {
    if (operation === "copy") await api.copyFilePath(request);
    else await api.renameFilePath(request);
    toast(t(operation === "copy" ? "fileManager.copySucceeded" : "fileManager.renameSucceeded"));
    await refreshDirectory();
  } catch (error) {
    if (typeof error === "object" && error && "code" in error && error.code === "already_exists") {
      replaceRequest.value = { operation, request: { ...request, replace: true } };
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
      await runTransfer("upload", pending.request);
      break;
    case "download":
      await runTransfer("download", pending.request);
      break;
    case "copy":
      await runRemoteOperation("copy", pending.request);
      break;
    case "rename":
      await runRemoteOperation("rename", pending.request);
      break;
  }
}

async function confirmDeleteEntry() {
  const connection = activeConnection.value;
  const entry = deleteEntryTarget.value;
  if (!connection || !entry) return;
  deleteEntryTarget.value = undefined;
  operationActive.value = `delete:${entry.path}`;
  try {
    await api.deleteFilePath(connection.id, entry.path);
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
  browseError.value = "";
}

async function removeConnection() {
  if (!deleting.value) return;
  deleteActive.value = true;
  try {
    await store.remove(deleting.value.id);
    toast(t("fileManager.connectionDeleted"));
    deleting.value = undefined;
  } catch (error) {
    toast(formatError(error), 4000);
  } finally {
    deleteActive.value = false;
  }
}
</script>

<template>
  <div class="flex h-full min-h-0 flex-1 flex-col">
    <section class="flex h-full min-h-0 flex-col bg-background">
      <header class="flex h-11 shrink-0 items-center justify-between border-b px-3">
        <div class="flex min-w-0 items-center gap-2">
          <Button v-if="activeConnection" variant="ghost" size="icon" class="h-7 w-7 shrink-0" :title="t('fileManager.connections')" @click="closeBrowser">
            <ArrowLeft class="h-4 w-4" />
          </Button>
          <h1 class="truncate text-sm font-semibold">{{ activeConnection?.name ?? t("fileManager.title") }}</h1>
        </div>
        <Button v-if="!activeConnection" size="sm" class="h-7" @click="createConnection">
          <Plus class="h-4 w-4" />
          {{ t("fileManager.newConnection") }}
        </Button>
      </header>

      <template v-if="activeConnection">
        <div class="flex h-10 shrink-0 items-center gap-1 border-b px-2">
          <Button variant="ghost" size="icon" class="h-7 w-7" :disabled="!currentPath || browsing" :title="t('fileManager.up')" @click="goUp">
            <ChevronLeft class="h-4 w-4" />
          </Button>
          <Button variant="ghost" size="icon" class="h-7 w-7" :disabled="browsing" :title="t('fileManager.refresh')" @click="refreshDirectory">
            <RefreshCw class="h-4 w-4" :class="{ 'animate-spin': browsing }" />
          </Button>
          <span class="min-w-0 flex-1 truncate px-2 font-mono text-xs" :title="visiblePath">{{ visiblePath }}</span>
          <Button v-if="activeConnection.capabilities.write" variant="outline" size="sm" class="h-7" :disabled="!!operationActive" @click="selectUploadFile">
            <Loader2 v-if="operationActive.startsWith('upload:')" class="h-4 w-4 animate-spin" />
            <Upload v-else class="h-4 w-4" />
            {{ t("fileManager.upload") }}
          </Button>
          <span v-if="operationActive" role="status" class="sr-only">{{ t("fileManager.transferring") }}</span>
        </div>

        <div v-if="browseError" role="alert" class="border-b px-3 py-2 text-sm text-destructive">{{ browseError }}</div>
        <div class="min-h-0 flex-1 overflow-auto">
          <table class="w-full table-fixed text-sm">
            <thead class="sticky top-0 bg-muted/70 text-left text-xs text-muted-foreground">
              <tr>
                <th class="px-3 py-2 font-medium">{{ t("fileManager.fileName") }}</th>
                <th class="w-28 px-3 py-2 font-medium">{{ t("fileManager.type") }}</th>
                <th class="w-28 px-3 py-2 text-right font-medium">{{ t("fileManager.size") }}</th>
                <th class="w-48 px-3 py-2 font-medium">{{ t("fileManager.modified") }}</th>
                <th class="w-36 px-3 py-2 text-right font-medium">{{ t("fileManager.actions") }}</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="entry in entries" :key="entry.path" class="border-b" :class="{ 'cursor-pointer hover:bg-muted/50': entry.kind === 'directory' }" @dblclick="openEntry(entry)">
                <td class="px-3 py-2">
                  <button v-if="entry.kind === 'directory'" class="flex min-w-0 items-center gap-2" @click="openEntry(entry)">
                    <Folder class="h-4 w-4 shrink-0 text-amber-500" />
                    <span class="truncate">{{ entry.name }}</span>
                  </button>
                  <span v-else class="block truncate pl-6">{{ entry.name }}</span>
                </td>
                <td class="px-3 py-2 text-muted-foreground">{{ t(`fileManager.kind.${entry.kind}`) }}</td>
                <td class="px-3 py-2 text-right tabular-nums text-muted-foreground">{{ entry.kind === "file" ? formatFileSize(entry.size) : "—" }}</td>
                <td class="truncate px-3 py-2 text-muted-foreground">{{ entry.modifiedAt ? new Date(entry.modifiedAt).toLocaleString() : "—" }}</td>
                <td class="px-3 py-1 text-right">
                  <Button v-if="entry.kind === 'file' && activeConnection.capabilities.copy" variant="ghost" size="icon" class="h-7 w-7" :disabled="!!operationActive" :title="t('fileManager.copy')" @click="startRemoteOperation(entry, 'copy')">
                    <Loader2 v-if="operationActive === `copy:${entry.path}`" class="h-4 w-4 animate-spin" />
                    <Copy v-else class="h-4 w-4" />
                  </Button>
                  <Button v-if="entry.kind === 'file' && activeConnection.capabilities.rename" variant="ghost" size="icon" class="h-7 w-7" :disabled="!!operationActive" :title="t('fileManager.rename')" @click="startRemoteOperation(entry, 'rename')">
                    <Loader2 v-if="operationActive === `rename:${entry.path}`" class="h-4 w-4 animate-spin" />
                    <FilePenLine v-else class="h-4 w-4" />
                  </Button>
                  <Button v-if="entry.kind === 'file' && activeConnection.capabilities.read" variant="ghost" size="icon" class="h-7 w-7" :disabled="!!operationActive" :title="t('fileManager.download')" @click="startDownload(entry)">
                    <Loader2 v-if="operationActive === `download:${entry.path}`" class="h-4 w-4 animate-spin" />
                    <Download v-else class="h-4 w-4" />
                  </Button>
                  <Button v-if="activeConnection.capabilities.delete" variant="ghost" size="icon" class="h-7 w-7 text-destructive" :disabled="!!operationActive" :title="t('common.delete')" @click="deleteEntryTarget = entry">
                    <Loader2 v-if="operationActive === `delete:${entry.path}`" class="h-4 w-4 animate-spin" />
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
      </template>

      <template v-else>
        <div v-if="store.loading" class="flex flex-1 items-center justify-center text-muted-foreground">
          <Loader2 class="h-5 w-5 animate-spin" />
        </div>
        <div v-else-if="loadError" role="alert" class="p-4 text-sm text-destructive">{{ loadError }}</div>
        <div v-else-if="store.connections.length === 0" class="flex flex-1 flex-col items-center justify-center gap-3 text-muted-foreground">
          <Unplug class="h-8 w-8" />
          <p class="text-sm">{{ t("fileManager.noConnections") }}</p>
          <Button variant="outline" size="sm" @click="createConnection">{{ t("fileManager.newConnection") }}</Button>
        </div>
        <div v-else class="min-h-0 flex-1 overflow-auto">
          <table class="w-full table-fixed text-sm">
            <thead class="sticky top-0 bg-muted/70 text-left text-xs text-muted-foreground">
              <tr>
                <th class="w-[34%] px-3 py-2 font-medium">{{ t("fileManager.name") }}</th>
                <th class="w-24 px-3 py-2 font-medium">{{ t("fileManager.protocol") }}</th>
                <th class="px-3 py-2 font-medium">{{ t("fileManager.endpoint") }}</th>
                <th class="w-28 px-3 py-2 text-right font-medium">{{ t("fileManager.actions") }}</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="connection in store.connections" :key="connection.id" class="border-b">
                <td class="truncate px-3 py-2 font-medium">{{ connection.name }}</td>
                <td class="px-3 py-2 uppercase">{{ connection.config.protocol }}</td>
                <td class="truncate px-3 py-2 text-muted-foreground">{{ "endpoint" in connection.config ? connection.config.endpoint : connection.config.nameNodeUri }}</td>
                <td class="px-3 py-1 text-right">
                  <Button v-if="connection.capabilities.list" variant="ghost" size="icon" class="h-7 w-7" :title="t('fileManager.open')" @click="openConnection(connection)">
                    <FolderOpen class="h-4 w-4" />
                  </Button>
                  <Button variant="ghost" size="icon" class="h-7 w-7" :title="t('common.edit')" @click="editConnection(connection)">
                    <Pencil class="h-4 w-4" />
                  </Button>
                  <Button variant="ghost" size="icon" class="h-7 w-7 text-destructive" :title="t('common.delete')" @click="deleting = connection">
                    <Trash2 class="h-4 w-4" />
                  </Button>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </template>
    </section>

    <FileConnectionDialog v-model:open="dialogOpen" :connection="editing" @saved="toast(t('fileManager.connectionSaved'))" />

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

    <Dialog :open="!!replaceRequest" @update:open="(open) => !open && (replaceRequest = undefined)">
      <DialogContent class="sm:max-w-[420px]">
        <DialogHeader>
          <DialogTitle>{{ t("fileManager.replaceTitle") }}</DialogTitle>
        </DialogHeader>
        <p class="text-sm text-muted-foreground">{{ t("fileManager.replaceMessage") }}</p>
        <p class="truncate font-mono text-xs">{{ replaceDestination }}</p>
        <DialogFooter>
          <Button variant="outline" @click="replaceRequest = undefined">{{ t("common.cancel") }}</Button>
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
            <p v-if="remoteOperation?.operation === 'rename' && activeConnection?.capabilities.renameMode === 'copy_delete'">{{ t("fileManager.nonAtomicRenameNotice") }}</p>
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

    <Dialog :open="!!deleting" @update:open="(open) => !open && (deleting = undefined)">
      <DialogContent class="sm:max-w-[400px]">
        <DialogHeader>
          <DialogTitle>{{ t("fileManager.deleteConnectionTitle") }}</DialogTitle>
        </DialogHeader>
        <p class="text-sm text-muted-foreground">{{ t("fileManager.deleteConnectionMessage", { name: deleting?.name }) }}</p>
        <DialogFooter>
          <Button variant="outline" :disabled="deleteActive" @click="deleting = undefined">{{ t("common.cancel") }}</Button>
          <Button variant="destructive" :disabled="deleteActive" @click="removeConnection">{{ t("common.delete") }}</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  </div>
</template>
