<script setup lang="ts">
import { computed, onMounted, ref } from "vue";
import { useI18n } from "vue-i18n";
import { ArrowLeft, ChevronLeft, Folder, FolderOpen, Loader2, Pencil, Plus, RefreshCw, Trash2, Unplug } from "@lucide/vue";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { useFileConnectionStore } from "@/stores/fileConnectionStore";
import { useToast } from "@/composables/useToast";
import { formatError } from "@/lib/backend/errorUtils";
import * as api from "@/lib/backend/api";
import type { FileConnection, FileEntry } from "@/types/fileManager";
import FileConnectionDialog from "./FileConnectionDialog.vue";
import { displayFilePath, formatFileSize, parentFilePath } from "./filePath";

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
</template>
