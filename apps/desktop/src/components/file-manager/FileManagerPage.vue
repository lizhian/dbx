<script setup lang="ts">
import { onMounted, ref } from "vue";
import { useI18n } from "vue-i18n";
import { FolderOpen, Loader2, Pencil, Plus, Trash2, Unplug } from "@lucide/vue";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { useFileConnectionStore } from "@/stores/fileConnectionStore";
import { useToast } from "@/composables/useToast";
import { formatError } from "@/lib/backend/errorUtils";
import type { FileConnection } from "@/types/fileManager";
import FileConnectionDialog from "./FileConnectionDialog.vue";

const emit = defineEmits<{
  open: [connection: FileConnection];
}>();

const { t } = useI18n();
const { toast } = useToast();
const store = useFileConnectionStore();
const dialogOpen = ref(false);
const editing = ref<FileConnection>();
const deleting = ref<FileConnection>();
const deleteActive = ref(false);
const loadError = ref("");

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
      <h1 class="text-sm font-semibold">{{ t("fileManager.title") }}</h1>
      <Button size="sm" class="h-7" @click="createConnection">
        <Plus class="h-4 w-4" />
        {{ t("fileManager.newConnection") }}
      </Button>
    </header>

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
              <Button variant="ghost" size="icon" class="h-7 w-7" :title="t('fileManager.open')" @click="emit('open', connection)">
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
