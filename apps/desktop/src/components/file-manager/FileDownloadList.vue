<script setup lang="ts">
import { computed } from "vue";
import { useI18n } from "vue-i18n";
import { CheckCircle2, Download, File as FileIcon, FolderOpen, Loader2, XCircle } from "@lucide/vue";
import { Button } from "@/components/ui/button";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import { formatFileSize } from "./filePath";
import { fileDownloadProgressPercent, type FileDownloadTask } from "./fileDownload";

const props = defineProps<{
  tasks: FileDownloadTask[];
}>();

const emit = defineEmits<{
  openFile: [task: FileDownloadTask];
  openFolder: [task: FileDownloadTask];
}>();

const { t } = useI18n();
const activeCount = computed(() => props.tasks.filter((task) => task.status === "downloading" || task.status === "waiting").length);
const visibleTasks = computed(() => [...props.tasks].reverse());

function progressText(task: FileDownloadTask): string {
  if (task.status === "waiting") return t("fileManager.downloadWaiting");
  if (task.status === "failed") return task.error || t("fileManager.downloadFailed");
  if (task.status === "cancelled") return t("fileManager.downloadCancelled");
  if (task.status === "completed") return t("fileManager.downloadCompleted");
  if (task.totalBytes <= 0) return t("fileManager.downloading");
  return `${t("fileManager.downloading")} ${fileDownloadProgressPercent(task)}%`;
}

function sizeText(task: FileDownloadTask): string {
  if (task.status === "completed") return formatFileSize(task.bytesTransferred);
  if (task.totalBytes <= 0) return formatFileSize(task.bytesTransferred);
  return `${formatFileSize(task.bytesTransferred)} / ${formatFileSize(task.totalBytes)}`;
}
</script>

<template>
  <Popover>
    <PopoverTrigger as-child>
      <Button data-file-download-list-trigger variant="outline" size="sm" class="relative h-7 shrink-0">
        <Download class="h-4 w-4" />
        {{ t("fileManager.downloadList") }}
        <span v-if="activeCount" class="flex h-4 min-w-4 items-center justify-center rounded-full bg-primary px-1 text-[10px] leading-none text-primary-foreground">
          {{ activeCount > 9 ? "9+" : activeCount }}
        </span>
      </Button>
    </PopoverTrigger>

    <PopoverContent data-file-download-list align="end" class="w-[min(92vw,30rem)] gap-0 overflow-hidden p-0" :side-offset="8">
      <div class="border-b bg-muted/40 px-4 py-3 text-sm font-semibold">{{ t("fileManager.downloadList") }}</div>
      <p v-if="visibleTasks.length === 0" class="px-4 py-8 text-center text-sm text-muted-foreground">{{ t("fileManager.noDownloads") }}</p>
      <div v-else class="max-h-96 overflow-auto">
        <div v-for="task in visibleTasks" :key="task.id" :data-file-download-task="task.remotePath" class="flex items-center gap-2 border-b px-4 py-3 last:border-b-0">
          <div class="flex min-w-0 items-center gap-2">
            <Loader2 v-if="task.status === 'downloading' || task.status === 'waiting'" class="h-4 w-4 shrink-0 animate-spin text-primary" />
            <CheckCircle2 v-else-if="task.status === 'completed'" class="h-4 w-4 shrink-0 text-green-600" />
            <XCircle v-else class="h-4 w-4 shrink-0 text-destructive" />
            <FileIcon class="h-4 w-4 shrink-0 text-muted-foreground" />
            <span class="max-w-44 truncate text-sm font-medium" :title="task.remotePath">{{ task.fileName }}</span>
          </div>
          <span class="shrink-0 text-xs text-muted-foreground" :class="{ 'text-destructive': task.status === 'failed' }" :title="progressText(task)">{{ progressText(task) }}</span>
          <span class="shrink-0 text-xs tabular-nums text-muted-foreground" :title="sizeText(task)">{{ sizeText(task) }}</span>
          <div class="ml-auto flex shrink-0 items-center gap-1">
            <Button variant="ghost" size="icon" class="h-7 w-7" :disabled="task.status !== 'completed'" :title="t('fileManager.openDownloadedFile')" :aria-label="t('fileManager.openDownloadedFile')" @click="emit('openFile', task)">
              <FileIcon class="h-4 w-4" />
            </Button>
            <Button variant="ghost" size="icon" class="h-7 w-7" :disabled="task.status !== 'completed'" :title="t('fileManager.openDownloadFolder')" :aria-label="t('fileManager.openDownloadFolder')" @click="emit('openFolder', task)">
              <FolderOpen class="h-4 w-4" />
            </Button>
          </div>
        </div>
      </div>
    </PopoverContent>
  </Popover>
</template>
