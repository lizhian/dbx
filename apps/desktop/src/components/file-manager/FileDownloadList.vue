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

    <PopoverContent data-file-download-list align="end" class="w-[min(94vw,52rem)] gap-0 overflow-hidden p-0" :side-offset="8">
      <div class="border-b bg-muted/40 px-4 py-3 text-sm font-semibold">{{ t("fileManager.downloadList") }}</div>
      <p v-if="visibleTasks.length === 0" class="px-4 py-8 text-center text-sm text-muted-foreground">{{ t("fileManager.noDownloads") }}</p>
      <div v-else class="max-h-96 overflow-auto">
        <div class="grid min-w-[46rem] grid-cols-[minmax(10rem,1fr)_10rem_9rem_7rem_7rem] items-center gap-2 border-b bg-muted/20 px-3 py-2 text-xs font-medium text-muted-foreground">
          <span>{{ t("fileManager.fileName") }}</span>
          <span>{{ t("fileManager.downloadStatus") }}</span>
          <span>{{ t("fileManager.size") }}</span>
          <span>{{ t("fileManager.openDownloadedFile") }}</span>
          <span>{{ t("fileManager.openDownloadFolder") }}</span>
        </div>
        <div v-for="task in visibleTasks" :key="task.id" :data-file-download-task="task.remotePath" class="grid min-w-[46rem] grid-cols-[minmax(10rem,1fr)_10rem_9rem_7rem_7rem] items-center gap-2 border-b px-3 py-2 last:border-b-0">
          <div class="flex min-w-0 items-center gap-2">
            <Loader2 v-if="task.status === 'downloading' || task.status === 'waiting'" class="h-4 w-4 shrink-0 animate-spin text-primary" />
            <CheckCircle2 v-else-if="task.status === 'completed'" class="h-4 w-4 shrink-0 text-green-600" />
            <XCircle v-else class="h-4 w-4 shrink-0 text-destructive" />
            <FileIcon class="h-4 w-4 shrink-0 text-muted-foreground" />
            <span class="truncate text-sm font-medium" :title="task.remotePath">{{ task.fileName }}</span>
          </div>
          <div class="min-w-0 space-y-1">
            <p class="truncate text-xs text-muted-foreground" :class="{ 'text-destructive': task.status === 'failed' }" :title="progressText(task)">{{ progressText(task) }}</p>
            <div v-if="task.status === 'downloading'" class="h-1.5 overflow-hidden rounded-full bg-muted">
              <div v-if="task.totalBytes > 0" class="h-full rounded-full bg-primary transition-[width] duration-200" :style="{ width: `${fileDownloadProgressPercent(task)}%` }" />
              <div v-else class="file-download-progress-indeterminate h-full rounded-full bg-primary" />
            </div>
          </div>
          <span class="truncate text-xs tabular-nums text-muted-foreground" :title="sizeText(task)">{{ sizeText(task) }}</span>
          <Button variant="ghost" size="sm" class="h-7 justify-start px-2 text-xs" :disabled="task.status !== 'completed'" :title="t('fileManager.openDownloadedFile')" @click="emit('openFile', task)">
            <FileIcon class="h-3.5 w-3.5" />
            {{ t("fileManager.openDownloadedFile") }}
          </Button>
          <Button variant="ghost" size="sm" class="h-7 justify-start px-2 text-xs" :disabled="task.status !== 'completed'" :title="t('fileManager.openDownloadFolder')" @click="emit('openFolder', task)">
            <FolderOpen class="h-3.5 w-3.5" />
            {{ t("fileManager.openDownloadFolder") }}
          </Button>
        </div>
      </div>
    </PopoverContent>
  </Popover>
</template>

<style scoped>
.file-download-progress-indeterminate {
  width: 42%;
  animation: file-download-progress-slide 1.15s ease-in-out infinite;
}

@keyframes file-download-progress-slide {
  0% {
    transform: translateX(-110%);
  }
  50% {
    transform: translateX(70%);
  }
  100% {
    transform: translateX(250%);
  }
}
</style>
