<script setup lang="ts">
import { computed, reactive, watch } from "vue";
import { useI18n } from "vue-i18n";
import { AlertTriangle, FolderOpen, Loader2 } from "@lucide/vue";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import PasswordInput from "@/components/ui/PasswordInput.vue";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { useFileConnectionStore } from "@/stores/fileConnectionStore";
import { formatError } from "@/lib/backend/errorUtils";
import type { FileConnection } from "@/types/fileManager";
import { createFileConnectionDraft, createProtocolDraft, fileConnectionRequestFromDraft, type FileConnectionDraft } from "./fileConnectionDraft";

const props = defineProps<{
  open: boolean;
  connection?: FileConnection;
}>();

const emit = defineEmits<{
  "update:open": [value: boolean];
  saved: [connection: FileConnection];
}>();

const { t } = useI18n();
const store = useFileConnectionStore();
const draft = reactive<FileConnectionDraft>(createFileConnectionDraft());
const testing = reactive({ active: false, message: "", error: false });
const saving = reactive({ active: false, message: "" });
const isEditing = computed(() => !!props.connection);
const isWindows = typeof navigator !== "undefined" && /Windows/i.test(navigator.userAgent);
const canSubmit = computed(() => {
  const portValid = draft.protocol === "s3" || (draft.port > 0 && draft.port <= 65535);
  const common = !!draft.name.trim() && !!draft.endpoint.trim() && portValid && !!draft.root.trim();
  if (!common || (draft.protocol === "sftp" && isWindows)) return false;
  if (draft.protocol === "s3") {
    const accessKey = !!draft.accessKey || (!!props.connection?.secretStatus.accessKey && !draft.clearAccessKey);
    const secretKey = !!draft.secretKey || (!!props.connection?.secretStatus.secretKey && !draft.clearSecretKey);
    return !!draft.region.trim() && !!draft.bucket.trim() && accessKey && secretKey;
  }
  if (draft.protocol !== "sftp" || draft.authentication !== "private_key") return true;
  return !!draft.privateKey || (!!props.connection?.secretStatus.privateKey && !draft.clearPrivateKey);
});

watch(
  () => [props.open, props.connection] as const,
  ([open]) => {
    if (!open) return;
    Object.assign(draft, createFileConnectionDraft(props.connection));
    testing.message = "";
    testing.error = false;
    saving.message = "";
  },
  { immediate: true },
);

async function testConnection() {
  testing.active = true;
  testing.message = "";
  try {
    const request = fileConnectionRequestFromDraft(draft);
    await store.test({ id: isEditing.value ? request.id : undefined, config: request.config, secrets: request.secrets });
    testing.message = t("fileManager.testSucceeded");
    testing.error = false;
  } catch (error) {
    testing.message = formatError(error);
    testing.error = true;
  } finally {
    testing.active = false;
  }
}

async function saveConnection() {
  if (!canSubmit.value) return;
  saving.active = true;
  saving.message = "";
  try {
    const saved = await store.save(fileConnectionRequestFromDraft(draft));
    emit("saved", saved);
    emit("update:open", false);
  } catch (error) {
    saving.message = formatError(error);
  } finally {
    saving.active = false;
  }
}

function changeProtocol(value: unknown) {
  if (value !== "ftp" && value !== "sftp" && value !== "s3") return;
  Object.assign(draft, createProtocolDraft(value, draft));
}

async function selectPrivateKey() {
  try {
    const { open } = await import("@tauri-apps/plugin-dialog");
    const selected = await open({ multiple: false, directory: false, title: t("fileManager.selectPrivateKey") });
    if (!selected || Array.isArray(selected)) return;
    draft.privateKey = selected;
    draft.clearPrivateKey = false;
  } catch (error) {
    testing.message = formatError(error);
    testing.error = true;
  }
}
</script>

<template>
  <Dialog :open="open" @update:open="emit('update:open', $event)">
    <DialogContent class="max-h-[90vh] overflow-y-auto sm:max-w-[520px]">
      <DialogHeader>
        <DialogTitle>{{ isEditing ? t("fileManager.editConnection") : t("fileManager.newConnection") }}</DialogTitle>
      </DialogHeader>

      <form class="grid gap-4" @submit.prevent="saveConnection">
        <div class="grid gap-1.5">
          <Label for="file-connection-name">{{ t("fileManager.name") }}</Label>
          <Input id="file-connection-name" v-model="draft.name" autocomplete="off" />
        </div>

        <div class="grid gap-1.5">
          <Label>{{ t("fileManager.protocol") }}</Label>
          <Select :model-value="draft.protocol" :disabled="isEditing" @update:model-value="changeProtocol">
            <SelectTrigger>
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value="ftp">FTP</SelectItem>
              <SelectItem value="sftp">SFTP</SelectItem>
              <SelectItem value="s3">S3</SelectItem>
            </SelectContent>
          </Select>
        </div>

        <div class="grid gap-3" :class="{ 'grid-cols-[1fr_104px]': draft.protocol !== 's3' }">
          <div class="grid gap-1.5">
            <Label for="file-connection-endpoint">{{ t("fileManager.endpoint") }}</Label>
            <Input id="file-connection-endpoint" v-model="draft.endpoint" autocomplete="off" />
          </div>
          <div v-if="draft.protocol !== 's3'" class="grid gap-1.5">
            <Label for="file-connection-port">{{ t("fileManager.port") }}</Label>
            <Input id="file-connection-port" v-model.number="draft.port" type="number" min="1" max="65535" />
          </div>
        </div>

        <div class="grid gap-1.5">
          <Label for="file-connection-root">{{ t("fileManager.root") }}</Label>
          <Input id="file-connection-root" v-model="draft.root" autocomplete="off" />
        </div>

        <div v-if="draft.protocol !== 's3'" class="grid gap-1.5">
          <Label for="file-connection-username">{{ t("fileManager.username") }}</Label>
          <Input id="file-connection-username" v-model="draft.username" autocomplete="username" />
        </div>

        <template v-if="draft.protocol === 's3'">
          <div class="grid grid-cols-2 gap-3">
            <div class="grid gap-1.5">
              <Label for="file-connection-region">{{ t("fileManager.region") }}</Label>
              <Input id="file-connection-region" v-model="draft.region" autocomplete="off" />
            </div>
            <div class="grid gap-1.5">
              <Label for="file-connection-bucket">{{ t("fileManager.bucket") }}</Label>
              <Input id="file-connection-bucket" v-model="draft.bucket" autocomplete="off" />
            </div>
          </div>

          <div class="grid gap-1.5">
            <Label for="file-connection-access-key">{{ t("fileManager.accessKey") }}</Label>
            <PasswordInput id="file-connection-access-key" v-model="draft.accessKey" :disabled="draft.clearAccessKey" :placeholder="connection?.secretStatus.accessKey ? t('fileManager.secretPreserved') : undefined" autocomplete="off" />
            <label v-if="connection?.secretStatus.accessKey" class="inline-flex w-fit items-center gap-2 text-xs text-muted-foreground">
              <input v-model="draft.clearAccessKey" type="checkbox" />
              {{ t("fileManager.clearSavedAccessKey") }}
            </label>
          </div>

          <div class="grid gap-1.5">
            <Label for="file-connection-secret-key">{{ t("fileManager.secretKey") }}</Label>
            <PasswordInput id="file-connection-secret-key" v-model="draft.secretKey" :disabled="draft.clearSecretKey" :placeholder="connection?.secretStatus.secretKey ? t('fileManager.secretPreserved') : undefined" autocomplete="off" />
            <label v-if="connection?.secretStatus.secretKey" class="inline-flex w-fit items-center gap-2 text-xs text-muted-foreground">
              <input v-model="draft.clearSecretKey" type="checkbox" />
              {{ t("fileManager.clearSavedSecretKey") }}
            </label>
          </div>

          <div class="grid gap-1.5">
            <Label for="file-connection-session-token">{{ t("fileManager.sessionToken") }}</Label>
            <PasswordInput id="file-connection-session-token" v-model="draft.sessionToken" :disabled="draft.clearSessionToken" :placeholder="connection?.secretStatus.sessionToken ? t('fileManager.secretPreserved') : t('fileManager.optional')" autocomplete="off" />
            <label v-if="connection?.secretStatus.sessionToken" class="inline-flex w-fit items-center gap-2 text-xs text-muted-foreground">
              <input v-model="draft.clearSessionToken" type="checkbox" />
              {{ t("fileManager.clearSavedSessionToken") }}
            </label>
          </div>

          <label class="inline-flex w-fit items-center gap-2 text-sm">
            <input v-model="draft.pathStyle" type="checkbox" />
            {{ t("fileManager.pathStyle") }}
          </label>
        </template>

        <div v-if="draft.protocol === 'ftp'" class="grid gap-1.5">
          <Label for="file-connection-password">{{ t("fileManager.password") }}</Label>
          <PasswordInput id="file-connection-password" v-model="draft.password" :disabled="draft.clearPassword" :placeholder="connection?.secretStatus.password ? t('fileManager.secretPreserved') : undefined" autocomplete="new-password" />
          <label v-if="connection?.secretStatus.password" class="inline-flex w-fit items-center gap-2 text-xs text-muted-foreground">
            <input v-model="draft.clearPassword" type="checkbox" />
            {{ t("fileManager.clearSavedPassword") }}
          </label>
        </div>

        <template v-if="draft.protocol === 'sftp'">
          <div class="grid gap-1.5">
            <Label>{{ t("fileManager.authentication") }}</Label>
            <Select v-model="draft.authentication">
              <SelectTrigger>
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="ssh_config">{{ t("fileManager.sshConfig") }}</SelectItem>
                <SelectItem value="ssh_agent">{{ t("fileManager.sshAgent") }}</SelectItem>
                <SelectItem value="private_key">{{ t("fileManager.privateKey") }}</SelectItem>
              </SelectContent>
            </Select>
          </div>

          <div v-if="draft.authentication === 'private_key'" class="grid gap-1.5">
            <Label for="file-connection-private-key">{{ t("fileManager.privateKey") }}</Label>
            <div class="flex gap-2">
              <Input id="file-connection-private-key" :model-value="draft.privateKey" :placeholder="connection?.secretStatus.privateKey ? t('fileManager.privateKeyPreserved') : undefined" disabled />
              <Button type="button" variant="outline" size="icon" :title="t('fileManager.selectPrivateKey')" :disabled="draft.clearPrivateKey" @click="selectPrivateKey">
                <FolderOpen class="h-4 w-4" />
              </Button>
            </div>
            <label v-if="connection?.secretStatus.privateKey" class="inline-flex w-fit items-center gap-2 text-xs text-muted-foreground">
              <input v-model="draft.clearPrivateKey" type="checkbox" />
              {{ t("fileManager.clearSavedPrivateKey") }}
            </label>
          </div>
        </template>

        <div v-if="draft.protocol === 'ftp'" role="alert" class="flex gap-2 border border-amber-500/40 bg-amber-500/10 p-3 text-xs text-amber-800 dark:text-amber-200">
          <AlertTriangle class="mt-0.5 h-4 w-4 shrink-0" />
          <span>{{ t("fileManager.ftpWarning") }}</span>
        </div>
        <div v-else-if="draft.protocol === 'sftp'" role="alert" class="flex gap-2 border border-amber-500/40 bg-amber-500/10 p-3 text-xs text-amber-800 dark:text-amber-200">
          <AlertTriangle class="mt-0.5 h-4 w-4 shrink-0" />
          <span>{{ t(isWindows ? "fileManager.sftpWindowsUnsupported" : "fileManager.sftpAuthenticationNotice") }}</span>
        </div>

        <p v-if="testing.message" role="status" class="text-xs" :class="testing.error ? 'text-destructive' : 'text-emerald-600'">{{ testing.message }}</p>
        <p v-if="saving.message" role="alert" class="text-xs text-destructive">{{ saving.message }}</p>
      </form>

      <DialogFooter>
        <Button variant="outline" :disabled="testing.active || saving.active" @click="emit('update:open', false)">{{ t("common.cancel") }}</Button>
        <Button variant="outline" :disabled="!canSubmit || testing.active || saving.active" @click="testConnection">
          <Loader2 v-if="testing.active" class="h-4 w-4 animate-spin" />
          {{ t("fileManager.test") }}
        </Button>
        <Button :disabled="!canSubmit || testing.active || saving.active" @click="saveConnection">
          <Loader2 v-if="saving.active" class="h-4 w-4 animate-spin" />
          {{ t("common.save") }}
        </Button>
      </DialogFooter>
    </DialogContent>
  </Dialog>
</template>
