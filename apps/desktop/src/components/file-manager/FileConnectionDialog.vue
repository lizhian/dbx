<script setup lang="ts">
import { computed, reactive, watch } from "vue";
import { useI18n } from "vue-i18n";
import { AlertTriangle, Loader2 } from "@lucide/vue";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import PasswordInput from "@/components/ui/PasswordInput.vue";
import { useFileConnectionStore } from "@/stores/fileConnectionStore";
import { formatError } from "@/lib/backend/errorUtils";
import type { FileConnection } from "@/types/fileManager";
import { createFtpConnectionDraft, ftpRequestFromDraft, type FtpConnectionDraft } from "./fileConnectionDraft";

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
const draft = reactive<FtpConnectionDraft>(createFtpConnectionDraft());
const testing = reactive({ active: false, message: "", error: false });
const saving = reactive({ active: false, message: "" });
const isEditing = computed(() => !!props.connection);
const canSubmit = computed(() => !!draft.name.trim() && !!draft.endpoint.trim() && draft.port > 0 && draft.port <= 65535 && !!draft.root.trim());

watch(
  () => [props.open, props.connection] as const,
  ([open]) => {
    if (!open) return;
    Object.assign(draft, createFtpConnectionDraft(props.connection));
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
    const request = ftpRequestFromDraft(draft);
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
    const saved = await store.save(ftpRequestFromDraft(draft));
    emit("saved", saved);
    emit("update:open", false);
  } catch (error) {
    saving.message = formatError(error);
  } finally {
    saving.active = false;
  }
}
</script>

<template>
  <Dialog :open="open" @update:open="emit('update:open', $event)">
    <DialogContent class="sm:max-w-[520px]">
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
          <Input value="FTP" disabled />
        </div>

        <div class="grid grid-cols-[1fr_104px] gap-3">
          <div class="grid gap-1.5">
            <Label for="file-connection-endpoint">{{ t("fileManager.endpoint") }}</Label>
            <Input id="file-connection-endpoint" v-model="draft.endpoint" autocomplete="off" />
          </div>
          <div class="grid gap-1.5">
            <Label for="file-connection-port">{{ t("fileManager.port") }}</Label>
            <Input id="file-connection-port" v-model.number="draft.port" type="number" min="1" max="65535" />
          </div>
        </div>

        <div class="grid gap-1.5">
          <Label for="file-connection-root">{{ t("fileManager.root") }}</Label>
          <Input id="file-connection-root" v-model="draft.root" autocomplete="off" />
        </div>

        <div class="grid gap-1.5">
          <Label for="file-connection-username">{{ t("fileManager.username") }}</Label>
          <Input id="file-connection-username" v-model="draft.username" autocomplete="username" />
        </div>

        <div class="grid gap-1.5">
          <Label for="file-connection-password">{{ t("fileManager.password") }}</Label>
          <PasswordInput id="file-connection-password" v-model="draft.password" :disabled="draft.clearPassword" :placeholder="connection?.secretStatus.password ? t('fileManager.secretPreserved') : undefined" autocomplete="new-password" />
          <label v-if="connection?.secretStatus.password" class="inline-flex w-fit items-center gap-2 text-xs text-muted-foreground">
            <input v-model="draft.clearPassword" type="checkbox" />
            {{ t("fileManager.clearSavedPassword") }}
          </label>
        </div>

        <div role="alert" class="flex gap-2 border border-amber-500/40 bg-amber-500/10 p-3 text-xs text-amber-800 dark:text-amber-200">
          <AlertTriangle class="mt-0.5 h-4 w-4 shrink-0" />
          <span>{{ t("fileManager.ftpWarning") }}</span>
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
