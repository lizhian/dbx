<script setup lang="ts">
import { AlertTriangle, FolderOpen } from "@lucide/vue";
import { useI18n } from "vue-i18n";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import PasswordInput from "@/components/ui/PasswordInput.vue";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import type { FileSecretStatus } from "@/types/fileManager";
import type { FileConnectionDraft } from "@/components/file-manager/fileConnectionDraft";

const props = defineProps<{
  draft: FileConnectionDraft;
  secretStatus?: FileSecretStatus;
}>();

const emit = defineEmits<{
  error: [message: string];
}>();

const { t } = useI18n();
const isWindows = typeof navigator !== "undefined" && /Windows/i.test(navigator.userAgent);

async function selectPrivateKey() {
  try {
    const { open } = await import("@tauri-apps/plugin-dialog");
    const selected = await open({ multiple: false, directory: false, title: t("fileManager.selectPrivateKey") });
    if (!selected || Array.isArray(selected)) return;
    props.draft.privateKey = selected;
    props.draft.clearPrivateKey = false;
  } catch (error) {
    emit("error", String(error));
  }
}

async function selectHadoopConfigDirectory() {
  try {
    const { open } = await import("@tauri-apps/plugin-dialog");
    const selected = await open({ multiple: false, directory: true, title: t("fileManager.selectHadoopConfigDirectory") });
    if (!selected || Array.isArray(selected)) return;
    props.draft.hadoopConfigDirectory = selected;
  } catch (error) {
    emit("error", String(error));
  }
}
</script>

<template>
  <div class="grid gap-4" data-file-connection-fields>
    <div v-if="draft.protocol !== 'hdfs' || draft.hdfsImplementation === 'webhdfs'" class="grid gap-3" :class="{ 'grid-cols-[1fr_104px]': draft.protocol === 'ftp' || draft.protocol === 'sftp' }">
      <div class="grid gap-1.5">
        <Label for="file-connection-endpoint">{{ t("fileManager.endpoint") }}</Label>
        <Input id="file-connection-endpoint" v-model="draft.endpoint" autocomplete="off" />
      </div>
      <div v-if="draft.protocol === 'ftp' || draft.protocol === 'sftp'" class="grid gap-1.5">
        <Label for="file-connection-port">{{ t("fileManager.port") }}</Label>
        <Input id="file-connection-port" v-model.number="draft.port" type="number" min="1" max="65535" />
      </div>
    </div>

    <div class="grid gap-1.5">
      <Label for="file-connection-root">{{ t("fileManager.root") }}</Label>
      <Input id="file-connection-root" v-model="draft.root" autocomplete="off" />
    </div>

    <div v-if="draft.protocol === 'ftp' || draft.protocol === 'sftp'" class="grid gap-1.5">
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
        <PasswordInput id="file-connection-access-key" v-model="draft.accessKey" :disabled="draft.clearAccessKey" :placeholder="secretStatus?.accessKey ? t('fileManager.secretPreserved') : undefined" autocomplete="off" />
        <label v-if="secretStatus?.accessKey" class="inline-flex w-fit items-center gap-2 text-xs text-muted-foreground">
          <input v-model="draft.clearAccessKey" type="checkbox" />
          {{ t("fileManager.clearSavedAccessKey") }}
        </label>
      </div>

      <div class="grid gap-1.5">
        <Label for="file-connection-secret-key">{{ t("fileManager.secretKey") }}</Label>
        <PasswordInput id="file-connection-secret-key" v-model="draft.secretKey" :disabled="draft.clearSecretKey" :placeholder="secretStatus?.secretKey ? t('fileManager.secretPreserved') : undefined" autocomplete="off" />
        <label v-if="secretStatus?.secretKey" class="inline-flex w-fit items-center gap-2 text-xs text-muted-foreground">
          <input v-model="draft.clearSecretKey" type="checkbox" />
          {{ t("fileManager.clearSavedSecretKey") }}
        </label>
      </div>

      <div class="grid gap-1.5">
        <Label for="file-connection-session-token">{{ t("fileManager.sessionToken") }}</Label>
        <PasswordInput id="file-connection-session-token" v-model="draft.sessionToken" :disabled="draft.clearSessionToken" :placeholder="secretStatus?.sessionToken ? t('fileManager.secretPreserved') : t('fileManager.optional')" autocomplete="off" />
        <label v-if="secretStatus?.sessionToken" class="inline-flex w-fit items-center gap-2 text-xs text-muted-foreground">
          <input v-model="draft.clearSessionToken" type="checkbox" />
          {{ t("fileManager.clearSavedSessionToken") }}
        </label>
      </div>

      <label class="inline-flex w-fit items-center gap-2 text-sm">
        <input v-model="draft.pathStyle" type="checkbox" />
        {{ t("fileManager.pathStyle") }}
      </label>
    </template>

    <template v-if="draft.protocol === 'webdav'">
      <div class="grid gap-1.5">
        <Label>{{ t("fileManager.authentication") }}</Label>
        <Select v-model="draft.webdavAuthentication">
          <SelectTrigger><SelectValue /></SelectTrigger>
          <SelectContent>
            <SelectItem value="basic">{{ t("fileManager.basicAuthentication") }}</SelectItem>
            <SelectItem value="bearer">{{ t("fileManager.bearerAuthentication") }}</SelectItem>
          </SelectContent>
        </Select>
      </div>

      <div v-if="draft.webdavAuthentication === 'basic'" class="grid gap-1.5">
        <Label for="file-webdav-username">{{ t("fileManager.username") }}</Label>
        <Input id="file-webdav-username" v-model="draft.username" autocomplete="username" />
      </div>

      <div v-if="draft.webdavAuthentication === 'bearer'" class="grid gap-1.5">
        <Label for="file-connection-bearer-token">{{ t("fileManager.bearerToken") }}</Label>
        <PasswordInput id="file-connection-bearer-token" v-model="draft.bearerToken" :disabled="draft.clearBearerToken" :placeholder="secretStatus?.bearerToken ? t('fileManager.secretPreserved') : undefined" autocomplete="off" />
        <label v-if="secretStatus?.bearerToken" class="inline-flex w-fit items-center gap-2 text-xs text-muted-foreground">
          <input v-model="draft.clearBearerToken" type="checkbox" />
          {{ t("fileManager.clearSavedBearerToken") }}
        </label>
      </div>
    </template>

    <div v-if="draft.protocol === 'ftp' || (draft.protocol === 'webdav' && draft.webdavAuthentication === 'basic')" class="grid gap-1.5">
      <Label for="file-connection-password">{{ t("fileManager.password") }}</Label>
      <PasswordInput id="file-connection-password" v-model="draft.password" :disabled="draft.clearPassword" :placeholder="secretStatus?.password ? t('fileManager.secretPreserved') : undefined" autocomplete="new-password" />
      <label v-if="secretStatus?.password" class="inline-flex w-fit items-center gap-2 text-xs text-muted-foreground">
        <input v-model="draft.clearPassword" type="checkbox" />
        {{ t("fileManager.clearSavedPassword") }}
      </label>
    </div>

    <template v-if="draft.protocol === 'hdfs' && draft.hdfsImplementation === 'webhdfs'">
      <label class="inline-flex w-fit items-center gap-2 text-sm">
        <input v-model="draft.useDelegationToken" type="checkbox" />
        {{ t("fileManager.useDelegationToken") }}
      </label>
      <div v-if="!draft.useDelegationToken" class="grid gap-1.5">
        <Label for="file-connection-simple-user">{{ t("fileManager.simpleUser") }}</Label>
        <Input id="file-connection-simple-user" v-model="draft.simpleUser" autocomplete="username" />
      </div>
      <div v-else class="grid gap-1.5">
        <Label for="file-connection-delegation-token">{{ t("fileManager.delegationToken") }}</Label>
        <PasswordInput id="file-connection-delegation-token" v-model="draft.delegationToken" :disabled="draft.clearDelegationToken" :placeholder="secretStatus?.delegationToken ? t('fileManager.secretPreserved') : undefined" autocomplete="off" />
        <label v-if="secretStatus?.delegationToken" class="inline-flex w-fit items-center gap-2 text-xs text-muted-foreground">
          <input v-model="draft.clearDelegationToken" type="checkbox" />
          {{ t("fileManager.clearSavedDelegationToken") }}
        </label>
      </div>
    </template>

    <template v-if="draft.protocol === 'hdfs' && draft.hdfsImplementation === 'native'">
      <div class="grid gap-1.5">
        <Label for="file-connection-name-node-uri">{{ t("fileManager.nameNodeUri") }}</Label>
        <Input id="file-connection-name-node-uri" v-model="draft.nameNodeUri" autocomplete="off" />
      </div>
      <div class="grid gap-1.5">
        <Label for="file-connection-hadoop-config-directory">{{ t("fileManager.hadoopConfigDirectory") }}</Label>
        <div class="flex gap-2">
          <Input id="file-connection-hadoop-config-directory" :model-value="draft.hadoopConfigDirectory" disabled />
          <Button type="button" variant="outline" size="icon" :title="t('fileManager.selectHadoopConfigDirectory')" :aria-label="t('fileManager.selectHadoopConfigDirectory')" @click="selectHadoopConfigDirectory">
            <FolderOpen class="h-4 w-4" />
          </Button>
        </div>
      </div>
    </template>

    <template v-if="draft.protocol === 'sftp'">
      <div class="grid gap-1.5">
        <Label>{{ t("fileManager.authentication") }}</Label>
        <Select v-model="draft.authentication">
          <SelectTrigger><SelectValue /></SelectTrigger>
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
          <Input id="file-connection-private-key" v-model="draft.privateKey" :placeholder="secretStatus?.privateKey ? t('fileManager.privateKeyPreserved') : undefined" :disabled="draft.clearPrivateKey" autocomplete="off" />
          <Button type="button" variant="outline" size="icon" :title="t('fileManager.selectPrivateKey')" :disabled="draft.clearPrivateKey" @click="selectPrivateKey">
            <FolderOpen class="h-4 w-4" />
          </Button>
        </div>
        <label v-if="secretStatus?.privateKey" class="inline-flex w-fit items-center gap-2 text-xs text-muted-foreground">
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
  </div>
</template>
