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
const fieldRowClass = "grid grid-cols-4 items-center gap-4";
const fieldTopRowClass = "grid grid-cols-4 items-start gap-4";
const fieldLabelClass = "justify-self-start text-left";
const fieldLabelTopClass = `${fieldLabelClass} mt-2`;

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
    <div v-if="draft.protocol !== 'hdfs' || draft.hdfsImplementation === 'webhdfs'" :class="fieldRowClass">
      <Label for="file-connection-endpoint" :class="fieldLabelClass">{{ t("fileManager.endpoint") }}</Label>
      <div class="col-span-3 grid min-w-0 gap-3" :class="{ 'grid-cols-[minmax(0,1fr)_104px]': draft.protocol === 'ftp' || draft.protocol === 'sftp' }">
        <Input id="file-connection-endpoint" v-model="draft.endpoint" class="min-w-0" autocomplete="off" />
        <Input v-if="draft.protocol === 'ftp' || draft.protocol === 'sftp'" id="file-connection-port" v-model.number="draft.port" type="number" min="1" max="65535" :aria-label="t('fileManager.port')" :title="t('fileManager.port')" />
      </div>
    </div>

    <div :class="fieldRowClass">
      <Label for="file-connection-root" :class="fieldLabelClass">{{ t("fileManager.root") }}</Label>
      <Input id="file-connection-root" v-model="draft.root" class="col-span-3" autocomplete="off" />
    </div>

    <div v-if="draft.protocol === 'ftp' || draft.protocol === 'sftp'" :class="fieldRowClass">
      <Label for="file-connection-username" :class="fieldLabelClass">{{ t("fileManager.username") }}</Label>
      <Input id="file-connection-username" v-model="draft.username" class="col-span-3" autocomplete="username" />
    </div>

    <template v-if="draft.protocol === 's3'">
      <div :class="fieldRowClass">
        <Label for="file-connection-region" :class="fieldLabelClass">{{ t("fileManager.region") }}</Label>
        <Input id="file-connection-region" v-model="draft.region" class="col-span-3" autocomplete="off" />
      </div>

      <div :class="fieldRowClass">
        <Label for="file-connection-bucket" :class="fieldLabelClass">{{ t("fileManager.bucket") }}</Label>
        <Input id="file-connection-bucket" v-model="draft.bucket" class="col-span-3" autocomplete="off" />
      </div>

      <div :class="fieldTopRowClass">
        <Label for="file-connection-access-key" :class="fieldLabelTopClass">{{ t("fileManager.accessKey") }}</Label>
        <div class="col-span-3 grid min-w-0 gap-1.5">
          <PasswordInput id="file-connection-access-key" v-model="draft.accessKey" :disabled="draft.clearAccessKey" :placeholder="secretStatus?.accessKey ? t('fileManager.secretPreserved') : undefined" autocomplete="off" />
          <label v-if="secretStatus?.accessKey" class="inline-flex w-fit items-center gap-2 text-xs text-muted-foreground">
            <input v-model="draft.clearAccessKey" type="checkbox" />
            {{ t("fileManager.clearSavedAccessKey") }}
          </label>
        </div>
      </div>

      <div :class="fieldTopRowClass">
        <Label for="file-connection-secret-key" :class="fieldLabelTopClass">{{ t("fileManager.secretKey") }}</Label>
        <div class="col-span-3 grid min-w-0 gap-1.5">
          <PasswordInput id="file-connection-secret-key" v-model="draft.secretKey" :disabled="draft.clearSecretKey" :placeholder="secretStatus?.secretKey ? t('fileManager.secretPreserved') : undefined" autocomplete="off" />
          <label v-if="secretStatus?.secretKey" class="inline-flex w-fit items-center gap-2 text-xs text-muted-foreground">
            <input v-model="draft.clearSecretKey" type="checkbox" />
            {{ t("fileManager.clearSavedSecretKey") }}
          </label>
        </div>
      </div>

      <div :class="fieldTopRowClass">
        <Label for="file-connection-session-token" :class="fieldLabelTopClass">{{ t("fileManager.sessionToken") }}</Label>
        <div class="col-span-3 grid min-w-0 gap-1.5">
          <PasswordInput id="file-connection-session-token" v-model="draft.sessionToken" :disabled="draft.clearSessionToken" :placeholder="secretStatus?.sessionToken ? t('fileManager.secretPreserved') : t('fileManager.optional')" autocomplete="off" />
          <label v-if="secretStatus?.sessionToken" class="inline-flex w-fit items-center gap-2 text-xs text-muted-foreground">
            <input v-model="draft.clearSessionToken" type="checkbox" />
            {{ t("fileManager.clearSavedSessionToken") }}
          </label>
        </div>
      </div>

      <div :class="fieldRowClass">
        <Label for="file-connection-path-style" :class="fieldLabelClass">{{ t("fileManager.pathStyle") }}</Label>
        <input id="file-connection-path-style" v-model="draft.pathStyle" class="col-span-3 h-4 w-4" type="checkbox" />
      </div>
    </template>

    <template v-if="draft.protocol === 'webdav'">
      <div :class="fieldRowClass">
        <Label :class="fieldLabelClass">{{ t("fileManager.authentication") }}</Label>
        <div class="col-span-3 min-w-0">
          <Select v-model="draft.webdavAuthentication">
            <SelectTrigger><SelectValue /></SelectTrigger>
            <SelectContent>
              <SelectItem value="basic">{{ t("fileManager.basicAuthentication") }}</SelectItem>
              <SelectItem value="bearer">{{ t("fileManager.bearerAuthentication") }}</SelectItem>
            </SelectContent>
          </Select>
        </div>
      </div>

      <div v-if="draft.webdavAuthentication === 'basic'" :class="fieldRowClass">
        <Label for="file-webdav-username" :class="fieldLabelClass">{{ t("fileManager.username") }}</Label>
        <Input id="file-webdav-username" v-model="draft.username" class="col-span-3" autocomplete="username" />
      </div>

      <div v-if="draft.webdavAuthentication === 'bearer'" :class="fieldTopRowClass">
        <Label for="file-connection-bearer-token" :class="fieldLabelTopClass">{{ t("fileManager.bearerToken") }}</Label>
        <div class="col-span-3 grid min-w-0 gap-1.5">
          <PasswordInput id="file-connection-bearer-token" v-model="draft.bearerToken" :disabled="draft.clearBearerToken" :placeholder="secretStatus?.bearerToken ? t('fileManager.secretPreserved') : undefined" autocomplete="off" />
          <label v-if="secretStatus?.bearerToken" class="inline-flex w-fit items-center gap-2 text-xs text-muted-foreground">
            <input v-model="draft.clearBearerToken" type="checkbox" />
            {{ t("fileManager.clearSavedBearerToken") }}
          </label>
        </div>
      </div>
    </template>

    <div v-if="draft.protocol === 'ftp' || (draft.protocol === 'webdav' && draft.webdavAuthentication === 'basic')" :class="fieldTopRowClass">
      <Label for="file-connection-password" :class="fieldLabelTopClass">{{ t("fileManager.password") }}</Label>
      <div class="col-span-3 grid min-w-0 gap-1.5">
        <PasswordInput id="file-connection-password" v-model="draft.password" :disabled="draft.clearPassword" :placeholder="secretStatus?.password ? t('fileManager.secretPreserved') : undefined" autocomplete="new-password" />
        <label v-if="secretStatus?.password" class="inline-flex w-fit items-center gap-2 text-xs text-muted-foreground">
          <input v-model="draft.clearPassword" type="checkbox" />
          {{ t("fileManager.clearSavedPassword") }}
        </label>
      </div>
    </div>

    <template v-if="draft.protocol === 'hdfs' && draft.hdfsImplementation === 'webhdfs'">
      <div :class="fieldRowClass">
        <Label for="file-connection-use-delegation-token" :class="fieldLabelClass">{{ t("fileManager.useDelegationToken") }}</Label>
        <input id="file-connection-use-delegation-token" v-model="draft.useDelegationToken" class="col-span-3 h-4 w-4" type="checkbox" />
      </div>
      <div v-if="!draft.useDelegationToken" :class="fieldRowClass">
        <Label for="file-connection-simple-user" :class="fieldLabelClass">{{ t("fileManager.simpleUser") }}</Label>
        <Input id="file-connection-simple-user" v-model="draft.simpleUser" class="col-span-3" autocomplete="username" />
      </div>
      <div v-else :class="fieldTopRowClass">
        <Label for="file-connection-delegation-token" :class="fieldLabelTopClass">{{ t("fileManager.delegationToken") }}</Label>
        <div class="col-span-3 grid min-w-0 gap-1.5">
          <PasswordInput id="file-connection-delegation-token" v-model="draft.delegationToken" :disabled="draft.clearDelegationToken" :placeholder="secretStatus?.delegationToken ? t('fileManager.secretPreserved') : undefined" autocomplete="off" />
          <label v-if="secretStatus?.delegationToken" class="inline-flex w-fit items-center gap-2 text-xs text-muted-foreground">
            <input v-model="draft.clearDelegationToken" type="checkbox" />
            {{ t("fileManager.clearSavedDelegationToken") }}
          </label>
        </div>
      </div>
    </template>

    <template v-if="draft.protocol === 'hdfs' && draft.hdfsImplementation === 'native'">
      <div :class="fieldRowClass">
        <Label for="file-connection-name-node-uri" :class="fieldLabelClass">{{ t("fileManager.nameNodeUri") }}</Label>
        <Input id="file-connection-name-node-uri" v-model="draft.nameNodeUri" class="col-span-3" autocomplete="off" />
      </div>
      <div :class="fieldRowClass">
        <Label for="file-connection-hadoop-config-directory" :class="fieldLabelClass">{{ t("fileManager.hadoopConfigDirectory") }}</Label>
        <div class="col-span-3 flex min-w-0 gap-2">
          <Input id="file-connection-hadoop-config-directory" :model-value="draft.hadoopConfigDirectory" disabled />
          <Button type="button" variant="outline" size="icon" :title="t('fileManager.selectHadoopConfigDirectory')" :aria-label="t('fileManager.selectHadoopConfigDirectory')" @click="selectHadoopConfigDirectory">
            <FolderOpen class="h-4 w-4" />
          </Button>
        </div>
      </div>
    </template>

    <template v-if="draft.protocol === 'sftp'">
      <div :class="fieldRowClass">
        <Label :class="fieldLabelClass">{{ t("fileManager.authentication") }}</Label>
        <div class="col-span-3 min-w-0">
          <Select v-model="draft.authentication">
            <SelectTrigger><SelectValue /></SelectTrigger>
            <SelectContent>
              <SelectItem value="ssh_config">{{ t("fileManager.sshConfig") }}</SelectItem>
              <SelectItem value="ssh_agent">{{ t("fileManager.sshAgent") }}</SelectItem>
              <SelectItem value="private_key">{{ t("fileManager.privateKey") }}</SelectItem>
            </SelectContent>
          </Select>
        </div>
      </div>
      <div v-if="draft.authentication === 'private_key'" :class="fieldTopRowClass">
        <Label for="file-connection-private-key" :class="fieldLabelTopClass">{{ t("fileManager.privateKey") }}</Label>
        <div class="col-span-3 grid min-w-0 gap-1.5">
          <div class="flex min-w-0 gap-2">
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
      </div>
    </template>

    <div v-if="draft.protocol === 'ftp'" :class="fieldTopRowClass">
      <span />
      <div role="alert" class="col-span-3 flex gap-2 border border-amber-500/40 bg-amber-500/10 p-3 text-xs text-amber-800 dark:text-amber-200">
        <AlertTriangle class="mt-0.5 h-4 w-4 shrink-0" />
        <span>{{ t("fileManager.ftpWarning") }}</span>
      </div>
    </div>
    <div v-else-if="draft.protocol === 'sftp'" :class="fieldTopRowClass">
      <span />
      <div role="alert" class="col-span-3 flex gap-2 border border-amber-500/40 bg-amber-500/10 p-3 text-xs text-amber-800 dark:text-amber-200">
        <AlertTriangle class="mt-0.5 h-4 w-4 shrink-0" />
        <span>{{ t(isWindows ? "fileManager.sftpWindowsUnsupported" : "fileManager.sftpAuthenticationNotice") }}</span>
      </div>
    </div>
  </div>
</template>
