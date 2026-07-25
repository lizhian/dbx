<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref } from "vue";
import { useI18n } from "vue-i18n";
import { open, save } from "@tauri-apps/plugin-dialog";
import type { UnlistenFn } from "@tauri-apps/api/event";
import { AlertTriangle, CheckCircle2, ChevronDown, ChevronRight, Copy, Download, File, FilePenLine, Folder, FolderPlus, Loader2, Pencil, Plus, RefreshCcw, RotateCcw, Server, Trash2, Upload, X, XCircle } from "@lucide/vue";
import { Button } from "@/components/ui/button";
import { Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import PasswordInput from "@/components/ui/PasswordInput.vue";
import { useToast } from "@/composables/useToast";
import * as api from "@/lib/backend/api";
import type { FileConnection, FileConnectionInput, FileConnectionTestResult, FileEntryStat, FileManagerEntry, FileTransfer, FileTransferStatus } from "@/lib/backend/tauri";

const LIST_PAGE_SIZE = 200;

const { t } = useI18n();
const { toast } = useToast();
const text = computed(() => ({
  title: t("fileManager.title"),
  add: t("fileManager.add"),
  emptyConnections: t("fileManager.emptyConnections"),
  emptyDirectory: t("fileManager.emptyDirectory"),
  name: t("fileManager.name"),
  endpoint: t("fileManager.endpoint"),
  connectionType: t("fileManager.connectionType"),
  ftp: "FTP",
  s3: "S3",
  webdav: "WebDAV",
  region: t("fileManager.region"),
  bucket: t("fileManager.bucket"),
  accessKeyId: t("fileManager.accessKeyId"),
  secretAccessKey: t("fileManager.secretAccessKey"),
  sessionToken: t("fileManager.sessionToken"),
  virtualHostStyle: t("fileManager.virtualHostStyle"),
  anonymous: t("fileManager.anonymous"),
  clearS3Credentials: t("fileManager.clearS3Credentials"),
  root: t("fileManager.root"),
  username: t("fileManager.username"),
  password: t("fileManager.password"),
  keepPassword: t("fileManager.keepPassword"),
  clearPassword: t("fileManager.clearPassword"),
  ftpSecurity: t("fileManager.ftpSecurity"),
  s3Security: t("fileManager.s3Security"),
  webdavSecurity: t("fileManager.webdavSecurity"),
  authentication: t("fileManager.authentication"),
  authNone: t("fileManager.authNone"),
  authBasic: t("fileManager.authBasic"),
  authBearer: t("fileManager.authBearer"),
  bearerToken: t("fileManager.bearerToken"),
  clearWebdavCredentials: t("fileManager.clearWebdavCredentials"),
  test: t("fileManager.test"),
  save: t("common.save"),
  cancel: t("common.cancel"),
  edit: t("fileManager.edit"),
  delete: t("fileManager.delete"),
  deleteConfirm: t("fileManager.deleteConfirm"),
  createDirectory: t("fileManager.createDirectory"),
  directoryPath: t("fileManager.directoryPath"),
  directoryPathHint: t("fileManager.directoryPathHint"),
  deleteEntry: t("fileManager.deleteEntry"),
  deleteEntryConfirm: (name: string) => t("fileManager.deleteEntryConfirm", { name }),
  operationComplete: t("fileManager.operationComplete"),
  loadError: t("fileManager.loadError"),
  testSuccess: t("fileManager.testSuccess"),
  refresh: t("fileManager.refresh"),
  stage: {
    configuration: t("fileManager.stageConfiguration"),
    dns: "DNS",
    tcp: "TCP",
    authentication: t("fileManager.stageAuthentication"),
    bucket: t("fileManager.bucket"),
    root: t("fileManager.root"),
  },
  type: t("fileManager.type"),
  size: t("fileManager.size"),
  modified: t("fileManager.modified"),
  loadMore: t("fileManager.loadMore"),
  cursorExpired: t("fileManager.cursorExpired"),
  metadata: t("fileManager.metadata"),
  contentType: t("fileManager.contentType"),
  contentEncoding: t("fileManager.contentEncoding"),
  contentDisposition: t("fileManager.contentDisposition"),
  cacheControl: t("fileManager.cacheControl"),
  contentMd5: t("fileManager.contentMd5"),
  etag: "ETag",
  version: t("fileManager.version"),
  loadedCount: t("fileManager.loadedCount"),
  actions: t("fileManager.actions"),
  download: t("fileManager.download"),
  upload: t("fileManager.upload"),
  uploadRiskConfirm: (path: string) => t("fileManager.uploadRiskConfirm", { path }),
  s3UploadRiskConfirm: (path: string) => t("fileManager.s3UploadRiskConfirm", { path }),
  copy: t("fileManager.copy"),
  rename: t("fileManager.rename"),
  destinationPath: t("fileManager.destinationPath"),
  copyRenameRisk: t("fileManager.copyRenameRisk"),
  s3CopyRisk: t("fileManager.s3CopyRisk"),
  s3RenameRisk: t("fileManager.s3RenameRisk"),
  webdavCopyRisk: t("fileManager.webdavCopyRisk"),
  webdavRenameRisk: t("fileManager.webdavRenameRisk"),
  replaceDestination: t("fileManager.replaceDestination"),
  replaceConfirm: (path: string) => t("fileManager.replaceConfirm", { path }),
  retrySourceDelete: t("fileManager.retrySourceDelete"),
  operationOutcome: t("fileManager.operationOutcome"),
  transfers: t("fileManager.transfers"),
  noTransfers: t("fileManager.noTransfers"),
  cancelTransfer: t("fileManager.cancelTransfer"),
  transferStatus: {
    queued: t("fileManager.transferQueued"),
    running: t("fileManager.transferRunning"),
    cancelling: t("fileManager.transferCancelling"),
    publishing: t("fileManager.transferPublishing"),
    completed: t("fileManager.transferCompleted"),
    failed: t("fileManager.transferFailed"),
    cancelled: t("fileManager.transferCancelled"),
    partial: t("fileManager.transferPartial"),
  } satisfies Record<FileTransferStatus, string>,
  transferUploading: t("fileManager.transferUploading"),
  transferCopying: t("fileManager.transferCopying"),
  transferRenaming: t("fileManager.transferRenaming"),
  partialDestination: t("fileManager.partialDestination"),
  abortOutcome: t("fileManager.abortOutcome"),
  publishOutcome: t("fileManager.publishOutcome"),
}));

const connections = ref<FileConnection[]>([]);
const selectedId = ref<string | null>(null);
const entries = ref<FileManagerEntry[]>([]);
const currentPath = ref("");
const listCursor = ref<string | null>(null);
const selectedEntry = ref<FileManagerEntry | null>(null);
const entryStat = ref<FileEntryStat | null>(null);
const statError = ref<string | null>(null);
const transfers = ref<FileTransfer[]>([]);
const rootError = ref<string | null>(null);
const loadingConnections = ref(false);
const loadingEntries = ref(false);
const loadingMore = ref(false);
const loadingStat = ref(false);
const editorOpen = ref(false);
const deleteOpen = ref(false);
const createDirectoryOpen = ref(false);
const entryDeleteOpen = ref(false);
const remoteOperationOpen = ref(false);
const saving = ref(false);
const testing = ref(false);
const deleting = ref(false);
const mutating = ref(false);
const testResult = ref<FileConnectionTestResult | null>(null);
const editingId = ref<string | null>(null);
const pendingDeleteEntry = ref<FileManagerEntry | null>(null);
const pendingRemoteEntry = ref<FileManagerEntry | null>(null);
const remoteOperation = ref<"copy" | "rename">("copy");
const remoteDestinationPath = ref("");
const replaceDestination = ref(false);
const directoryPath = ref("");
const clearPassword = ref(false);
const clearS3Credentials = ref(false);
const clearWebdavCredentials = ref(false);
const form = ref({
  type: "ftp" as "ftp" | "s3" | "webdav",
  name: "",
  endpoint: "ftp://localhost:21",
  root: "/",
  username: "",
  password: "",
  region: "us-east-1",
  bucket: "",
  accessKeyId: "",
  secretAccessKey: "",
  sessionToken: "",
  virtualHostStyle: false,
  anonymous: false,
  webdavAuthentication: "basic" as "none" | "basic" | "bearer",
  webdavToken: "",
});
let connectionsGeneration = 0;
let rootGeneration = 0;
let statGeneration = 0;
let navigationGeneration = 0;
let unlistenTransfers: UnlistenFn | null = null;
let transferPoll: ReturnType<typeof setInterval> | null = null;

const selectedConnection = computed(() => connections.value.find((connection) => connection.id === selectedId.value));
const connectionSecurityText = computed(() => (form.value.type === "ftp" ? text.value.ftpSecurity : form.value.type === "s3" ? text.value.s3Security : text.value.webdavSecurity));
const canSubmit = computed(
  () =>
    !!form.value.name.trim() &&
    !!form.value.endpoint.trim() &&
    form.value.root.startsWith("/") &&
    (form.value.type !== "s3" || (!!form.value.region.trim() && !!form.value.bucket.trim())) &&
    (form.value.type !== "webdav" || form.value.webdavAuthentication !== "basic" || !!form.value.username.trim()),
);
const breadcrumbs = computed(() => {
  const result = [{ label: "/", path: "" }];
  const segments = currentPath.value.split("/").filter(Boolean);
  let path = "";
  for (const segment of segments) {
    path = path ? `${path}/${segment}` : segment;
    result.push({ label: segment, path });
  }
  return result;
});

function inputFromForm(): FileConnectionInput {
  if (form.value.type === "s3") {
    const hasCredentials = !!form.value.accessKeyId || !!form.value.secretAccessKey || !!form.value.sessionToken;
    return {
      id: editingId.value,
      expectedRevision: editingId.value ? selectedConnection.value?.revision : undefined,
      name: form.value.name.trim(),
      config: {
        type: "s3",
        endpoint: form.value.endpoint.trim(),
        region: form.value.region.trim(),
        bucket: form.value.bucket.trim(),
        root: form.value.root.trim(),
        virtualHostStyle: form.value.virtualHostStyle,
        anonymous: form.value.anonymous,
      },
      secrets: clearS3Credentials.value
        ? { clearS3Credentials: true }
        : hasCredentials
          ? {
              accessKeyId: form.value.accessKeyId,
              secretAccessKey: form.value.secretAccessKey,
              sessionToken: form.value.sessionToken || undefined,
            }
          : undefined,
    };
  }
  if (form.value.type === "webdav") {
    const hasCredential = (form.value.webdavAuthentication === "basic" && !!form.value.password) || (form.value.webdavAuthentication === "bearer" && !!form.value.webdavToken);
    return {
      id: editingId.value,
      expectedRevision: editingId.value ? selectedConnection.value?.revision : undefined,
      name: form.value.name.trim(),
      config: {
        type: "webdav",
        endpoint: form.value.endpoint.trim(),
        root: form.value.root.trim(),
        authentication: form.value.webdavAuthentication,
        username: form.value.webdavAuthentication === "basic" ? form.value.username.trim() : "",
      },
      secrets: clearWebdavCredentials.value || form.value.webdavAuthentication === "none" ? { clearWebdavCredentials: true } : hasCredential ? (form.value.webdavAuthentication === "basic" ? { password: form.value.password } : { webdavToken: form.value.webdavToken }) : undefined,
    };
  }
  return {
    id: editingId.value,
    expectedRevision: editingId.value ? selectedConnection.value?.revision : undefined,
    name: form.value.name.trim(),
    config: {
      type: "ftp",
      endpoint: form.value.endpoint.trim(),
      root: form.value.root.trim(),
      username: form.value.username.trim(),
    },
    secrets: clearPassword.value ? { password: null, clearPassword: true } : form.value.password ? { password: form.value.password } : undefined,
  };
}

function remoteOperationRisk(): string {
  if (selectedConnection.value?.config.type === "s3") {
    return remoteOperation.value === "copy" ? text.value.s3CopyRisk : text.value.s3RenameRisk;
  }
  if (selectedConnection.value?.config.type === "webdav") {
    return remoteOperation.value === "copy" ? text.value.webdavCopyRisk : text.value.webdavRenameRisk;
  }
  return text.value.copyRenameRisk;
}

async function loadConnections(preferredId?: string) {
  const generation = ++connectionsGeneration;
  const navigation = ++navigationGeneration;
  rootGeneration += 1;
  const closePromise = closeActiveCursor();
  loadingConnections.value = true;
  try {
    const [loaded] = await Promise.all([api.listFileConnections(), closePromise]);
    if (generation !== connectionsGeneration || navigation !== navigationGeneration) return;
    connections.value = loaded;
    const nextId = preferredId && connections.value.some((connection) => connection.id === preferredId) ? preferredId : selectedId.value;
    selectedId.value = nextId && connections.value.some((connection) => connection.id === nextId) ? nextId : (connections.value[0]?.id ?? null);
    currentPath.value = "";
    await loadDirectory();
  } catch (error) {
    if (generation === connectionsGeneration) toast(`${text.value.loadError}: ${String(error)}`, 5000);
  } finally {
    if (generation === connectionsGeneration) loadingConnections.value = false;
  }
}

async function closeActiveCursor() {
  const cursor = listCursor.value;
  const connectionId = selectedId.value;
  listCursor.value = null;
  if (!cursor || !connectionId) return;
  await closeCursor(connectionId, cursor);
}

async function closeCursor(connectionId: string, cursor: string) {
  try {
    await api.closeFileListCursor(connectionId, cursor);
  } catch {
    // Closing is best effort; a missing/expired cursor is already unusable.
  }
}

function clearSelection() {
  statGeneration += 1;
  selectedEntry.value = null;
  entryStat.value = null;
  statError.value = null;
  loadingStat.value = false;
}

async function loadDirectory() {
  const generation = ++rootGeneration;
  entries.value = [];
  clearSelection();
  rootError.value = null;
  if (!selectedId.value) {
    loadingEntries.value = false;
    return;
  }
  const connectionId = selectedId.value;
  const path = currentPath.value;
  loadingEntries.value = true;
  try {
    const page = await api.listFileEntries(connectionId, path, { pageSize: LIST_PAGE_SIZE });
    if (generation === rootGeneration && selectedId.value === connectionId && currentPath.value === path) {
      entries.value = page.entries;
      listCursor.value = page.cursor ?? null;
    } else if (page.cursor) {
      await closeCursor(connectionId, page.cursor);
    }
  } catch (error) {
    if (generation === rootGeneration) {
      rootError.value = String(error);
      toast(rootError.value, 5000);
    }
  } finally {
    if (generation === rootGeneration) loadingEntries.value = false;
  }
}

async function refreshDirectory() {
  const navigation = ++navigationGeneration;
  rootGeneration += 1;
  const closePromise = closeActiveCursor();
  await closePromise;
  if (navigation !== navigationGeneration) return;
  await loadDirectory();
}

async function loadMore() {
  const cursor = listCursor.value;
  const connectionId = selectedId.value;
  const path = currentPath.value;
  if (!cursor || !connectionId || loadingMore.value) return;
  loadingMore.value = true;
  try {
    const page = await api.listFileEntriesNext(connectionId, cursor, path, { pageSize: LIST_PAGE_SIZE });
    if (listCursor.value === cursor && selectedId.value === connectionId && currentPath.value === path) {
      entries.value.push(...page.entries);
      listCursor.value = page.cursor ?? null;
    } else if (page.cursor) {
      await closeCursor(connectionId, page.cursor);
    }
  } catch (error) {
    if (listCursor.value === cursor) {
      listCursor.value = null;
      const message = String(error);
      rootError.value = message.includes("CursorExpired") ? text.value.cursorExpired : message;
      toast(rootError.value, 5000);
    }
  } finally {
    loadingMore.value = false;
  }
}

async function selectConnection(id: string) {
  if (selectedId.value === id) return;
  const navigation = ++navigationGeneration;
  rootGeneration += 1;
  const closePromise = closeActiveCursor();
  selectedId.value = id;
  currentPath.value = "";
  await closePromise;
  if (navigation !== navigationGeneration) return;
  await loadDirectory();
}

async function openDirectory(path: string) {
  path = path.replace(/\/+$/, "");
  if (path === currentPath.value) return;
  const navigation = ++navigationGeneration;
  rootGeneration += 1;
  const closePromise = closeActiveCursor();
  currentPath.value = path;
  await closePromise;
  if (navigation !== navigationGeneration) return;
  await loadDirectory();
}

async function selectEntry(entry: FileManagerEntry) {
  selectedEntry.value = entry;
  entryStat.value = null;
  statError.value = null;
  const generation = ++statGeneration;
  const connectionId = selectedId.value;
  if (!connectionId || selectedConnection.value?.capabilities?.stat === false) return;
  loadingStat.value = true;
  try {
    const metadata = await api.statFileEntry(connectionId, entry.path);
    if (generation === statGeneration && selectedId.value === connectionId && selectedEntry.value?.path === entry.path) {
      entryStat.value = metadata;
    }
  } catch (error) {
    if (generation === statGeneration) statError.value = String(error);
  } finally {
    if (generation === statGeneration) loadingStat.value = false;
  }
}

function openCreate() {
  editingId.value = null;
  form.value = {
    type: "ftp",
    name: "",
    endpoint: "ftp://localhost:21",
    root: "/",
    username: "",
    password: "",
    region: "us-east-1",
    bucket: "",
    accessKeyId: "",
    secretAccessKey: "",
    sessionToken: "",
    virtualHostStyle: false,
    anonymous: false,
    webdavAuthentication: "basic",
    webdavToken: "",
  };
  testResult.value = null;
  clearPassword.value = false;
  clearS3Credentials.value = false;
  clearWebdavCredentials.value = false;
  editorOpen.value = true;
}

function openEdit() {
  const connection = selectedConnection.value;
  if (!connection) return;
  editingId.value = connection.id;
  form.value = {
    type: connection.config.type,
    name: connection.name,
    endpoint: connection.config.endpoint,
    root: connection.config.root,
    username: connection.config.type === "ftp" || connection.config.type === "webdav" ? connection.config.username : "",
    password: "",
    region: connection.config.type === "s3" ? connection.config.region : "us-east-1",
    bucket: connection.config.type === "s3" ? connection.config.bucket : "",
    accessKeyId: "",
    secretAccessKey: "",
    sessionToken: "",
    virtualHostStyle: connection.config.type === "s3" && connection.config.virtualHostStyle,
    anonymous: connection.config.type === "s3" && connection.config.anonymous,
    webdavAuthentication: connection.config.type === "webdav" ? connection.config.authentication : "basic",
    webdavToken: "",
  };
  testResult.value = null;
  clearPassword.value = false;
  clearS3Credentials.value = false;
  clearWebdavCredentials.value = false;
  editorOpen.value = true;
}

async function testConnection() {
  testing.value = true;
  testResult.value = null;
  try {
    testResult.value = await api.testFileConnection(inputFromForm());
    if (testResult.value.success) toast(text.value.testSuccess, 2000);
  } catch (error) {
    toast(String(error), 5000);
  } finally {
    testing.value = false;
  }
}

async function saveConnection() {
  if (!canSubmit.value) return;
  saving.value = true;
  try {
    const saved = await api.saveFileConnection(inputFromForm());
    editorOpen.value = false;
    await loadConnections(saved.id);
  } catch (error) {
    toast(String(error), 5000);
  } finally {
    saving.value = false;
  }
}

async function deleteConnection() {
  if (!selectedId.value || deleting.value) return;
  const deletedId = selectedId.value;
  deleting.value = true;
  try {
    await api.deleteFileConnection(deletedId);
    deleteOpen.value = false;
    selectedId.value = null;
    await loadConnections();
  } catch (error) {
    toast(String(error), 5000);
  } finally {
    deleting.value = false;
  }
}

function openCreateDirectory() {
  directoryPath.value = "";
  createDirectoryOpen.value = true;
}

async function createDirectory() {
  if (!selectedId.value || !directoryPath.value || mutating.value) return;
  mutating.value = true;
  try {
    await api.createFileDirectory(selectedId.value, directoryPath.value);
    createDirectoryOpen.value = false;
    toast(text.value.operationComplete, 2000);
    await refreshDirectory();
  } catch (error) {
    toast(String(error), 5000);
  } finally {
    mutating.value = false;
  }
}

function openDeleteEntry(entry: FileManagerEntry) {
  pendingDeleteEntry.value = entry;
  entryDeleteOpen.value = true;
}

async function deleteEntry() {
  const connectionId = selectedId.value;
  const entry = pendingDeleteEntry.value;
  if (!connectionId || !entry || mutating.value) return;
  mutating.value = true;
  try {
    await api.deleteFileEntry(connectionId, entry.path, false, entry.kind);
    entryDeleteOpen.value = false;
    pendingDeleteEntry.value = null;
    toast(text.value.operationComplete, 2000);
    clearSelection();
    await refreshDirectory();
  } catch (error) {
    toast(String(error), 5000);
  } finally {
    mutating.value = false;
  }
}

function formatSize(bytes: number): string {
  if (!bytes) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  const exponent = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
  return `${(bytes / 1024 ** exponent).toFixed(exponent ? 1 : 0)} ${units[exponent]}`;
}

function upsertTransfer(transfer: FileTransfer) {
  const index = transfers.value.findIndex((item) => item.id === transfer.id);
  const previous = index >= 0 ? transfers.value[index] : undefined;
  if (index >= 0) {
    transfers.value.splice(index, 1, transfer);
  } else {
    transfers.value.unshift(transfer);
  }
  transfers.value.sort((left, right) => right.createdAt.localeCompare(left.createdAt));
  transfers.value = transfers.value.slice(0, 100);
  if (["upload", "copy", "rename"].includes(transfer.direction) && transfer.status === "completed" && previous?.status !== "completed" && transfer.connectionId === selectedId.value) {
    void refreshDirectory();
  }
}

async function refreshTransfers() {
  try {
    const previous = new Map(transfers.value.map((transfer) => [transfer.id, transfer]));
    const refreshed = await api.listFileTransfers();
    transfers.value = refreshed;
    if (
      refreshed.some((transfer) => {
        const prior = previous.get(transfer.id);
        return ["upload", "copy", "rename"].includes(transfer.direction) && transfer.status === "completed" && prior !== undefined && prior.status !== "completed" && transfer.connectionId === selectedId.value;
      })
    ) {
      void refreshDirectory();
    }
  } catch (error) {
    toast(String(error), 5000);
  }
}

async function downloadEntry(entry: FileManagerEntry) {
  if (!selectedId.value || entry.kind !== "file") return;
  const localPath = await save({ defaultPath: entry.name });
  if (!localPath) return;
  try {
    const started = await api.startFileDownload({
      connectionId: selectedId.value,
      remotePath: entry.path,
      localPath,
    });
    upsertTransfer(await api.getFileTransfer(started.transferId));
  } catch (error) {
    toast(String(error), 5000);
  }
}

async function uploadFile() {
  const connectionId = selectedId.value;
  const targetDirectory = currentPath.value;
  if (!connectionId) return;
  const selected = await open({ multiple: false, directory: false });
  if (typeof selected !== "string") return;
  const name = localFileName(selected);
  const remotePath = targetDirectory ? `${targetDirectory}/${name}` : name;
  const uploadConfirmation = selectedConnection.value?.config.type === "s3" ? text.value.s3UploadRiskConfirm(remotePath) : text.value.uploadRiskConfirm(remotePath);
  if (!globalThis.confirm(uploadConfirmation)) return;
  try {
    const started = await api.startFileUpload({
      connectionId,
      localPath: selected,
      remotePath,
      policy: {
        mode: "best_effort_no_clobber",
        atomicNoClobber: false,
        externalToctouRisk: true,
      },
    });
    upsertTransfer(await api.getFileTransfer(started.transferId));
  } catch (error) {
    toast(String(error), 5000);
  }
}

function suggestedCopyPath(entry: FileManagerEntry): string {
  const slash = entry.path.lastIndexOf("/");
  const parent = slash >= 0 ? entry.path.slice(0, slash + 1) : "";
  const name = slash >= 0 ? entry.path.slice(slash + 1) : entry.path;
  const dot = name.lastIndexOf(".");
  return dot > 0 ? `${parent}${name.slice(0, dot)} copy${name.slice(dot)}` : `${parent}${name} copy`;
}

function openRemoteOperation(entry: FileManagerEntry, operation: "copy" | "rename") {
  if (entry.kind !== "file") return;
  pendingRemoteEntry.value = entry;
  remoteOperation.value = operation;
  remoteDestinationPath.value = operation === "copy" ? suggestedCopyPath(entry) : entry.path;
  replaceDestination.value = false;
  remoteOperationOpen.value = true;
}

async function startRemoteOperation() {
  const connectionId = selectedId.value;
  const entry = pendingRemoteEntry.value;
  const destinationPath = remoteDestinationPath.value.trim();
  if (!connectionId || !entry || !destinationPath || mutating.value) return;
  if (replaceDestination.value && !globalThis.confirm(text.value.replaceConfirm(destinationPath))) return;
  mutating.value = true;
  try {
    const input = {
      connectionId,
      sourcePath: entry.path,
      destinationPath,
      policy: replaceDestination.value ? ({ mode: "replace", confirmed: true } as const) : ({ mode: "best_effort_no_clobber", atomicNoClobber: false, externalToctouRisk: true } as const),
    };
    const started = remoteOperation.value === "copy" ? await api.startFileCopy(input) : await api.startFileRename(input);
    upsertTransfer(await api.getFileTransfer(started.transferId));
    remoteOperationOpen.value = false;
  } catch (error) {
    toast(String(error), 5000);
  } finally {
    mutating.value = false;
  }
}

async function retrySourceDelete(transfer: FileTransfer) {
  try {
    upsertTransfer(await api.retryFileRenameSourceDelete(transfer.id));
  } catch (error) {
    toast(String(error), 5000);
  }
}

async function cancelTransfer(transfer: FileTransfer) {
  if (!["queued", "running"].includes(transfer.status)) return;
  try {
    upsertTransfer(await api.cancelFileTransfer(transfer.id));
  } catch (error) {
    toast(String(error), 5000);
  }
}

function transferPercent(transfer: FileTransfer): number {
  if (!transfer.totalBytes) return transfer.status === "completed" ? 100 : 0;
  return Math.min(100, Math.round((transfer.bytesTransferred / transfer.totalBytes) * 100));
}

function transferStatusText(transfer: FileTransfer): string {
  if (transfer.direction === "upload" && transfer.status === "running") return text.value.transferUploading;
  if (transfer.direction === "copy" && transfer.status === "running") return text.value.transferCopying;
  if (transfer.direction === "rename" && transfer.status === "running") return text.value.transferRenaming;
  return text.value.transferStatus[transfer.status];
}

function localFileName(path: string): string {
  return path.split(/[\\/]/).pop() || path;
}

onMounted(async () => {
  try {
    unlistenTransfers = await api.listenFileTransferProgress(upsertTransfer);
  } catch (error) {
    toast(String(error), 5000);
  }
  await Promise.all([loadConnections(), refreshTransfers()]);
  transferPoll = setInterval(() => {
    if (transfers.value.some((transfer) => ["queued", "running", "cancelling", "publishing"].includes(transfer.status))) {
      void refreshTransfers();
    }
  }, 2_000);
});
onBeforeUnmount(() => {
  navigationGeneration += 1;
  rootGeneration += 1;
  statGeneration += 1;
  void closeActiveCursor();
  unlistenTransfers?.();
  if (transferPoll) clearInterval(transferPoll);
});
</script>

<template>
  <div class="flex h-full min-h-0 bg-background">
    <aside class="flex w-12 shrink-0 flex-col border-r bg-muted/10 sm:w-64">
      <div class="flex h-10 items-center justify-between border-b px-2 sm:px-3">
        <div class="flex min-w-0 items-center gap-2 text-sm font-medium">
          <Server class="hidden h-4 w-4 shrink-0 sm:block" />
          <span class="hidden truncate sm:inline">{{ text.title }}</span>
        </div>
        <Button size="icon" variant="ghost" class="h-7 w-7" :title="text.add" :aria-label="text.add" @click="openCreate">
          <Plus class="h-4 w-4" />
        </Button>
      </div>
      <div class="min-h-0 flex-1 overflow-auto p-1.5">
        <div v-if="loadingConnections" class="flex justify-center py-6"><Loader2 class="h-4 w-4 animate-spin text-muted-foreground" /></div>
        <button
          v-for="connection in connections"
          :key="connection.id"
          type="button"
          class="mb-0.5 flex w-full min-w-0 items-center justify-center gap-2 rounded px-2 py-2 text-left text-sm hover:bg-muted sm:justify-start"
          :class="selectedId === connection.id ? 'bg-accent text-accent-foreground' : ''"
          :title="connection.name"
          :aria-label="connection.name"
          @click="void selectConnection(connection.id)"
        >
          <Server class="h-4 w-4 shrink-0 text-muted-foreground" />
          <span class="hidden min-w-0 flex-1 truncate sm:block">{{ connection.name }}</span>
          <span class="hidden text-[10px] uppercase text-muted-foreground sm:inline">{{ connection.config.type }}</span>
        </button>
        <div v-if="!loadingConnections && !connections.length" class="hidden px-3 py-8 text-center text-xs text-muted-foreground sm:block">{{ text.emptyConnections }}</div>
      </div>
    </aside>

    <section class="flex min-w-0 flex-1 flex-col">
      <div class="flex h-10 items-center gap-1 border-b px-2">
        <span class="min-w-0 flex-1 truncate px-1 text-sm font-medium">{{ selectedConnection?.name ?? text.title }}</span>
        <Button v-if="selectedConnection?.capabilities?.write" variant="ghost" size="icon" class="h-7 w-7" :disabled="loadingEntries" :title="text.upload" :aria-label="text.upload" @click="void uploadFile()">
          <Upload class="h-3.5 w-3.5" />
        </Button>
        <Button v-if="selectedConnection?.capabilities?.createDirectory" variant="ghost" size="icon" class="h-7 w-7" :disabled="loadingEntries || mutating" :title="text.createDirectory" :aria-label="text.createDirectory" @click="openCreateDirectory">
          <FolderPlus class="h-3.5 w-3.5" />
        </Button>
        <Button variant="ghost" size="icon" class="h-7 w-7" :disabled="!selectedConnection || loadingEntries" :title="text.refresh" :aria-label="text.refresh" @click="void refreshDirectory()">
          <RefreshCcw class="h-3.5 w-3.5" :class="{ 'animate-spin': loadingEntries }" />
        </Button>
        <Button variant="ghost" size="icon" class="h-7 w-7" :disabled="!selectedConnection" :title="text.edit" :aria-label="text.edit" @click="openEdit">
          <Pencil class="h-3.5 w-3.5" />
        </Button>
        <Button variant="ghost" size="icon" class="h-7 w-7 text-destructive hover:text-destructive" :disabled="!selectedConnection || deleting" :title="text.delete" :aria-label="text.delete" @click="deleteOpen = true">
          <Trash2 class="h-3.5 w-3.5" />
        </Button>
      </div>

      <div v-if="selectedConnection" data-file-manager-breadcrumb class="flex h-9 shrink-0 items-center gap-0.5 overflow-x-auto border-b px-3 text-xs">
        <template v-for="(crumb, index) in breadcrumbs" :key="crumb.path">
          <ChevronRight v-if="index" class="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
          <button type="button" class="max-w-48 truncate px-1 py-1 hover:text-foreground" :class="crumb.path === currentPath ? 'font-medium text-foreground' : 'text-muted-foreground'" :title="crumb.path || '/'" @click="void openDirectory(crumb.path)">
            {{ crumb.label }}
          </button>
        </template>
        <span class="ml-auto shrink-0 pl-3 text-muted-foreground">{{ text.loadedCount.replace("{count}", String(entries.length)) }}</span>
      </div>

      <div class="flex min-h-0 flex-1">
        <div class="min-w-0 flex-1 overflow-auto">
          <table v-if="selectedConnection" class="w-full table-fixed text-sm">
            <thead class="sticky top-0 z-10 border-b bg-background text-left text-xs text-muted-foreground">
              <tr>
                <th class="w-auto px-3 py-2 font-medium sm:w-[48%]">{{ text.name }}</th>
                <th class="hidden w-24 px-3 py-2 font-medium md:table-cell">{{ text.type }}</th>
                <th class="hidden w-28 px-3 py-2 text-right font-medium sm:table-cell">{{ text.size }}</th>
                <th class="hidden w-48 px-3 py-2 font-medium lg:table-cell">{{ text.modified }}</th>
                <th class="w-36 px-3 py-2 text-right font-medium">{{ text.actions }}</th>
              </tr>
            </thead>
            <tbody>
              <tr
                v-for="entry in entries"
                :key="`${entry.kind}:${entry.path}`"
                tabindex="0"
                class="border-b border-border/50 outline-none hover:bg-muted/40 focus-visible:bg-muted/60"
                :class="selectedEntry?.path === entry.path ? 'bg-accent/70' : ''"
                @click="void selectEntry(entry)"
                @dblclick="entry.kind === 'directory' && void openDirectory(entry.path)"
                @keydown.enter="entry.kind === 'directory' ? void openDirectory(entry.path) : void selectEntry(entry)"
              >
                <td class="px-3 py-2">
                  <div class="flex min-w-0 items-center gap-2">
                    <Folder v-if="entry.kind === 'directory'" class="h-4 w-4 shrink-0 text-amber-500" />
                    <File v-else class="h-4 w-4 shrink-0 text-muted-foreground" />
                    <span class="truncate" :title="entry.path">{{ entry.name }}</span>
                  </div>
                </td>
                <td class="hidden px-3 py-2 text-xs text-muted-foreground md:table-cell">{{ entry.kind }}</td>
                <td class="hidden px-3 py-2 text-right font-mono text-xs text-muted-foreground sm:table-cell">{{ entry.kind === "file" ? formatSize(entry.size) : "" }}</td>
                <td class="hidden truncate px-3 py-2 text-xs text-muted-foreground lg:table-cell">{{ entry.lastModified ? new Date(entry.lastModified).toLocaleString() : "" }}</td>
                <td class="px-2 py-1 text-right">
                  <Button v-if="entry.kind === 'file' && selectedConnection?.capabilities?.read" size="icon" variant="ghost" class="h-7 w-7" :title="text.download" :aria-label="`${text.download}: ${entry.name}`" @click.stop="void downloadEntry(entry)">
                    <Download class="h-3.5 w-3.5" />
                  </Button>
                  <Button v-if="entry.kind === 'file' && selectedConnection?.capabilities?.copy" size="icon" variant="ghost" class="h-7 w-7" :title="text.copy" :aria-label="`${text.copy}: ${entry.name}`" @click.stop="openRemoteOperation(entry, 'copy')">
                    <Copy class="h-3.5 w-3.5" />
                  </Button>
                  <Button v-if="entry.kind === 'file' && selectedConnection?.capabilities?.rename" size="icon" variant="ghost" class="h-7 w-7" :title="text.rename" :aria-label="`${text.rename}: ${entry.name}`" @click.stop="openRemoteOperation(entry, 'rename')">
                    <FilePenLine class="h-3.5 w-3.5" />
                  </Button>
                  <Button v-if="selectedConnection?.capabilities?.delete" variant="ghost" size="icon" class="h-7 w-7 text-destructive hover:text-destructive" :disabled="mutating" :title="text.deleteEntry" :aria-label="`${text.deleteEntry}: ${entry.name}`" @click.stop="openDeleteEntry(entry)">
                    <Trash2 class="h-3.5 w-3.5" />
                  </Button>
                </td>
              </tr>
            </tbody>
          </table>
          <div v-if="loadingEntries" class="flex justify-center py-12"><Loader2 class="h-5 w-5 animate-spin text-muted-foreground" /></div>
          <div v-else-if="rootError" role="alert" class="mx-auto max-w-xl px-6 py-6 text-center text-sm text-destructive">{{ rootError }}</div>
          <div v-else-if="selectedConnection && !entries.length" class="py-12 text-center text-sm text-muted-foreground">{{ text.emptyDirectory }}</div>
          <div v-if="listCursor && !rootError" class="flex justify-center border-t px-3 py-3">
            <Button data-file-manager-load-more variant="outline" size="sm" :disabled="loadingMore" @click="void loadMore()">
              <Loader2 v-if="loadingMore" class="mr-1.5 h-3.5 w-3.5 animate-spin" />
              <ChevronDown v-else class="mr-1.5 h-3.5 w-3.5" />
              {{ text.loadMore }}
            </Button>
          </div>
        </div>

        <aside v-if="selectedEntry" data-file-manager-metadata class="hidden w-72 shrink-0 overflow-auto border-l bg-muted/5 md:block">
          <div class="flex h-9 items-center border-b px-3">
            <span class="min-w-0 flex-1 truncate text-xs font-medium">{{ text.metadata }}</span>
            <Button variant="ghost" size="icon" class="h-6 w-6" :aria-label="text.cancel" @click="clearSelection">
              <X class="h-3.5 w-3.5" />
            </Button>
          </div>
          <div v-if="loadingStat" class="flex justify-center py-8"><Loader2 class="h-4 w-4 animate-spin text-muted-foreground" /></div>
          <div v-else-if="statError" role="alert" class="break-words px-3 py-4 text-xs text-destructive">{{ statError }}</div>
          <dl v-else-if="entryStat" class="grid grid-cols-[6.5rem_minmax(0,1fr)] gap-x-2 gap-y-2 px-3 py-3 text-xs">
            <dt class="text-muted-foreground">{{ text.name }}</dt>
            <dd class="break-all">{{ entryStat.name }}</dd>
            <dt class="text-muted-foreground">{{ text.type }}</dt>
            <dd>{{ entryStat.kind }}</dd>
            <dt class="text-muted-foreground">{{ text.size }}</dt>
            <dd>{{ entryStat.kind === "file" ? formatSize(entryStat.size) : "" }}</dd>
            <dt class="text-muted-foreground">{{ text.modified }}</dt>
            <dd>{{ entryStat.lastModified ? new Date(entryStat.lastModified).toLocaleString() : "" }}</dd>
            <template v-if="entryStat.etag"
              ><dt class="text-muted-foreground">{{ text.etag }}</dt>
              <dd class="break-all font-mono">{{ entryStat.etag }}</dd></template
            >
            <template v-if="entryStat.version"
              ><dt class="text-muted-foreground">{{ text.version }}</dt>
              <dd class="break-all font-mono">{{ entryStat.version }}</dd></template
            >
            <template v-if="entryStat.contentType"
              ><dt class="text-muted-foreground">{{ text.contentType }}</dt>
              <dd class="break-all">{{ entryStat.contentType }}</dd></template
            >
            <template v-if="entryStat.contentEncoding"
              ><dt class="text-muted-foreground">{{ text.contentEncoding }}</dt>
              <dd class="break-all">{{ entryStat.contentEncoding }}</dd></template
            >
            <template v-if="entryStat.contentDisposition"
              ><dt class="text-muted-foreground">{{ text.contentDisposition }}</dt>
              <dd class="break-all">{{ entryStat.contentDisposition }}</dd></template
            >
            <template v-if="entryStat.cacheControl"
              ><dt class="text-muted-foreground">{{ text.cacheControl }}</dt>
              <dd class="break-all">{{ entryStat.cacheControl }}</dd></template
            >
            <template v-if="entryStat.contentMd5"
              ><dt class="text-muted-foreground">{{ text.contentMd5 }}</dt>
              <dd class="break-all font-mono">{{ entryStat.contentMd5 }}</dd></template
            >
            <template v-for="(value, key) in entryStat.userMetadata" :key="key"
              ><dt class="break-all text-muted-foreground">{{ key }}</dt>
              <dd class="break-all">{{ value }}</dd></template
            >
          </dl>
        </aside>
      </div>

      <section class="max-h-48 shrink-0 overflow-auto border-t bg-muted/10" :aria-label="text.transfers">
        <div class="sticky top-0 z-10 flex h-8 items-center border-b bg-background/95 px-3 text-xs font-medium">{{ text.transfers }}</div>
        <div v-if="!transfers.length" class="px-3 py-4 text-center text-xs text-muted-foreground">{{ text.noTransfers }}</div>
        <div v-for="transfer in transfers.slice(0, 8)" :key="transfer.id" class="grid min-h-12 grid-cols-[minmax(0,1fr)_7rem_2rem] items-center gap-3 border-b px-3 py-1.5">
          <div class="min-w-0">
            <div class="flex min-w-0 items-center gap-2 text-xs">
              <Upload v-if="transfer.direction === 'upload'" class="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
              <Copy v-else-if="transfer.direction === 'copy'" class="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
              <FilePenLine v-else-if="transfer.direction === 'rename'" class="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
              <Download v-else class="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
              <span class="truncate font-medium" :title="transfer.direction === 'copy' || transfer.direction === 'rename' ? `${transfer.remotePath} -> ${transfer.localPath}` : transfer.localPath">
                {{ transfer.direction === "copy" || transfer.direction === "rename" ? `${transfer.remotePath} -> ${transfer.localPath}` : localFileName(transfer.localPath) }}
              </span>
              <span class="shrink-0 text-muted-foreground">{{ transferStatusText(transfer) }}</span>
            </div>
            <div class="mt-1 h-1 overflow-hidden rounded-sm bg-muted">
              <div class="h-full bg-primary transition-[width] duration-200" :class="{ 'bg-destructive': ['failed', 'partial'].includes(transfer.status), 'bg-muted-foreground': transfer.status === 'cancelled' }" :style="{ width: `${transferPercent(transfer)}%` }" />
            </div>
            <div v-if="transfer.error" class="mt-0.5 truncate text-[10px] text-destructive" :title="transfer.error">{{ transfer.error }}</div>
            <div v-if="transfer.partialDestination" class="mt-0.5 truncate font-mono text-[10px] text-destructive" :title="transfer.partialDestination">{{ text.partialDestination }}: {{ transfer.partialDestination }}</div>
            <div v-if="transfer.abortOutcome" class="mt-0.5 truncate font-mono text-[10px] text-destructive" :title="transfer.abortOutcome">{{ text.abortOutcome }}: {{ transfer.abortOutcome }}</div>
            <div v-if="transfer.publishOutcome" class="mt-0.5 truncate font-mono text-[10px] text-destructive" :title="transfer.publishOutcome">{{ text.publishOutcome }}: {{ transfer.publishOutcome }}</div>
            <div v-if="transfer.operationOutcome" class="mt-0.5 truncate font-mono text-[10px]" :class="transfer.operationOutcome === 'completed' ? 'text-muted-foreground' : 'text-destructive'" :title="transfer.operationOutcome">{{ text.operationOutcome }}: {{ transfer.operationOutcome }}</div>
          </div>
          <div class="text-right font-mono text-[10px] text-muted-foreground">
            {{ formatSize(transfer.bytesTransferred) }}<span v-if="transfer.totalBytes"> / {{ formatSize(transfer.totalBytes) }}</span>
          </div>
          <Button v-if="['queued', 'running'].includes(transfer.status)" size="icon" variant="ghost" class="h-7 w-7" :title="text.cancelTransfer" :aria-label="text.cancelTransfer" @click="void cancelTransfer(transfer)">
            <X class="h-3.5 w-3.5" />
          </Button>
          <Button v-else-if="transfer.operationOutcome === 'copied_source_delete_failed' && transfer.operationPhase === 'delete_uncertain'" size="icon" variant="ghost" class="h-7 w-7" :title="text.retrySourceDelete" :aria-label="text.retrySourceDelete" @click="void retrySourceDelete(transfer)">
            <RotateCcw class="h-3.5 w-3.5" />
          </Button>
          <span v-else />
        </div>
      </section>
    </section>
  </div>

  <Dialog v-model:open="editorOpen">
    <DialogContent class="sm:max-w-lg">
      <DialogHeader
        ><DialogTitle>{{ editingId ? text.edit : text.add }}</DialogTitle></DialogHeader
      >
      <div class="rounded border border-amber-500/40 bg-amber-500/10 px-3 py-2 text-xs text-amber-800 dark:text-amber-200">
        <div class="flex gap-2">
          <AlertTriangle class="mt-0.5 h-4 w-4 shrink-0" />
          <span>{{ connectionSecurityText }}</span>
        </div>
      </div>
      <div class="grid gap-3 py-1">
        <div class="grid gap-1.5">
          <Label for="file-connection-type">{{ text.connectionType }}</Label>
          <select
            id="file-connection-type"
            v-model="form.type"
            class="h-9 rounded-md border border-input bg-background px-3 text-sm"
            @change="
              form.endpoint = form.type === 'ftp' ? 'ftp://localhost:21' : form.type === 's3' ? 'http://localhost:9000' : 'http://localhost:8080';
              testResult = null;
            "
          >
            <option value="ftp">{{ text.ftp }}</option>
            <option value="s3">{{ text.s3 }}</option>
            <option value="webdav">{{ text.webdav }}</option>
          </select>
        </div>
        <div class="grid gap-1.5">
          <Label for="file-connection-name">{{ text.name }}</Label
          ><Input id="file-connection-name" v-model="form.name" />
        </div>
        <div class="grid gap-1.5">
          <Label for="file-connection-endpoint">{{ text.endpoint }}</Label
          ><Input id="file-connection-endpoint" v-model="form.endpoint" :placeholder="form.type === 'ftp' ? 'ftp://host:21' : form.type === 's3' ? 'https://s3.example.com' : 'https://dav.example.com/webdav'" />
        </div>
        <div v-if="form.type === 'ftp'" class="grid grid-cols-2 gap-3">
          <div class="grid gap-1.5">
            <Label for="file-connection-root">{{ text.root }}</Label
            ><Input id="file-connection-root" v-model="form.root" placeholder="/" />
          </div>
          <div class="grid gap-1.5">
            <Label for="file-connection-username">{{ text.username }}</Label
            ><Input id="file-connection-username" v-model="form.username" />
          </div>
        </div>
        <div v-if="form.type === 'ftp'" class="grid gap-1.5">
          <Label for="file-connection-password">{{ text.password }}</Label>
          <PasswordInput id="file-connection-password" v-model="form.password" :disabled="clearPassword" :placeholder="editingId ? text.keepPassword : ''" />
          <label v-if="editingId && selectedConnection?.hasPassword" class="flex items-center gap-2 text-xs text-muted-foreground">
            <input v-model="clearPassword" type="checkbox" class="h-3.5 w-3.5 accent-primary" @change="clearPassword && (form.password = '')" />
            <span>{{ text.clearPassword }}</span>
          </label>
        </div>
        <template v-else-if="form.type === 's3'">
          <div class="grid grid-cols-2 gap-3">
            <div class="grid gap-1.5">
              <Label for="file-connection-region">{{ text.region }}</Label>
              <Input id="file-connection-region" v-model="form.region" placeholder="us-east-1" />
            </div>
            <div class="grid gap-1.5">
              <Label for="file-connection-bucket">{{ text.bucket }}</Label>
              <Input id="file-connection-bucket" v-model="form.bucket" />
            </div>
          </div>
          <div class="grid gap-1.5">
            <Label for="file-connection-s3-root">{{ text.root }}</Label>
            <Input id="file-connection-s3-root" v-model="form.root" placeholder="/" />
          </div>
          <label class="flex items-center gap-2 text-xs text-muted-foreground">
            <input v-model="form.virtualHostStyle" type="checkbox" class="h-3.5 w-3.5 accent-primary" />
            <span>{{ text.virtualHostStyle }}</span>
          </label>
          <label class="flex items-center gap-2 text-xs text-muted-foreground">
            <input v-model="form.anonymous" type="checkbox" class="h-3.5 w-3.5 accent-primary" />
            <span>{{ text.anonymous }}</span>
          </label>
          <div v-if="!form.anonymous" class="grid gap-3">
            <div class="grid gap-1.5">
              <Label for="file-connection-access-key">{{ text.accessKeyId }}</Label>
              <PasswordInput id="file-connection-access-key" v-model="form.accessKeyId" :disabled="clearS3Credentials" :placeholder="editingId ? text.keepPassword : ''" />
            </div>
            <div class="grid gap-1.5">
              <Label for="file-connection-secret-key">{{ text.secretAccessKey }}</Label>
              <PasswordInput id="file-connection-secret-key" v-model="form.secretAccessKey" :disabled="clearS3Credentials" :placeholder="editingId ? text.keepPassword : ''" />
            </div>
            <div class="grid gap-1.5">
              <Label for="file-connection-session-token">{{ text.sessionToken }}</Label>
              <PasswordInput id="file-connection-session-token" v-model="form.sessionToken" :disabled="clearS3Credentials" :placeholder="editingId ? text.keepPassword : ''" />
            </div>
            <label v-if="editingId && selectedConnection?.hasCredentials" class="flex items-center gap-2 text-xs text-muted-foreground">
              <input
                v-model="clearS3Credentials"
                type="checkbox"
                class="h-3.5 w-3.5 accent-primary"
                @change="
                  if (clearS3Credentials) {
                    form.accessKeyId = '';
                    form.secretAccessKey = '';
                    form.sessionToken = '';
                  }
                "
              />
              <span>{{ text.clearS3Credentials }}</span>
            </label>
          </div>
        </template>
        <template v-else>
          <div class="grid gap-1.5">
            <Label for="file-connection-webdav-root">{{ text.root }}</Label>
            <Input id="file-connection-webdav-root" v-model="form.root" placeholder="/" />
          </div>
          <div class="grid gap-1.5">
            <Label for="file-connection-webdav-auth">{{ text.authentication }}</Label>
            <select id="file-connection-webdav-auth" v-model="form.webdavAuthentication" class="h-9 rounded-md border border-input bg-background px-3 text-sm">
              <option value="none">{{ text.authNone }}</option>
              <option value="basic">{{ text.authBasic }}</option>
              <option value="bearer">{{ text.authBearer }}</option>
            </select>
          </div>
          <div v-if="form.webdavAuthentication === 'basic'" class="grid gap-3">
            <div class="grid gap-1.5">
              <Label for="file-connection-webdav-username">{{ text.username }}</Label>
              <Input id="file-connection-webdav-username" v-model="form.username" />
            </div>
            <div class="grid gap-1.5">
              <Label for="file-connection-webdav-password">{{ text.password }}</Label>
              <PasswordInput id="file-connection-webdav-password" v-model="form.password" :disabled="clearWebdavCredentials" :placeholder="editingId ? text.keepPassword : ''" />
            </div>
          </div>
          <div v-else-if="form.webdavAuthentication === 'bearer'" class="grid gap-1.5">
            <Label for="file-connection-webdav-token">{{ text.bearerToken }}</Label>
            <PasswordInput id="file-connection-webdav-token" v-model="form.webdavToken" :disabled="clearWebdavCredentials" :placeholder="editingId ? text.keepPassword : ''" />
          </div>
          <label v-if="editingId && selectedConnection?.hasCredentials && form.webdavAuthentication !== 'none'" class="flex items-center gap-2 text-xs text-muted-foreground">
            <input
              v-model="clearWebdavCredentials"
              type="checkbox"
              class="h-3.5 w-3.5 accent-primary"
              @change="
                if (clearWebdavCredentials) {
                  form.password = '';
                  form.webdavToken = '';
                }
              "
            />
            <span>{{ text.clearWebdavCredentials }}</span>
          </label>
        </template>
      </div>
      <div v-if="testResult" class="space-y-1 rounded border p-2">
        <div v-for="stage in testResult.stages" :key="stage.stage" class="flex min-w-0 items-start gap-2 text-xs">
          <CheckCircle2 v-if="stage.status === 'passed'" class="h-3.5 w-3.5 shrink-0 text-green-600" />
          <XCircle v-else-if="stage.status === 'failed'" class="h-3.5 w-3.5 shrink-0 text-destructive" />
          <span v-else class="h-3.5 w-3.5 shrink-0 rounded-full border" />
          <span class="w-24 shrink-0 font-medium">{{ text.stage[stage.stage] }}</span>
          <span class="min-w-0 break-words text-muted-foreground">{{ stage.message }}</span>
        </div>
      </div>
      <DialogFooter>
        <Button variant="outline" :disabled="saving || testing" @click="editorOpen = false">{{ text.cancel }}</Button>
        <Button variant="outline" :disabled="!canSubmit || saving || testing" @click="void testConnection()"> <Loader2 v-if="testing" class="mr-1.5 h-3.5 w-3.5 animate-spin" />{{ text.test }} </Button>
        <Button :disabled="!canSubmit || saving || testing" @click="void saveConnection()"> <Loader2 v-if="saving" class="mr-1.5 h-3.5 w-3.5 animate-spin" />{{ text.save }} </Button>
      </DialogFooter>
    </DialogContent>
  </Dialog>

  <Dialog v-model:open="createDirectoryOpen">
    <DialogContent class="sm:max-w-sm">
      <DialogHeader
        ><DialogTitle>{{ text.createDirectory }}</DialogTitle></DialogHeader
      >
      <div class="grid gap-1.5">
        <Label for="file-directory-path">{{ text.directoryPath }}</Label>
        <Input id="file-directory-path" v-model="directoryPath" :placeholder="text.directoryPathHint" @keydown.enter.prevent="void createDirectory()" />
        <p class="text-xs text-muted-foreground">{{ text.directoryPathHint }}</p>
      </div>
      <DialogFooter>
        <Button variant="outline" :disabled="mutating" @click="createDirectoryOpen = false">{{ text.cancel }}</Button>
        <Button :disabled="!directoryPath || mutating" @click="void createDirectory()">
          <Loader2 v-if="mutating" class="mr-1.5 h-3.5 w-3.5 animate-spin" />
          {{ text.createDirectory }}
        </Button>
      </DialogFooter>
    </DialogContent>
  </Dialog>

  <Dialog v-model:open="remoteOperationOpen">
    <DialogContent class="sm:max-w-md">
      <DialogHeader
        ><DialogTitle>{{ remoteOperation === "copy" ? text.copy : text.rename }}</DialogTitle></DialogHeader
      >
      <div class="grid gap-3">
        <div class="grid gap-1.5">
          <Label for="file-remote-destination">{{ text.destinationPath }}</Label>
          <Input id="file-remote-destination" v-model="remoteDestinationPath" @keydown.enter.prevent="void startRemoteOperation()" />
        </div>
        <div class="flex gap-2 rounded border border-amber-500/40 bg-amber-500/10 px-3 py-2 text-xs text-amber-800 dark:text-amber-200">
          <AlertTriangle class="mt-0.5 h-4 w-4 shrink-0" />
          <span>{{ remoteOperationRisk() }}</span>
        </div>
        <label class="flex items-center gap-2 text-sm">
          <input v-model="replaceDestination" type="checkbox" class="h-4 w-4 accent-primary" />
          <span>{{ text.replaceDestination }}</span>
        </label>
      </div>
      <DialogFooter>
        <Button variant="outline" :disabled="mutating" @click="remoteOperationOpen = false">{{ text.cancel }}</Button>
        <Button :disabled="!remoteDestinationPath.trim() || mutating" @click="void startRemoteOperation()">
          <Loader2 v-if="mutating" class="mr-1.5 h-3.5 w-3.5 animate-spin" />
          {{ remoteOperation === "copy" ? text.copy : text.rename }}
        </Button>
      </DialogFooter>
    </DialogContent>
  </Dialog>

  <Dialog v-model:open="entryDeleteOpen">
    <DialogContent class="sm:max-w-sm">
      <DialogHeader
        ><DialogTitle>{{ text.deleteEntry }}</DialogTitle></DialogHeader
      >
      <p class="break-words text-sm text-muted-foreground">{{ text.deleteEntryConfirm(pendingDeleteEntry?.name ?? "") }}</p>
      <DialogFooter>
        <Button variant="outline" :disabled="mutating" @click="entryDeleteOpen = false">{{ text.cancel }}</Button>
        <Button variant="destructive" :disabled="!pendingDeleteEntry || mutating" @click="void deleteEntry()">
          <Loader2 v-if="mutating" class="mr-1.5 h-3.5 w-3.5 animate-spin" />
          {{ text.deleteEntry }}
        </Button>
      </DialogFooter>
    </DialogContent>
  </Dialog>

  <Dialog v-model:open="deleteOpen">
    <DialogContent class="sm:max-w-sm">
      <DialogHeader
        ><DialogTitle>{{ text.delete }}</DialogTitle></DialogHeader
      >
      <p class="text-sm text-muted-foreground">{{ text.deleteConfirm }}</p>
      <DialogFooter>
        <Button variant="outline" :disabled="deleting" @click="deleteOpen = false">{{ text.cancel }}</Button>
        <Button variant="destructive" :disabled="deleting" @click="void deleteConnection()">
          <Loader2 v-if="deleting" class="mr-1.5 h-3.5 w-3.5 animate-spin" />
          {{ text.delete }}
        </Button>
      </DialogFooter>
    </DialogContent>
  </Dialog>
</template>
