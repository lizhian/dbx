// @vitest-environment happy-dom

import { createApp, defineComponent, h, nextTick, type Component } from "vue";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { FileConnection, FileManagerEntry, FileTransfer } from "@/lib/backend/tauri";

const mocks = vi.hoisted(() => ({
  cancelFileTransfer: vi.fn(),
  confirmUploadRisk: vi.fn(),
  closeFileListCursor: vi.fn(),
  createFileDirectory: vi.fn(),
  deleteFileConnection: vi.fn(),
  deleteFileEntry: vi.fn(),
  getFileTransfer: vi.fn(),
  listFileConnections: vi.fn(),
  listFileEntries: vi.fn(),
  listFileEntriesNext: vi.fn(),
  listFileTransfers: vi.fn(),
  listenFileTransferProgress: vi.fn(),
  openDialog: vi.fn(),
  saveDialog: vi.fn(),
  saveFileConnection: vi.fn(),
  startFileDownload: vi.fn(),
  startFileCopy: vi.fn(),
  startFileRename: vi.fn(),
  startFileUpload: vi.fn(),
  retryFileRenameSourceDelete: vi.fn(),
  statFileEntry: vi.fn(),
  testFileConnection: vi.fn(),
  toast: vi.fn(),
  unlisten: vi.fn(),
  progressListener: null as null | ((transfer: FileTransfer) => void),
}));

function passthrough(tag: string): Component {
  return defineComponent({
    inheritAttrs: false,
    setup(_, { attrs, slots }) {
      return () => h(tag, attrs, slots.default?.());
    },
  });
}

vi.mock("vue-i18n", () => ({ useI18n: () => ({ t: (key: string) => key }) }));
vi.mock("@tauri-apps/plugin-dialog", () => ({ open: mocks.openDialog, save: mocks.saveDialog }));
vi.mock("@lucide/vue", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@lucide/vue")>();
  const Icon = passthrough("span");
  return {
    ...actual,
    AlertTriangle: Icon,
    CheckCircle2: Icon,
    Copy: Icon,
    Download: Icon,
    File: Icon,
    FilePenLine: Icon,
    Folder: Icon,
    Loader2: Icon,
    Pencil: Icon,
    Plus: Icon,
    RefreshCcw: Icon,
    RotateCcw: Icon,
    Server: Icon,
    Trash2: Icon,
    X: Icon,
    XCircle: Icon,
  };
});
vi.mock("@/components/ui/button", () => ({ Button: passthrough("button") }));
vi.mock("@/components/ui/dialog", () => ({
  Dialog: passthrough("div"),
  DialogContent: passthrough("div"),
  DialogFooter: passthrough("div"),
  DialogHeader: passthrough("div"),
  DialogTitle: passthrough("div"),
}));
vi.mock("@/components/ui/input", () => ({ Input: passthrough("input") }));
vi.mock("@/components/ui/label", () => ({ Label: passthrough("label") }));
vi.mock("@/components/ui/PasswordInput.vue", () => ({ default: passthrough("input") }));
vi.mock("@/composables/useToast", () => ({ useToast: () => ({ toast: mocks.toast }) }));
vi.mock("@/lib/backend/api", () => ({
  cancelFileTransfer: mocks.cancelFileTransfer,
  closeFileListCursor: mocks.closeFileListCursor,
  createFileDirectory: mocks.createFileDirectory,
  deleteFileConnection: mocks.deleteFileConnection,
  deleteFileEntry: mocks.deleteFileEntry,
  getFileTransfer: mocks.getFileTransfer,
  listFileConnections: mocks.listFileConnections,
  listFileEntries: mocks.listFileEntries,
  listFileEntriesNext: mocks.listFileEntriesNext,
  listFileTransfers: mocks.listFileTransfers,
  listenFileTransferProgress: mocks.listenFileTransferProgress,
  saveFileConnection: mocks.saveFileConnection,
  startFileDownload: mocks.startFileDownload,
  startFileCopy: mocks.startFileCopy,
  startFileRename: mocks.startFileRename,
  startFileUpload: mocks.startFileUpload,
  retryFileRenameSourceDelete: mocks.retryFileRenameSourceDelete,
  statFileEntry: mocks.statFileEntry,
  testFileConnection: mocks.testFileConnection,
}));

import FileManagerPage from "@/components/file-manager/FileManagerPage.vue";

const connection: FileConnection = {
  id: "ftp-1",
  name: "Files",
  revision: 1,
  config: {
    type: "ftp",
    endpoint: "ftp://localhost:21",
    root: "/",
    username: "tester",
  },
  hasPassword: true,
  createdAt: "2026-07-25T08:00:00Z",
  updatedAt: "2026-07-25T08:00:00Z",
};

const secondConnection: FileConnection = {
  ...connection,
  id: "ftp-2",
  name: "Other files",
  config: {
    ...connection.config,
    endpoint: "ftp://other.example:21",
  },
};

const remoteEntry: FileManagerEntry = {
  path: "a%2Fb",
  name: "remote.bin",
  kind: "file",
  size: 100,
  lastModified: null,
};

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  const promise = new Promise<T>((resolvePromise) => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

function transfer(id: string, status: FileTransfer["status"], overrides: Partial<FileTransfer> = {}): FileTransfer {
  return {
    id,
    connectionId: connection.id,
    direction: "download",
    remotePath: `${id}.bin`,
    localPath: `/tmp/${id}.bin`,
    status,
    bytesTransferred: 0,
    totalBytes: 100,
    error: null,
    createdAt: `2026-07-25T08:00:0${id.length}Z`,
    updatedAt: "2026-07-25T08:00:00Z",
    completedAt: null,
    ...overrides,
  };
}

let app: ReturnType<typeof createApp> | undefined;
let root: HTMLDivElement | undefined;

async function mountPage() {
  root = document.createElement("div");
  document.body.append(root);
  app = createApp(FileManagerPage);
  app.mount(root);
  await vi.waitFor(() => {
    expect(mocks.listFileConnections).toHaveBeenCalledOnce();
    expect(mocks.listFileEntries).toHaveBeenCalledOnce();
    expect(mocks.listFileTransfers).toHaveBeenCalledOnce();
  });
  await nextTick();
}

beforeEach(() => {
  vi.stubGlobal("confirm", mocks.confirmUploadRisk);
  mocks.confirmUploadRisk.mockReturnValue(true);
  mocks.progressListener = null;
  mocks.closeFileListCursor.mockResolvedValue(undefined);
  mocks.listFileConnections.mockResolvedValue([connection]);
  mocks.listFileEntries.mockResolvedValue({ entries: [remoteEntry], cursor: null });
  mocks.listFileTransfers.mockResolvedValue([]);
  mocks.listenFileTransferProgress.mockImplementation(async (listener: (value: FileTransfer) => void) => {
    mocks.progressListener = listener;
    return mocks.unlisten;
  });
});

afterEach(() => {
  app?.unmount();
  root?.remove();
  app = undefined;
  root = undefined;
  vi.useRealTimers();
  vi.unstubAllGlobals();
  vi.clearAllMocks();
});

describe("FileManagerPage transfer lifecycle", () => {
  it("hydrates terminal and active transfers, applies progress, and exposes cancellation", async () => {
    const completed = transfer("completed", "completed", {
      bytesTransferred: 100,
      completedAt: "2026-07-25T08:01:00Z",
    });
    const failed = transfer("failed", "failed", {
      bytesTransferred: 25,
      error: "remote disconnected",
      completedAt: "2026-07-25T08:02:00Z",
    });
    const running = transfer("running", "running", { bytesTransferred: 10 });
    mocks.listFileTransfers.mockResolvedValueOnce([completed, failed, running]);
    mocks.cancelFileTransfer.mockResolvedValue(transfer("running", "cancelling", { bytesTransferred: 60 }));

    await mountPage();

    expect(root?.textContent).toContain("completed.bin");
    expect(root?.textContent).toContain("fileManager.transferCompleted");
    expect(root?.textContent).toContain("failed.bin");
    expect(root?.textContent).toContain("fileManager.transferFailed");
    expect(root?.textContent).toContain("remote disconnected");
    expect(root?.textContent).toContain("running.bin");
    expect(root?.textContent).toContain("fileManager.transferRunning");

    mocks.progressListener?.(transfer("running", "running", { bytesTransferred: 60 }));
    await nextTick();

    const runningRow = Array.from(root?.querySelectorAll(".grid") ?? []).find((element) => element.textContent?.includes("running.bin"));
    expect(runningRow?.textContent).toContain("60 B / 100 B");
    expect(runningRow?.querySelector<HTMLElement>('[style*="width: 60%"]')).toBeTruthy();

    const cancel = runningRow?.querySelector<HTMLButtonElement>('button[aria-label="fileManager.cancelTransfer"]');
    cancel?.click();
    await vi.waitFor(() => expect(mocks.cancelFileTransfer).toHaveBeenCalledWith("running"));
    await nextTick();

    expect(runningRow?.textContent).toContain("fileManager.transferCancelling");
    expect(runningRow?.querySelector('button[aria-label="fileManager.cancelTransfer"]')).toBeNull();
  });

  it("supplements a start response with getFileTransfer and preserves the raw remote path", async () => {
    const started = transfer("started", "running", {
      remotePath: remoteEntry.path,
      localPath: "/tmp/remote.bin",
      bytesTransferred: 5,
    });
    mocks.saveDialog.mockResolvedValue("/tmp/remote.bin");
    mocks.startFileDownload.mockResolvedValue({ transferId: started.id });
    mocks.getFileTransfer.mockResolvedValue(started);

    await mountPage();

    root?.querySelector<HTMLButtonElement>('button[aria-label="fileManager.download: remote.bin"]')?.click();

    await vi.waitFor(() => expect(mocks.getFileTransfer).toHaveBeenCalledWith(started.id));
    expect(mocks.startFileDownload).toHaveBeenCalledWith({
      connectionId: connection.id,
      remotePath: "a%2Fb",
      localPath: "/tmp/remote.bin",
    });
    expect(root?.textContent).toContain("fileManager.transferRunning");
    expect(root?.textContent).toContain("5 B / 100 B");
  });

  it("recovers a missed terminal event through the transfer query poll", async () => {
    vi.useFakeTimers();
    const running = transfer("missed", "running", {
      direction: "upload",
      bytesTransferred: 40,
    });
    const completed = transfer("missed", "completed", {
      direction: "upload",
      bytesTransferred: 100,
      completedAt: "2026-07-25T08:03:00Z",
    });
    mocks.listFileTransfers.mockResolvedValueOnce([running]).mockResolvedValueOnce([completed]);

    await mountPage();
    expect(root?.textContent).toContain("fileManager.transferUploading");
    expect(mocks.progressListener).toBeTypeOf("function");

    await vi.advanceTimersByTimeAsync(2_000);
    await vi.waitFor(() => expect(mocks.listFileTransfers).toHaveBeenCalledTimes(2));
    await nextTick();

    expect(root?.textContent).toContain("fileManager.transferCompleted");
    expect(root?.textContent).toContain("100 B / 100 B");
    await vi.waitFor(() => expect(mocks.listFileEntries).toHaveBeenCalledTimes(2));

    await vi.advanceTimersByTimeAsync(4_000);
    expect(mocks.listFileTransfers).toHaveBeenCalledTimes(2);
  });

  it("binds a deferred upload dialog to the connection and directory where it was opened", async () => {
    const uploadSelection = deferred<string | null>();
    const directory: FileManagerEntry = {
      path: "captured",
      name: "captured",
      kind: "directory",
      size: 0,
      lastModified: null,
    };
    const running = transfer("captured-upload", "running", {
      direction: "upload",
      connectionId: connection.id,
      remotePath: "captured/local.bin",
      localPath: "/tmp/local.bin",
    });
    mocks.listFileConnections.mockResolvedValue([connection, secondConnection]);
    mocks.listFileEntries.mockResolvedValueOnce({ entries: [directory], cursor: null }).mockResolvedValue({ entries: [], cursor: null });
    mocks.openDialog.mockReturnValue(uploadSelection.promise);
    mocks.startFileUpload.mockResolvedValue({ transferId: running.id });
    mocks.getFileTransfer.mockResolvedValue(running);

    await mountPage();

    const directoryRow = Array.from(root?.querySelectorAll("tbody tr") ?? []).find((element) => element.textContent?.includes(directory.name));
    directoryRow?.dispatchEvent(new MouseEvent("dblclick", { bubbles: true }));
    await vi.waitFor(() =>
      expect(mocks.listFileEntries).toHaveBeenNthCalledWith(2, connection.id, directory.path, {
        pageSize: 200,
      }),
    );

    root?.querySelector<HTMLButtonElement>('button[aria-label="fileManager.upload"]')?.click();
    await vi.waitFor(() => expect(mocks.openDialog).toHaveBeenCalledOnce());

    root?.querySelector<HTMLButtonElement>(`button[aria-label="${secondConnection.name}"]`)?.click();
    await vi.waitFor(() =>
      expect(mocks.listFileEntries).toHaveBeenNthCalledWith(3, secondConnection.id, "", {
        pageSize: 200,
      }),
    );

    uploadSelection.resolve("/tmp/local.bin");
    await vi.waitFor(() =>
      expect(mocks.startFileUpload).toHaveBeenCalledWith({
        connectionId: connection.id,
        localPath: "/tmp/local.bin",
        remotePath: "captured/local.bin",
        policy: {
          mode: "best_effort_no_clobber",
          atomicNoClobber: false,
          externalToctouRisk: true,
        },
      }),
    );
  });

  it("starts an upload into the current directory, renders partial state, and refreshes after completion", async () => {
    const running = transfer("upload", "running", {
      direction: "upload",
      remotePath: "local.bin",
      localPath: "/tmp/local.bin",
      bytesTransferred: 10,
    });
    mocks.openDialog.mockResolvedValue("/tmp/local.bin");
    mocks.startFileUpload.mockResolvedValue({ transferId: running.id });
    mocks.getFileTransfer.mockResolvedValue(running);

    await mountPage();
    root?.querySelector<HTMLButtonElement>('button[aria-label="fileManager.upload"]')?.click();

    await vi.waitFor(() => expect(mocks.startFileUpload).toHaveBeenCalledOnce());
    expect(mocks.startFileUpload).toHaveBeenCalledWith({
      connectionId: connection.id,
      localPath: "/tmp/local.bin",
      remotePath: "local.bin",
      policy: {
        mode: "best_effort_no_clobber",
        atomicNoClobber: false,
        externalToctouRisk: true,
      },
    });
    expect(mocks.confirmUploadRisk).toHaveBeenCalledOnce();
    expect(root?.textContent).toContain("fileManager.transferUploading");

    mocks.progressListener?.(
      transfer("upload", "partial", {
        direction: "upload",
        remotePath: "local.bin",
        localPath: "/tmp/local.bin",
        bytesTransferred: 40,
        partialDestination: ".dbx-upload-upload-random.part",
        abortOutcome: "unsupported",
        publishOutcome: "partial_source",
      }),
    );
    await nextTick();
    expect(root?.textContent).toContain("fileManager.transferPartial");
    expect(root?.textContent).toContain(".dbx-upload-upload-random.part");
    expect(root?.textContent).toContain("unsupported");
    expect(root?.textContent).toContain("partial_source");

    mocks.progressListener?.(
      transfer("upload", "completed", {
        direction: "upload",
        remotePath: "local.bin",
        localPath: "/tmp/local.bin",
        bytesTransferred: 100,
      }),
    );
    await vi.waitFor(() => expect(mocks.listFileEntries).toHaveBeenCalledTimes(2));
  });

  it("does not start an upload when the FTP no-clobber risk is declined", async () => {
    mocks.openDialog.mockResolvedValue("/tmp/declined.bin");
    mocks.confirmUploadRisk.mockReturnValue(false);

    await mountPage();
    root?.querySelector<HTMLButtonElement>('button[aria-label="fileManager.upload"]')?.click();

    await vi.waitFor(() => expect(mocks.confirmUploadRisk).toHaveBeenCalledOnce());
    expect(mocks.startFileUpload).not.toHaveBeenCalled();
  });

  it("starts same-connection copy with the explicit best-effort policy", async () => {
    const running = transfer("copy", "running", {
      direction: "copy",
      remotePath: remoteEntry.path,
      localPath: "copied.bin",
    });
    mocks.startFileCopy.mockResolvedValue({ transferId: running.id });
    mocks.getFileTransfer.mockResolvedValue(running);

    await mountPage();
    root?.querySelector<HTMLButtonElement>('button[aria-label="fileManager.copy: remote.bin"]')?.click();
    await nextTick();

    const submit = Array.from(root?.querySelectorAll<HTMLButtonElement>("button") ?? [])
      .filter((button) => button.textContent?.trim() === "fileManager.copy")
      .at(-1);
    submit?.click();

    await vi.waitFor(() =>
      expect(mocks.startFileCopy).toHaveBeenCalledWith({
        connectionId: connection.id,
        sourcePath: remoteEntry.path,
        destinationPath: "a%2Fb copy",
        policy: {
          mode: "best_effort_no_clobber",
          atomicNoClobber: false,
          externalToctouRisk: true,
        },
      }),
    );
    await vi.waitFor(() => expect(root?.textContent).toContain("fileManager.transferCopying"));
  });

  it("requires a second explicit confirmation before replace", async () => {
    mocks.confirmUploadRisk.mockReturnValue(false);

    await mountPage();
    root?.querySelector<HTMLButtonElement>('button[aria-label="fileManager.rename: remote.bin"]')?.click();
    await nextTick();

    root?.querySelector<HTMLInputElement>('input[type="checkbox"]')?.click();
    const submit = Array.from(root?.querySelectorAll<HTMLButtonElement>("button") ?? [])
      .filter((button) => button.textContent?.trim() === "fileManager.rename")
      .at(-1);
    submit?.click();

    await vi.waitFor(() => expect(mocks.confirmUploadRisk).toHaveBeenCalledOnce());
    expect(mocks.startFileRename).not.toHaveBeenCalled();
  });

  it("shows copied_source_delete_failed and exposes fingerprint-checked recovery", async () => {
    const partial = transfer("rename-partial", "partial", {
      direction: "rename",
      remotePath: "source.bin",
      localPath: "destination.bin",
      operationOutcome: "copied_source_delete_failed",
      operationPhase: "delete_uncertain",
      partialDestination: "destination.bin",
      error: "source delete response was not observed",
    });
    const completed = {
      ...partial,
      status: "completed" as const,
      operationOutcome: "completed" as const,
      operationPhase: "completed" as const,
      partialDestination: null,
      error: null,
    };
    mocks.listFileTransfers.mockResolvedValue([partial]);
    mocks.retryFileRenameSourceDelete.mockResolvedValue(completed);

    await mountPage();

    expect(root?.textContent).toContain("copied_source_delete_failed");
    expect(root?.textContent).toContain("source.bin -> destination.bin");
    root?.querySelector<HTMLButtonElement>('button[aria-label="fileManager.retrySourceDelete"]')?.click();

    await vi.waitFor(() => expect(mocks.retryFileRenameSourceDelete).toHaveBeenCalledWith(partial.id));
    await vi.waitFor(() => expect(root?.textContent).toContain("fileManager.transferCompleted"));
    expect(root?.textContent).toContain("fileManager.operationOutcome: completed");
  });

  it("does not offer source deletion retry before delete_uncertain", async () => {
    mocks.listFileTransfers.mockResolvedValue([
      transfer("rename-published", "partial", {
        direction: "rename",
        remotePath: "source.bin",
        localPath: "destination.bin",
        operationOutcome: "copied_source_delete_failed",
        operationPhase: "published_before_delete",
        partialDestination: "destination.bin",
      }),
    ]);

    await mountPage();

    expect(root?.textContent).toContain("source.bin -> destination.bin");
    expect(root?.querySelector('button[aria-label="fileManager.retrySourceDelete"]')).toBeNull();
  });
});
