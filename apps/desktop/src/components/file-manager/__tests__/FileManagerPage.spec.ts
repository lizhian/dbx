// @vitest-environment happy-dom

import { createApp, defineComponent, h, nextTick, type Component } from "vue";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { FileConnection, FileManagerEntry, FileTransfer } from "@/lib/backend/tauri";

const mocks = vi.hoisted(() => ({
  cancelFileTransfer: vi.fn(),
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
  saveDialog: vi.fn(),
  saveFileConnection: vi.fn(),
  startFileDownload: vi.fn(),
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
vi.mock("@tauri-apps/plugin-dialog", () => ({ save: mocks.saveDialog }));
vi.mock("@lucide/vue", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@lucide/vue")>();
  const Icon = passthrough("span");
  return {
    ...actual,
    AlertTriangle: Icon,
    CheckCircle2: Icon,
    Download: Icon,
    File: Icon,
    Folder: Icon,
    Loader2: Icon,
    Pencil: Icon,
    Plus: Icon,
    RefreshCcw: Icon,
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

const remoteEntry: FileManagerEntry = {
  path: "a%2Fb",
  name: "remote.bin",
  kind: "file",
  size: 100,
  lastModified: null,
};

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
    const running = transfer("missed", "running", { bytesTransferred: 40 });
    const completed = transfer("missed", "completed", {
      bytesTransferred: 100,
      completedAt: "2026-07-25T08:03:00Z",
    });
    mocks.listFileTransfers.mockResolvedValueOnce([running]).mockResolvedValueOnce([completed]);

    await mountPage();
    expect(root?.textContent).toContain("fileManager.transferRunning");
    expect(mocks.progressListener).toBeTypeOf("function");

    await vi.advanceTimersByTimeAsync(2_000);
    await vi.waitFor(() => expect(mocks.listFileTransfers).toHaveBeenCalledTimes(2));
    await nextTick();

    expect(root?.textContent).toContain("fileManager.transferCompleted");
    expect(root?.textContent).toContain("100 B / 100 B");

    await vi.advanceTimersByTimeAsync(4_000);
    expect(mocks.listFileTransfers).toHaveBeenCalledTimes(2);
  });
});
