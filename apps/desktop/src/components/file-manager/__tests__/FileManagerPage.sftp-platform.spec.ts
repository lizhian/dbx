// @vitest-environment happy-dom

import { createApp, defineComponent, h, nextTick, type Component } from "vue";
import { afterEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  closeFileListCursor: vi.fn().mockResolvedValue(undefined),
  listFileConnections: vi.fn().mockResolvedValue([]),
  listFileTransfers: vi.fn().mockResolvedValue([]),
  listenFileTransferProgress: vi.fn().mockResolvedValue(vi.fn()),
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
vi.mock("@tauri-apps/plugin-dialog", () => ({ open: vi.fn(), save: vi.fn() }));
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
vi.mock("@/composables/useToast", () => ({ useToast: () => ({ toast: vi.fn() }) }));
vi.mock("@/lib/backend/platform", () => ({ getPlatform: () => "windows" }));
vi.mock("@/lib/backend/api", () => ({
  cancelFileTransfer: vi.fn(),
  closeFileListCursor: mocks.closeFileListCursor,
  createFileDirectory: vi.fn(),
  deleteFileConnection: vi.fn(),
  deleteFileEntry: vi.fn(),
  getFileTransfer: vi.fn(),
  listFileConnections: mocks.listFileConnections,
  listFileEntries: vi.fn(),
  listFileEntriesNext: vi.fn(),
  listFileTransfers: mocks.listFileTransfers,
  listenFileTransferProgress: mocks.listenFileTransferProgress,
  retryFileRenameSourceDelete: vi.fn(),
  saveFileConnection: vi.fn(),
  startFileCopy: vi.fn(),
  startFileDownload: vi.fn(),
  startFileRename: vi.fn(),
  startFileUpload: vi.fn(),
  statFileEntry: vi.fn(),
  testFileConnection: vi.fn(),
}));

import FileManagerPage from "@/components/file-manager/FileManagerPage.vue";

afterEach(() => {
  document.body.replaceChildren();
  vi.clearAllMocks();
});

describe("FileManagerPage SFTP platform boundary", () => {
  it("disables SFTP on Windows while leaving other connection types available", async () => {
    const root = document.createElement("div");
    document.body.append(root);
    const app = createApp(FileManagerPage);
    app.mount(root);
    await vi.waitFor(() => expect(mocks.listFileConnections).toHaveBeenCalledOnce());

    root.querySelector<HTMLButtonElement>('button[aria-label="fileManager.add"]')?.click();
    await nextTick();
    const type = root.querySelector<HTMLSelectElement>("#file-connection-type");
    const sftp = type?.querySelector<HTMLOptionElement>('option[value="sftp"]');
    expect(sftp?.disabled).toBe(true);
    expect(sftp?.textContent).toContain("fileManager.sftpUnsupported");
    expect(type?.querySelector<HTMLOptionElement>('option[value="ftp"]')?.disabled).toBe(false);
    expect(type?.querySelector<HTMLOptionElement>('option[value="s3"]')?.disabled).toBe(false);
    expect(type?.querySelector<HTMLOptionElement>('option[value="webdav"]')?.disabled).toBe(false);

    app.unmount();
  });
});
