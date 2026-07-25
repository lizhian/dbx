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

function modelInput(): Component {
  return defineComponent({
    inheritAttrs: false,
    props: { modelValue: { type: [String, Number], default: "" } },
    emits: ["update:modelValue"],
    setup(props, { attrs, emit }) {
      return () =>
        h("input", {
          ...attrs,
          value: props.modelValue,
          onInput: (event: Event) => emit("update:modelValue", (event.target as HTMLInputElement).value),
        });
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
vi.mock("@/components/ui/input", () => ({ Input: modelInput() }));
vi.mock("@/components/ui/label", () => ({ Label: passthrough("label") }));
vi.mock("@/components/ui/PasswordInput.vue", () => ({ default: modelInput() }));
vi.mock("@/composables/useToast", () => ({ useToast: () => ({ toast: mocks.toast }) }));
vi.mock("@/lib/backend/platform", () => ({ getPlatform: () => "linux" }));
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
  hasCredentials: true,
  capabilities: {
    read: true,
    write: true,
    stat: true,
    list: true,
    createDirectory: true,
    delete: true,
    copy: true,
    rename: true,
    serverSideCopy: false,
    atomicRename: false,
    atomicNoClobber: false,
  },
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

function setInput(selector: string, value: string) {
  const input = root?.querySelector<HTMLInputElement | HTMLTextAreaElement>(selector);
  expect(input, selector).toBeTruthy();
  if (!input) return;
  input.value = value;
  input.dispatchEvent(new Event("input", { bubbles: true }));
}

function setSelect(selector: string, value: string) {
  const select = root?.querySelector<HTMLSelectElement>(selector);
  expect(select, selector).toBeTruthy();
  if (!select) return;
  select.value = value;
  select.dispatchEvent(new Event("change", { bubbles: true }));
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
  it("serializes HDFS Native configuration without storing environment values", async () => {
    mocks.saveFileConnection.mockResolvedValue({
      ...connection,
      id: "hdfs-1",
      config: {
        type: "hdfs",
        implementation: "native",
        nameNodeUri: "hdfs://namenode:9000",
        root: "/dbx",
        options: { "dfs.client.use.datanode.hostname": "true" },
        hadoopConfigDirectory: "/etc/hadoop/conf",
        authenticationEnvironment: { userName: "HADOOP_USER_NAME" },
      },
    });
    await mountPage();

    root?.querySelector<HTMLButtonElement>('button[aria-label="fileManager.add"]')?.click();
    await nextTick();
    setSelect("#file-connection-type", "hdfs");
    await nextTick();
    expect(root?.textContent).toContain("fileManager.hdfsSecurity");
    expect(root?.querySelector("#file-connection-hdfs-implementation")).toBeTruthy();

    setInput("#file-connection-name", "Native HDFS");
    setInput("#file-connection-endpoint", "hdfs://namenode:9000");
    setInput("#file-connection-hdfs-root", "/dbx");
    setInput("#file-connection-hdfs-config", "/etc/hadoop/conf");
    setInput("#file-connection-hdfs-options", "dfs.client.use.datanode.hostname=true");
    root?.querySelector<HTMLInputElement>("#file-connection-hdfs-user-environment")?.click();
    await nextTick();

    const save = Array.from(root?.querySelectorAll<HTMLButtonElement>("button") ?? []).findLast((button) => button.textContent?.trim() === "common.save");
    save?.click();

    await vi.waitFor(() =>
      expect(mocks.saveFileConnection).toHaveBeenCalledWith({
        id: null,
        expectedRevision: undefined,
        name: "Native HDFS",
        config: {
          type: "hdfs",
          implementation: "native",
          nameNodeUri: "hdfs://namenode:9000",
          root: "/dbx",
          options: { "dfs.client.use.datanode.hostname": "true" },
          hadoopConfigDirectory: "/etc/hadoop/conf",
          authenticationEnvironment: { userName: "HADOOP_USER_NAME" },
        },
      }),
    );
  });

  it("serializes a new WebHDFS delegation token only in the secrets payload", async () => {
    mocks.saveFileConnection.mockResolvedValue({
      ...connection,
      id: "webhdfs-1",
      name: "WebHDFS files",
      config: {
        type: "hdfs",
        implementation: "webhdfs",
        endpoint: "http://localhost:9870",
        root: "/",
        authentication: "delegation",
        userName: "",
        disableListBatch: false,
        allowedDataNodeOrigins: ["http://localhost:9864"],
        dataNodeHostnameMapping: {},
        tlsCaCertificatePath: null,
        proxyUrl: null,
        proxyBypass: null,
        allowTlsDowngrade: false,
        connectTimeoutSeconds: 10,
        controlTimeoutSeconds: 30,
        idleTimeoutSeconds: 30,
        chunkSizeMib: 4,
        writeOptions: {
          permission: null,
          replication: null,
          blockSize: null,
          bufferSize: null,
        },
      },
    });
    await mountPage();

    root?.querySelector<HTMLButtonElement>('button[aria-label="fileManager.add"]')?.click();
    await nextTick();
    setSelect("#file-connection-type", "hdfs");
    await nextTick();
    setSelect("#file-connection-hdfs-implementation", "webhdfs");
    await nextTick();
    setSelect("#file-connection-webhdfs-auth", "delegation");
    await nextTick();
    setInput("#file-connection-name", "WebHDFS files");
    setInput("#file-connection-webhdfs-token", "delegation-secret");
    await nextTick();

    const save = Array.from(root?.querySelectorAll<HTMLButtonElement>("button") ?? []).findLast((button) => button.textContent?.trim() === "common.save");
    expect(save?.disabled).toBe(false);
    save?.click();

    await vi.waitFor(() => expect(mocks.saveFileConnection).toHaveBeenCalledOnce());
    const input = mocks.saveFileConnection.mock.calls[0]?.[0];
    expect(input).toEqual({
      id: null,
      expectedRevision: undefined,
      name: "WebHDFS files",
      config: {
        type: "hdfs",
        implementation: "webhdfs",
        endpoint: "http://localhost:9870",
        root: "/",
        authentication: "delegation",
        userName: "",
        disableListBatch: false,
        allowedDataNodeOrigins: ["http://localhost:9864"],
        dataNodeHostnameMapping: {},
        tlsCaCertificatePath: null,
        proxyUrl: null,
        proxyBypass: null,
        allowTlsDowngrade: false,
        connectTimeoutSeconds: 10,
        controlTimeoutSeconds: 30,
        idleTimeoutSeconds: 30,
        chunkSizeMib: 4,
        writeOptions: {
          permission: null,
          replication: null,
          blockSize: null,
          bufferSize: null,
        },
      },
      secrets: {
        webhdfsDelegationToken: "delegation-secret",
      },
    });
    expect(JSON.stringify(input?.config)).not.toContain("delegation-secret");
  });

  it("preserves or explicitly clears an existing WebHDFS delegation token during edit", async () => {
    const webhdfsConnection: FileConnection = {
      ...connection,
      id: "webhdfs-existing",
      name: "Existing WebHDFS",
      revision: 12,
      config: {
        type: "hdfs",
        implementation: "webhdfs",
        endpoint: "https://namenode.example:9871",
        root: "/tenant/dbx",
        authentication: "delegation",
        userName: "",
        disableListBatch: true,
        allowedDataNodeOrigins: ["https://datanode.example:9865"],
        dataNodeHostnameMapping: {
          "datanode.example": "10.0.0.12:9865",
        },
        tlsCaCertificatePath: "/etc/ssl/hadoop-ca.pem",
        proxyUrl: null,
        proxyBypass: "localhost",
        allowTlsDowngrade: false,
        connectTimeoutSeconds: 11,
        controlTimeoutSeconds: 31,
        idleTimeoutSeconds: 41,
        chunkSizeMib: 8,
        writeOptions: {
          permission: "640",
          replication: 2,
          blockSize: 134217728,
          bufferSize: 65536,
        },
      },
      hasPassword: false,
      hasCredentials: true,
    };
    mocks.listFileConnections.mockResolvedValue([webhdfsConnection]);
    mocks.saveFileConnection.mockResolvedValue(webhdfsConnection);
    await mountPage();

    root?.querySelector<HTMLButtonElement>('button[aria-label="fileManager.edit"]')?.click();
    await nextTick();
    expect(root?.querySelector<HTMLInputElement>("#file-connection-webhdfs-token")?.value).toBe("");
    expect(root?.querySelector<HTMLInputElement>("#file-connection-webhdfs-token")?.placeholder).toBe("fileManager.keepPassword");

    let save = Array.from(root?.querySelectorAll<HTMLButtonElement>("button") ?? []).findLast((button) => button.textContent?.trim() === "common.save");
    save?.click();
    await vi.waitFor(() => expect(mocks.saveFileConnection).toHaveBeenCalledTimes(1));
    expect(mocks.saveFileConnection.mock.calls[0]?.[0]).toEqual({
      id: "webhdfs-existing",
      expectedRevision: 12,
      name: "Existing WebHDFS",
      config: webhdfsConnection.config,
      secrets: undefined,
    });

    await vi.waitFor(() => expect(mocks.listFileConnections).toHaveBeenCalledTimes(2));
    await nextTick();
    root?.querySelector<HTMLButtonElement>('button[aria-label="fileManager.edit"]')?.click();
    await nextTick();
    const clearLabel = Array.from(root?.querySelectorAll<HTMLLabelElement>("label") ?? []).find((label) => label.textContent?.includes("fileManager.clearWebhdfsCredentials"));
    const clear = clearLabel?.querySelector<HTMLInputElement>('input[type="checkbox"]');
    expect(clear).toBeTruthy();
    clear?.click();
    await nextTick();
    expect(clear?.checked).toBe(true);

    save = Array.from(root?.querySelectorAll<HTMLButtonElement>("button") ?? []).findLast((button) => button.textContent?.trim() === "common.save");
    expect(save?.disabled).toBe(true);
    setSelect("#file-connection-webhdfs-auth", "simple");
    await nextTick();
    setInput("#file-connection-webhdfs-user", "dbx");
    await nextTick();
    await vi.waitFor(() => expect(save?.disabled).toBe(false));
    save?.click();
    await vi.waitFor(() => expect(mocks.saveFileConnection).toHaveBeenCalledTimes(2));
    expect(mocks.saveFileConnection.mock.calls[1]?.[0]).toEqual({
      id: "webhdfs-existing",
      expectedRevision: 12,
      name: "Existing WebHDFS",
      config: {
        ...webhdfsConnection.config,
        authentication: "simple",
        userName: "dbx",
      },
      secrets: {
        clearWebhdfsCredentials: true,
      },
    });
  });

  it("requires a token when changing an existing simple WebHDFS connection to delegation", async () => {
    const webhdfsConnection: FileConnection = {
      ...connection,
      id: "webhdfs-simple",
      revision: 7,
      hasPassword: false,
      hasCredentials: false,
      config: {
        type: "hdfs",
        implementation: "webhdfs",
        endpoint: "https://namenode.example:9871",
        root: "/tenant/dbx",
        authentication: "simple",
        userName: "dbx",
        disableListBatch: false,
        allowedDataNodeOrigins: ["https://datanode.example:9865"],
        dataNodeHostnameMapping: {},
        tlsCaCertificatePath: null,
        proxyUrl: null,
        proxyBypass: null,
        allowTlsDowngrade: false,
        connectTimeoutSeconds: 10,
        controlTimeoutSeconds: 30,
        idleTimeoutSeconds: 30,
        chunkSizeMib: 4,
        writeOptions: {
          permission: null,
          replication: null,
          blockSize: null,
          bufferSize: null,
        },
      },
    };
    mocks.listFileConnections.mockResolvedValue([webhdfsConnection]);
    mocks.saveFileConnection.mockResolvedValue(webhdfsConnection);
    await mountPage();

    root?.querySelector<HTMLButtonElement>('button[aria-label="fileManager.edit"]')?.click();
    await nextTick();
    setSelect("#file-connection-webhdfs-auth", "delegation");
    await nextTick();
    let save = Array.from(root?.querySelectorAll<HTMLButtonElement>("button") ?? []).findLast((button) => button.textContent?.trim() === "common.save");
    expect(save?.disabled).toBe(true);
    save?.click();
    expect(mocks.saveFileConnection).not.toHaveBeenCalled();

    setInput("#file-connection-webhdfs-token", "new-delegation-token");
    await nextTick();
    save = Array.from(root?.querySelectorAll<HTMLButtonElement>("button") ?? []).findLast((button) => button.textContent?.trim() === "common.save");
    expect(save?.disabled).toBe(false);
    save?.click();
    await vi.waitFor(() => expect(mocks.saveFileConnection).toHaveBeenCalledTimes(1));
    expect(mocks.saveFileConnection.mock.calls[0]?.[0]).toMatchObject({
      id: "webhdfs-simple",
      expectedRevision: 7,
      config: {
        authentication: "delegation",
        userName: "",
      },
      secrets: {
        webhdfsDelegationToken: "new-delegation-token",
      },
    });
  });

  it("submits inline SFTP private-key authentication without password-only support", async () => {
    mocks.saveFileConnection.mockResolvedValue({
      ...connection,
      id: "sftp-1",
      config: {
        type: "sftp",
        endpoint: "ssh://sftp.example:2222",
        root: "/srv/dbx",
        username: "dbx",
        authentication: "private_key",
      },
    });
    await mountPage();

    root?.querySelector<HTMLButtonElement>('button[aria-label="fileManager.add"]')?.click();
    await nextTick();
    setSelect("#file-connection-type", "sftp");
    await nextTick();
    expect(root?.textContent).toContain("fileManager.sftpSecurity");
    expect(root?.querySelector("#file-connection-sftp-auth")).toBeTruthy();

    setInput("#file-connection-name", "SFTP files");
    setInput("#file-connection-endpoint", "ssh://sftp.example:2222");
    setInput("#file-connection-sftp-root", "/srv/dbx");
    setInput("#file-connection-sftp-username", "dbx");
    setSelect("#file-connection-sftp-auth", "private_key");
    await nextTick();
    setInput("#file-connection-sftp-private-key", "-----BEGIN OPENSSH PRIVATE KEY-----\nkey\n-----END OPENSSH PRIVATE KEY-----");
    setInput("#file-connection-sftp-passphrase", "key-passphrase");
    await nextTick();

    const save = Array.from(root?.querySelectorAll<HTMLButtonElement>("button") ?? []).findLast((button) => button.textContent?.trim() === "common.save");
    expect(save?.disabled).toBe(false);
    save?.click();

    await vi.waitFor(() =>
      expect(mocks.saveFileConnection).toHaveBeenCalledWith({
        id: null,
        expectedRevision: undefined,
        name: "SFTP files",
        config: {
          type: "sftp",
          endpoint: "ssh://sftp.example:2222",
          root: "/srv/dbx",
          username: "dbx",
          authentication: "private_key",
        },
        secrets: {
          sftpPrivateKey: "-----BEGIN OPENSSH PRIVATE KEY-----\nkey\n-----END OPENSSH PRIVATE KEY-----",
          sftpPrivateKeyPassphrase: "key-passphrase",
        },
      }),
    );
  });

  it("rejects a new SFTP passphrase without new private-key material", async () => {
    await mountPage();

    root?.querySelector<HTMLButtonElement>('button[aria-label="fileManager.add"]')?.click();
    await nextTick();
    setSelect("#file-connection-type", "sftp");
    await nextTick();
    setInput("#file-connection-name", "SFTP files");
    setSelect("#file-connection-sftp-auth", "private_key");
    await nextTick();
    setInput("#file-connection-sftp-passphrase", "passphrase-without-key");
    await nextTick();

    const save = Array.from(root?.querySelectorAll<HTMLButtonElement>("button") ?? []).findLast((button) => button.textContent?.trim() === "common.save");
    expect(save?.disabled).toBe(true);
    save?.click();
    expect(mocks.saveFileConnection).not.toHaveBeenCalled();
  });

  it("round-trips an existing HDFS Native nested configuration through edit", async () => {
    const hdfsConnection: FileConnection = {
      ...connection,
      id: "hdfs-existing",
      name: "Existing HDFS",
      revision: 8,
      config: {
        type: "hdfs",
        implementation: "native",
        nameNodeUri: "hdfs://namenode:9000",
        root: "/tenant/dbx",
        options: {
          "dfs.client.use.datanode.hostname": "true",
          "dfs.user.home.dir.prefix": "/users",
        },
        hadoopConfigDirectory: "/etc/hadoop/conf",
        authenticationEnvironment: { userName: "HADOOP_USER_NAME" },
      },
      hasPassword: false,
      hasCredentials: false,
    };
    mocks.listFileConnections.mockResolvedValue([hdfsConnection]);
    mocks.saveFileConnection.mockResolvedValue(hdfsConnection);
    await mountPage();

    root?.querySelector<HTMLButtonElement>('button[aria-label="fileManager.edit"]')?.click();
    await nextTick();
    expect(root?.querySelector<HTMLInputElement>("#file-connection-endpoint")?.value).toBe("hdfs://namenode:9000");
    expect(root?.querySelector<HTMLInputElement>("#file-connection-hdfs-root")?.value).toBe("/tenant/dbx");
    expect(root?.querySelector<HTMLInputElement>("#file-connection-hdfs-config")?.value).toBe("/etc/hadoop/conf");
    expect(root?.querySelector<HTMLTextAreaElement>("#file-connection-hdfs-options")?.value).toContain("dfs.client.use.datanode.hostname=true");
    expect(root?.querySelector<HTMLInputElement>("#file-connection-hdfs-user-environment")?.checked).toBe(true);

    const save = Array.from(root?.querySelectorAll<HTMLButtonElement>("button") ?? []).findLast((button) => button.textContent?.trim() === "common.save");
    save?.click();
    await vi.waitFor(() =>
      expect(mocks.saveFileConnection).toHaveBeenCalledWith({
        id: "hdfs-existing",
        expectedRevision: 8,
        name: "Existing HDFS",
        config: hdfsConnection.config,
      }),
    );
  });

  it("keeps the connection editor footer outside the scroll region at small viewport heights", async () => {
    vi.stubGlobal("innerHeight", 420);
    await mountPage();

    root?.querySelector<HTMLButtonElement>('button[aria-label="fileManager.add"]')?.click();
    await nextTick();
    setSelect("#file-connection-type", "sftp");
    await nextTick();
    setSelect("#file-connection-sftp-auth", "private_key");
    await nextTick();

    const scrollRegion = root?.querySelector<HTMLElement>('[data-testid="connection-editor-scroll-region"]');
    const footer = root?.querySelector<HTMLElement>('[data-testid="connection-editor-footer"]');
    const dialog = scrollRegion?.parentElement;
    expect(dialog?.className).toContain("max-h-[calc(100dvh-2rem)]");
    expect(dialog?.className).toContain("overflow-hidden");
    expect(scrollRegion?.className).toContain("min-h-0");
    expect(scrollRegion?.className).toContain("overflow-y-auto");
    expect(scrollRegion?.querySelector("#file-connection-sftp-private-key")).toBeTruthy();
    expect(footer?.parentElement).toBe(dialog);
    expect(scrollRegion?.contains(footer ?? null)).toBe(false);
    expect(footer?.className).toContain("shrink-0");
  });

  it("preserves the stored SFTP key bundle when an edit omits both secret fields", async () => {
    const sftpConnection: FileConnection = {
      ...connection,
      id: "sftp-existing",
      name: "Existing SFTP",
      revision: 4,
      config: {
        type: "sftp",
        endpoint: "prod-sftp",
        root: "/srv/dbx",
        username: "dbx",
        authentication: "private_key",
      },
      hasCredentials: true,
    };
    mocks.listFileConnections.mockResolvedValue([sftpConnection]);
    mocks.saveFileConnection.mockResolvedValue(sftpConnection);
    await mountPage();

    root?.querySelector<HTMLButtonElement>('button[aria-label="fileManager.edit"]')?.click();
    await nextTick();
    expect(root?.querySelector<HTMLTextAreaElement>("#file-connection-sftp-private-key")?.placeholder).toBe("fileManager.keepPrivateKey");
    const save = Array.from(root?.querySelectorAll<HTMLButtonElement>("button") ?? []).findLast((button) => button.textContent?.trim() === "common.save");
    expect(save?.disabled).toBe(false);
    save?.click();

    await vi.waitFor(() =>
      expect(mocks.saveFileConnection).toHaveBeenCalledWith({
        id: "sftp-existing",
        expectedRevision: 4,
        name: "Existing SFTP",
        config: {
          type: "sftp",
          endpoint: "prod-sftp",
          root: "/srv/dbx",
          username: "dbx",
          authentication: "private_key",
        },
        secrets: undefined,
      }),
    );
  });

  it("shows protocol-specific connection security warnings", async () => {
    await mountPage();

    expect(root?.textContent).toContain("fileManager.ftpSecurity");
    expect(root?.textContent).not.toContain("fileManager.s3Security");

    const typeSelect = root?.querySelector<HTMLSelectElement>("#file-connection-type");
    expect(typeSelect).toBeTruthy();
    if (!typeSelect) return;
    typeSelect.value = "s3";
    typeSelect.dispatchEvent(new Event("change", { bubbles: true }));
    await nextTick();

    expect(root?.textContent).not.toContain("fileManager.ftpSecurity");
    expect(root?.textContent).toContain("fileManager.s3Security");

    typeSelect.value = "webdav";
    typeSelect.dispatchEvent(new Event("change", { bubbles: true }));
    await nextTick();

    expect(root?.textContent).not.toContain("fileManager.s3Security");
    expect(root?.textContent).toContain("fileManager.webdavSecurity");
    expect(root?.querySelector("#file-connection-webdav-auth")).toBeTruthy();
    expect(root?.querySelector("#file-connection-webdav-password")).toBeTruthy();
  });

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

  it("hides replace for WebHDFS and always submits remote copy as no-clobber", async () => {
    const webhdfsConnection: FileConnection = {
      ...connection,
      id: "webhdfs-copy",
      hasPassword: false,
      hasCredentials: false,
      config: {
        type: "hdfs",
        implementation: "webhdfs",
        endpoint: "http://localhost:9870",
        root: "/",
        authentication: "simple",
        userName: "dbx",
        disableListBatch: false,
        allowedDataNodeOrigins: ["http://localhost:9864"],
        dataNodeHostnameMapping: {},
        tlsCaCertificatePath: null,
        proxyUrl: null,
        proxyBypass: null,
        allowTlsDowngrade: false,
        connectTimeoutSeconds: 10,
        controlTimeoutSeconds: 30,
        idleTimeoutSeconds: 30,
        chunkSizeMib: 4,
        writeOptions: {
          permission: null,
          replication: null,
          blockSize: null,
          bufferSize: null,
        },
      },
    };
    const running = transfer("webhdfs-copy", "running", {
      connectionId: webhdfsConnection.id,
      direction: "copy",
      remotePath: remoteEntry.path,
      localPath: "a%2Fb copy",
    });
    mocks.listFileConnections.mockResolvedValue([webhdfsConnection]);
    mocks.startFileCopy.mockResolvedValue({ transferId: running.id });
    mocks.getFileTransfer.mockResolvedValue(running);
    await mountPage();

    root?.querySelector<HTMLButtonElement>('button[aria-label="fileManager.copy: remote.bin"]')?.click();
    await nextTick();
    const replaceLabel = Array.from(root?.querySelectorAll<HTMLLabelElement>("label") ?? []).find((label) => label.textContent?.trim() === "fileManager.replaceDestination");
    expect(replaceLabel).toBeUndefined();

    const submit = Array.from(root?.querySelectorAll<HTMLButtonElement>("button") ?? [])
      .filter((button) => button.textContent?.trim() === "fileManager.copy")
      .at(-1);
    submit?.click();
    await vi.waitFor(() =>
      expect(mocks.startFileCopy).toHaveBeenCalledWith({
        connectionId: webhdfsConnection.id,
        sourcePath: remoteEntry.path,
        destinationPath: "a%2Fb copy",
        policy: {
          mode: "best_effort_no_clobber",
          atomicNoClobber: false,
          externalToctouRisk: true,
        },
      }),
    );
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
