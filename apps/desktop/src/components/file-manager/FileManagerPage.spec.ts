// @vitest-environment happy-dom

import { createApp, nextTick, type App } from "vue";
import { createPinia } from "pinia";
import { afterEach, describe, expect, it, vi } from "vitest";
import { useToast } from "@/composables/useToast";
import i18n from "@/i18n";
import { useConnectionStore } from "@/stores/connectionStore";
import FileManagerPage from "./FileManagerPage.vue";
import { displayFilePath, parentFilePath } from "./filePath";

const { copyFilePath, createFileDirectory, deleteFilePath, downloadFile, executeWithProductionContextGuard, listFilePath, renameFilePath, revealPathInFileManager, shellOpen, uploadFile } = vi.hoisted(() => ({
  copyFilePath: vi.fn(async (_request: unknown) => undefined),
  createFileDirectory: vi.fn(async (_request: unknown) => undefined),
  deleteFilePath: vi.fn(async (_connectionId: string, _path: string) => undefined),
  downloadFile: vi.fn(async (_request: unknown, onProgress?: (progress: { bytesTransferred: number; totalBytes: number }) => void) => {
    onProgress?.({ bytesTransferred: 1536, totalBytes: 1536 });
    return 1536;
  }),
  executeWithProductionContextGuard: vi.fn(async (options: { execute: () => Promise<unknown> }) => options.execute()),
  listFilePath: vi.fn(async (_connectionId: string, path: string) =>
    path === "folder"
      ? [{ path: "folder/child.txt", name: "child.txt", kind: "file", size: 7 }]
      : path
        ? []
        : [
            { path: "/", name: "/", kind: "directory", size: 0 },
            { path: "folder", name: "folder", kind: "directory", size: 0 },
            { path: "fixture.txt", name: "fixture.txt", kind: "file", size: 1536, modifiedAt: "2026-07-27T00:00:00Z" },
          ],
  ),
  renameFilePath: vi.fn(async (_request: unknown) => undefined),
  revealPathInFileManager: vi.fn(async (_path: string) => undefined),
  shellOpen: vi.fn(async (_path: string) => undefined),
  uploadFile: vi.fn(async (_request: unknown) => 11),
}));

vi.mock("@/lib/database/productionExecutionGuard", () => ({
  executeWithProductionContextGuard,
}));

vi.mock("@/lib/tabs/tabResultCache", () => ({
  deleteTabResultSnapshotsForOwner: vi.fn(async () => undefined),
}));

vi.mock("@/components/ui/popover", async () => {
  const { defineComponent, h } = await import("vue");
  const passthrough = defineComponent({
    setup(_props, { slots }) {
      return () => h("div", slots.default?.());
    },
  });
  return { Popover: passthrough, PopoverContent: passthrough, PopoverTrigger: passthrough };
});

vi.mock("@/lib/backend/api", () => ({
  loadConnections: vi.fn(async () => [
    {
      id: "ftp-local",
      name: "Local FTP",
      db_type: "file",
      driver_profile: "ftp",
      driver_label: "FTP",
      host: "127.0.0.1",
      port: 2121,
      username: "dbx",
      password: "",
      ssl: false,
      read_only: false,
      external_config: { protocol: "ftp", endpoint: "127.0.0.1", port: 2121, root: "/ftp/dbx/", username: "dbx" },
    },
    {
      id: "ftp-other",
      name: "Other FTP",
      db_type: "file",
      driver_profile: "ftp",
      driver_label: "FTP",
      host: "127.0.0.1",
      port: 2122,
      username: "dbx",
      password: "",
      ssl: false,
      read_only: false,
      external_config: { protocol: "ftp", endpoint: "127.0.0.1", port: 2122, root: "/ftp/other/", username: "dbx" },
    },
  ]),
  loadSidebarLayout: vi.fn(async () => null),
  loadTunnelProfiles: vi.fn(async () => []),
  deleteSchemaCache: vi.fn(async () => undefined),
  deleteSchemaCachePrefix: vi.fn(async () => undefined),
  listFileConnections: vi.fn(async () => [
    {
      id: "ftp-local",
      name: "Local FTP",
      config: { protocol: "ftp", endpoint: "127.0.0.1", port: 2121, root: "/ftp/dbx/", username: "dbx" },
      capabilities: {
        read: true,
        write: true,
        stat: true,
        list: true,
        delete: true,
        copy: true,
        rename: true,
        nativeCopy: false,
        nativeRename: false,
        atomicRename: false,
        atomicNoClobber: false,
        copyMode: "stream_relay",
        renameMode: "copy_delete",
      },
      secretStatus: {
        password: true,
        privateKey: false,
        accessKey: false,
        secretKey: false,
        sessionToken: false,
        bearerToken: false,
        delegationToken: false,
      },
    },
    {
      id: "ftp-other",
      name: "Other FTP",
      config: { protocol: "ftp", endpoint: "127.0.0.1", port: 2122, root: "/ftp/other/", username: "dbx" },
      capabilities: {
        read: true,
        write: true,
        stat: true,
        list: true,
        delete: true,
        copy: true,
        rename: true,
        nativeCopy: false,
        nativeRename: false,
        atomicRename: false,
        atomicNoClobber: false,
        copyMode: "stream_relay",
        renameMode: "copy_delete",
      },
      secretStatus: {
        password: true,
        privateKey: false,
        accessKey: false,
        secretKey: false,
        sessionToken: false,
        bearerToken: false,
        delegationToken: false,
      },
    },
  ]),
  saveConnections: vi.fn(),
  listFilePath,
  statFilePath: vi.fn(),
  uploadFile,
  downloadFile,
  deleteFilePath,
  createFileDirectory,
  copyFilePath,
  renameFilePath,
  revealPathInFileManager,
}));

vi.mock("@tauri-apps/plugin-dialog", () => ({
  open: vi.fn(async () => "/tmp/replacement.txt"),
  save: vi.fn(async () => "/tmp/fixture.txt"),
}));

vi.mock("@tauri-apps/plugin-shell", () => ({
  open: shellOpen,
}));

const mountedApps: App[] = [];

async function flushPage() {
  await nextTick();
  await new Promise((resolve) => setTimeout(resolve, 0));
  await nextTick();
}

async function flushEntrySingleClick() {
  await new Promise((resolve) => setTimeout(resolve, 200));
  await flushPage();
}

async function mountPage(connectionId?: string) {
  const container = document.createElement("div");
  document.body.append(container);
  const app = createApp(FileManagerPage, connectionId ? { connectionId } : undefined);
  mountedApps.push(app);
  app.use(createPinia());
  app.use(i18n);
  const page = app.mount(container) as unknown as {
    openConnectionById: (connectionId: string) => Promise<void>;
  };
  await flushPage();
  return { container, page };
}

async function mountOpenPage() {
  const mounted = await mountPage();
  await mounted.page.openConnectionById("ftp-local");
  await flushPage();
  return mounted;
}

async function mountPageWithStore() {
  const container = document.createElement("div");
  document.body.append(container);
  const app = createApp(FileManagerPage);
  mountedApps.push(app);
  const pinia = createPinia();
  app.use(pinia);
  app.use(i18n);
  const page = app.mount(container) as unknown as {
    openConnectionById: (connectionId: string) => Promise<void>;
  };
  await flushPage();
  return { page, store: useConnectionStore(pinia) };
}

function buttonWithTitle(title: string): HTMLButtonElement | undefined {
  return Array.from(document.querySelectorAll<HTMLButtonElement>("button")).find((button) => button.title === title);
}

function entryRow(path: string): HTMLTableRowElement | null {
  return document.querySelector<HTMLTableRowElement>(`tr[data-file-entry-path="${path}"]`);
}

afterEach(() => {
  for (const app of mountedApps.splice(0)) app.unmount();
  document.body.innerHTML = "";
  listFilePath.mockClear();
  uploadFile.mockClear();
  downloadFile.mockClear();
  deleteFilePath.mockClear();
  createFileDirectory.mockClear();
  executeWithProductionContextGuard.mockClear();
  copyFilePath.mockClear();
  renameFilePath.mockClear();
  revealPathInFileManager.mockClear();
  shellOpen.mockClear();
});

describe("FileManagerPage browsing", () => {
  it("opens its connection from the tab-scoped prop", async () => {
    await mountPage("ftp-local");

    expect(listFilePath).toHaveBeenLastCalledWith("ftp-local", "");
    expect(document.body.textContent).toContain("fixture.txt");
  });

  it("exposes one layout root so parent visibility directives apply", async () => {
    const { container } = await mountPage();

    expect(container.childElementCount).toBe(1);
    expect(container.firstElementChild?.classList.contains("flex")).toBe(true);
    expect(container.firstElementChild?.classList.contains("min-h-0")).toBe(true);
    expect(container.firstElementChild?.classList.contains("flex-1")).toBe(true);
  });

  it("does not render a file connection index or creation control", async () => {
    await mountPage();

    expect(document.querySelector("[data-file-manager-loading]")).not.toBeNull();
    expect(document.body.textContent).not.toContain("Local FTP");
    expect(document.body.textContent).not.toContain("New file connection");
  });

  it("uses one toolbar and omits the type column", async () => {
    await mountOpenPage();

    const toolbar = document.querySelector<HTMLElement>("[data-file-manager-toolbar]");
    expect(document.querySelectorAll("header")).toHaveLength(1);
    expect(toolbar?.textContent).not.toContain("Local FTP");
    expect(toolbar?.textContent).toContain("/");
    expect(toolbar?.textContent).toContain("New folder");
    expect(toolbar?.textContent).toContain("Upload");
    expect(toolbar?.textContent).toContain("Downloads");
    expect(toolbar?.querySelector('button[title="Refresh"]')).not.toBeNull();

    const headings = Array.from(document.querySelectorAll("thead th")).map((heading) => heading.textContent?.trim());
    expect(headings).toEqual(["Name", "Size", "Modified", "Actions"]);
    expect(toolbar!.textContent!.indexOf("New folder")).toBeLessThan(toolbar!.textContent!.indexOf("Upload"));
  });

  it("creates a folder in the current directory and refreshes the listing", async () => {
    await mountOpenPage();

    const newFolderButton = Array.from(document.querySelectorAll<HTMLButtonElement>("button")).find((button) => button.textContent?.includes("New folder"));
    newFolderButton?.click();
    await flushPage();

    const input = document.querySelector<HTMLInputElement>("#file-create-directory-name");
    expect(input).not.toBeNull();
    input!.value = "reports";
    input!.dispatchEvent(new Event("input", { bubbles: true }));
    await flushPage();

    const createButton = Array.from(document.querySelectorAll<HTMLButtonElement>("button")).find((button) => button.textContent?.trim() === "Create");
    createButton?.click();
    await flushPage();

    expect(createFileDirectory).toHaveBeenCalledWith({
      connectionId: "ftp-local",
      path: "reports",
    });
    expect(executeWithProductionContextGuard).toHaveBeenCalledWith(
      expect.objectContaining({
        reviewText: "CREATE DIRECTORY reports",
      }),
    );
    expect(listFilePath).toHaveBeenLastCalledWith("ftp-local", "");
  });

  it("opens the connection root, displays metadata, navigates into a directory, and returns to root", async () => {
    await mountOpenPage();

    expect(listFilePath).toHaveBeenLastCalledWith("ftp-local", "");
    expect(document.body.textContent).toContain("fixture.txt");
    expect(document.body.textContent).toContain("1.5 KiB");
    expect(document.body.textContent).toContain("/");
    expect(entryRow("")).toBeNull();
    expect(document.querySelector('tr[data-file-entry-path="/"]')).toBeNull();
    expect(entryRow("fixture.txt")?.querySelector("svg")).not.toBeNull();

    const folderRow = entryRow("folder");
    folderRow?.dispatchEvent(new MouseEvent("click", { bubbles: true, detail: 1 }));
    folderRow?.dispatchEvent(new MouseEvent("click", { bubbles: true, detail: 2 }));
    folderRow?.dispatchEvent(new MouseEvent("dblclick", { bubbles: true, detail: 2 }));
    await flushPage();
    expect(listFilePath).toHaveBeenLastCalledWith("ftp-local", "folder");
    expect(document.body.textContent).toContain("/folder");
    expect(document.querySelector("tbody tr:first-child")?.textContent).toContain("../");

    document.querySelector<HTMLElement>("[data-file-parent-row]")?.click();
    await flushPage();
    expect(listFilePath).toHaveBeenLastCalledWith("ftp-local", "");
  });

  it("expands folders inline without changing the current directory", async () => {
    await mountOpenPage();

    entryRow("folder")?.dispatchEvent(new MouseEvent("click", { bubbles: true, detail: 1 }));
    await flushEntrySingleClick();

    expect(listFilePath).toHaveBeenLastCalledWith("ftp-local", "folder");
    expect(entryRow("folder/child.txt")).not.toBeNull();
    expect(entryRow("folder/child.txt")?.firstElementChild?.getAttribute("style")).toContain("24px");
    expect(document.body.textContent).toContain("/");
    expect(document.querySelector("[data-file-parent-row]")).toBeNull();
  });

  it("downloads a file when its row is clicked", async () => {
    await mountOpenPage();

    entryRow("fixture.txt")?.dispatchEvent(new MouseEvent("click", { bubbles: true, detail: 1 }));
    await flushEntrySingleClick();

    expect(downloadFile).toHaveBeenCalledWith(
      {
        connectionId: "ftp-local",
        remotePath: "fixture.txt",
        localPath: "/tmp/fixture.txt",
        replace: false,
      },
      expect.any(Function),
    );
  });

  it("shows download progress and opens completed files or their folders", async () => {
    let finishDownload: ((bytes: number) => void) | undefined;
    downloadFile.mockImplementationOnce(async (_request, onProgress) => {
      onProgress?.({ bytesTransferred: 512, totalBytes: 1536 });
      return new Promise<number>((resolve) => {
        finishDownload = resolve;
      });
    });
    await mountOpenPage();

    entryRow("fixture.txt")?.dispatchEvent(new MouseEvent("click", { bubbles: true, detail: 1 }));
    await flushEntrySingleClick();
    document.querySelector<HTMLButtonElement>("[data-file-download-list-trigger]")?.click();
    await flushPage();

    const task = document.querySelector<HTMLElement>('[data-file-download-task="fixture.txt"]');
    expect(task?.textContent).toContain("fixture.txt");
    expect(task?.textContent).not.toContain("Open file");
    expect(task?.textContent).not.toContain("Open folder");
    expect(task?.querySelector('button[aria-label="Open file"] svg')).not.toBeNull();
    expect(task?.querySelector('button[aria-label="Open folder"] svg')).not.toBeNull();
    expect(task?.textContent).toContain("512 B / 1.5 KiB");
    expect(task?.textContent).toContain("33%");

    finishDownload?.(1536);
    await flushPage();
    expect(task?.textContent).toContain("Completed");

    buttonWithTitle("Open file")?.click();
    buttonWithTitle("Open folder")?.click();
    await flushPage();
    expect(shellOpen).toHaveBeenCalledWith("/tmp/fixture.txt");
    expect(revealPathInFileManager).toHaveBeenCalledWith("/tmp/fixture.txt");
  });

  it("shows only downloads from the active connection", async () => {
    const { page } = await mountOpenPage();

    entryRow("fixture.txt")?.dispatchEvent(new MouseEvent("click", { bubbles: true, detail: 1 }));
    await flushEntrySingleClick();
    expect(document.querySelector('[data-file-download-task="fixture.txt"]')).not.toBeNull();

    await page.openConnectionById("ftp-other");
    await flushPage();
    expect(document.querySelector('[data-file-download-task="fixture.txt"]')).toBeNull();
    expect(document.querySelector("[data-file-download-list]")?.textContent).toContain("No downloads for this connection");
  });

  it("opens shared right-click actions for files and folders", async () => {
    await mountOpenPage();

    entryRow("folder")?.dispatchEvent(new MouseEvent("contextmenu", { bubbles: true, clientX: 20, clientY: 20 }));
    await flushPage();
    expect(document.body.textContent).toContain("Expand folder");
    expect(document.body.textContent).toContain("Rename or move");
    expect(document.body.textContent).toContain("Copy path");

    document.body.dispatchEvent(new PointerEvent("pointerdown", { bubbles: true, button: 0 }));
    entryRow("fixture.txt")?.dispatchEvent(new MouseEvent("contextmenu", { bubbles: true, clientX: 30, clientY: 30 }));
    await flushPage();
    expect(document.body.textContent).toContain("Download");
    expect(document.body.textContent).toContain("Copy");
    expect(document.body.textContent).toContain("Delete");
  });

  it("keeps root as the highest visible path", () => {
    expect(parentFilePath("folder")).toBe("");
    expect(parentFilePath("folder/child")).toBe("folder");
    expect(displayFilePath("")).toBe("/");
  });

  it("reports a stale sidebar connection instead of silently ignoring it", async () => {
    const { page } = await mountPage();

    await expect(page.openConnectionById("missing")).rejects.toThrow("File connection no longer exists");
  });

  it("uses the tab title and close control instead of duplicating them in the toolbar", async () => {
    await mountOpenPage();

    const toolbar = document.querySelector<HTMLElement>("[data-file-manager-toolbar]");
    expect(toolbar?.querySelector('button[title="Close"]')).toBeNull();
    expect(toolbar?.textContent).not.toContain("Local FTP");
  });

  it("refreshes the active connection snapshot after a generic connection edit", async () => {
    const { page, store } = await mountPageWithStore();
    await page.openConnectionById("ftp-local");
    await flushPage();
    expect(document.body.textContent).toContain("Upload");

    const config = store.getConfig("ftp-local");
    expect(config).toBeDefined();
    await store.updateConnection({ ...config!, read_only: true });
    await flushPage();

    expect(document.body.textContent).not.toContain("Upload");
    expect(buttonWithTitle("Copy")).toBeUndefined();
    expect(buttonWithTitle("Rename")).toBeUndefined();
    expect(buttonWithTitle("Delete")).toBeUndefined();
  });

  it("requires explicit Replace before retrying an existing upload", async () => {
    uploadFile.mockRejectedValueOnce({ code: "already_exists", message: "redacted" }).mockResolvedValueOnce(11);
    await mountOpenPage();

    const uploadButton = Array.from(document.querySelectorAll<HTMLButtonElement>("button")).find((button) => button.textContent?.includes("Upload"));
    uploadButton?.click();
    await flushPage();
    const uploadButtons = Array.from(document.querySelectorAll<HTMLButtonElement>("button")).filter((button) => button.textContent?.trim() === "Upload");
    const uploadConfirm = uploadButtons[uploadButtons.length - 1];
    uploadConfirm?.click();
    await flushPage();

    expect(uploadFile).toHaveBeenCalledTimes(1);
    expect(uploadFile).toHaveBeenLastCalledWith({
      connectionId: "ftp-local",
      remotePath: "replacement.txt",
      localPath: "/tmp/replacement.txt",
      replace: false,
    });

    const replaceButton = Array.from(document.querySelectorAll<HTMLButtonElement>("button")).find((button) => button.textContent?.trim() === "Replace");
    replaceButton?.click();
    await flushPage();
    expect(uploadFile).toHaveBeenCalledTimes(2);
    expect(uploadFile.mock.calls[1]?.[0]).toMatchObject({ replace: true });
  });

  it("requires confirmation before deleting an entry", async () => {
    await mountOpenPage();

    buttonWithTitle("Delete")?.click();
    await flushPage();
    expect(deleteFilePath).not.toHaveBeenCalled();

    const deleteButton = Array.from(document.querySelectorAll<HTMLButtonElement>("button")).find((button) => button.textContent?.trim() === "Delete");
    deleteButton?.click();
    await flushPage();
    expect(deleteFilePath).toHaveBeenCalledWith("ftp-local", "folder");
  });

  it("does not execute file mutations when production confirmation is cancelled", async () => {
    await mountOpenPage();

    executeWithProductionContextGuard.mockResolvedValueOnce(undefined);
    const uploadButton = Array.from(document.querySelectorAll<HTMLButtonElement>("button")).find((button) => button.textContent?.includes("Upload"));
    uploadButton?.click();
    await flushPage();
    const uploadButtons = Array.from(document.querySelectorAll<HTMLButtonElement>("button")).filter((button) => button.textContent?.trim() === "Upload");
    uploadButtons[uploadButtons.length - 1]?.click();
    await flushPage();
    expect(uploadFile).not.toHaveBeenCalled();

    executeWithProductionContextGuard.mockResolvedValueOnce(undefined);
    buttonWithTitle("Delete")?.click();
    await flushPage();
    const deleteButton = Array.from(document.querySelectorAll<HTMLButtonElement>("button")).find((button) => button.textContent?.trim() === "Delete");
    deleteButton?.click();
    await flushPage();
    expect(deleteFilePath).not.toHaveBeenCalled();

    executeWithProductionContextGuard.mockResolvedValueOnce(undefined);
    buttonWithTitle("Copy")?.click();
    await flushPage();
    const copyButtons = Array.from(document.querySelectorAll<HTMLButtonElement>("button")).filter((button) => button.textContent?.trim() === "Copy");
    copyButtons[copyButtons.length - 1]?.click();
    await flushPage();
    expect(copyFilePath).not.toHaveBeenCalled();

    executeWithProductionContextGuard.mockResolvedValueOnce(undefined);
    buttonWithTitle("Rename or move")?.click();
    await flushPage();
    const destination = document.querySelector<HTMLInputElement>("#file-operation-destination-path");
    if (destination) {
      destination.value = "moved.txt";
      destination.dispatchEvent(new Event("input"));
    }
    await flushPage();
    const renameButton = Array.from(document.querySelectorAll<HTMLButtonElement>("button")).find((button) => button.textContent?.trim() === "Rename or move");
    renameButton?.click();
    await flushPage();
    expect(renameFilePath).not.toHaveBeenCalled();
  });

  it("shows capability-driven Copy only for files and sends one connection ID", async () => {
    copyFilePath.mockRejectedValueOnce({ code: "already_exists", message: "redacted" }).mockResolvedValueOnce(undefined);
    await mountOpenPage();

    expect(document.querySelectorAll('button[title="Copy"]')).toHaveLength(1);
    buttonWithTitle("Copy")?.click();
    await flushPage();
    expect(document.body.textContent).toContain("bounded streaming relay");
    expect(document.body.textContent).toContain("best-effort");

    const destination = document.querySelector<HTMLInputElement>("#file-operation-destination-path");
    expect(destination?.value).toBe("fixture.txt.copy");
    const copyButtons = Array.from(document.querySelectorAll<HTMLButtonElement>("button")).filter((button) => button.textContent?.trim() === "Copy");
    copyButtons[copyButtons.length - 1]?.click();
    await flushPage();

    expect(copyFilePath).toHaveBeenCalledTimes(1);
    expect(copyFilePath).toHaveBeenCalledWith({
      connectionId: "ftp-local",
      sourcePath: "fixture.txt",
      destinationPath: "fixture.txt.copy",
      replace: false,
    });
    expect(copyFilePath.mock.calls[0]?.[0]).not.toHaveProperty("destinationConnectionId");

    const replaceButton = Array.from(document.querySelectorAll<HTMLButtonElement>("button")).find((button) => button.textContent?.trim() === "Replace");
    replaceButton?.click();
    await flushPage();
    expect(copyFilePath).toHaveBeenCalledTimes(2);
    expect(copyFilePath.mock.calls[1]?.[0]).toMatchObject({ replace: true });
  });

  it("shows non-atomic Rename risk and preserves partial-success recovery", async () => {
    renameFilePath.mockRejectedValueOnce({
      code: "partial_success",
      message: "The destination was created, but the source file could not be deleted",
      recovery: "Delete the source manually.",
    });
    await mountOpenPage();

    entryRow("fixture.txt")?.querySelector<HTMLButtonElement>('button[title="Rename or move"]')?.click();
    await flushPage();
    expect(document.body.textContent).toContain("non-atomic");
    const destination = document.querySelector<HTMLInputElement>("#file-operation-destination-path");
    if (destination) {
      destination.value = "moved.txt";
      destination.dispatchEvent(new Event("input"));
    }
    await flushPage();
    const renameButton = Array.from(document.querySelectorAll<HTMLButtonElement>("button")).find((button) => button.textContent?.trim() === "Rename or move");
    renameButton?.click();
    await flushPage();

    expect(renameFilePath).toHaveBeenCalledWith({
      connectionId: "ftp-local",
      sourcePath: "fixture.txt",
      destinationPath: "moved.txt",
      replace: false,
    });
    expect(useToast().message.value).toContain("Delete the source manually.");
  });

  it("renames or moves folders through the same operation flow", async () => {
    await mountOpenPage();

    entryRow("folder")?.querySelector<HTMLButtonElement>('button[title="Rename or move"]')?.click();
    await flushPage();
    expect(document.body.textContent).toContain("Folder moves are non-atomic");
    const destination = document.querySelector<HTMLInputElement>("#file-operation-destination-path");
    if (destination) {
      destination.value = "moved-folder";
      destination.dispatchEvent(new Event("input"));
    }
    await flushPage();
    const renameButton = Array.from(document.querySelectorAll<HTMLButtonElement>("button")).find((button) => button.textContent?.trim() === "Rename or move");
    renameButton?.click();
    await flushPage();

    expect(renameFilePath).toHaveBeenCalledWith({
      connectionId: "ftp-local",
      sourcePath: "folder",
      destinationPath: "moved-folder",
      replace: false,
    });
  });
});
