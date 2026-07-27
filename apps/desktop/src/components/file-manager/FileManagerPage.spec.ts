// @vitest-environment happy-dom

import { createApp, defineComponent, h, nextTick, type App } from "vue";
import { createPinia } from "pinia";
import { afterEach, describe, expect, it, vi } from "vitest";
import { useToast } from "@/composables/useToast";
import i18n from "@/i18n";
import FileManagerPage from "./FileManagerPage.vue";
import { displayFilePath, parentFilePath } from "./filePath";

const { copyFilePath, deleteFilePath, downloadFile, listFilePath, renameFilePath, uploadFile } = vi.hoisted(() => ({
  copyFilePath: vi.fn(async (_request: unknown) => undefined),
  deleteFilePath: vi.fn(async (_connectionId: string, _path: string) => undefined),
  downloadFile: vi.fn(async (_request: unknown) => 11),
  listFilePath: vi.fn(async (_connectionId: string, path: string) =>
    path
      ? []
      : [
          { path: "folder", name: "folder", kind: "directory", size: 0 },
          { path: "fixture.txt", name: "fixture.txt", kind: "file", size: 1536, modifiedAt: "2026-07-27T00:00:00Z" },
        ],
  ),
  renameFilePath: vi.fn(async (_request: unknown) => undefined),
  uploadFile: vi.fn(async (_request: unknown) => 11),
}));

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
  ]),
  loadSidebarLayout: vi.fn(async () => null),
  loadTunnelProfiles: vi.fn(async () => []),
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
  ]),
  saveConnections: vi.fn(),
  listFilePath,
  statFilePath: vi.fn(),
  uploadFile,
  downloadFile,
  deleteFilePath,
  copyFilePath,
  renameFilePath,
}));

vi.mock("@tauri-apps/plugin-dialog", () => ({
  open: vi.fn(async () => "/tmp/replacement.txt"),
  save: vi.fn(async () => "/tmp/fixture.txt"),
}));

const mountedApps: App[] = [];

async function flushPage() {
  await nextTick();
  await new Promise((resolve) => setTimeout(resolve, 0));
  await nextTick();
}

async function mountPage() {
  const container = document.createElement("div");
  document.body.append(container);
  const app = createApp(defineComponent({ setup: () => () => h(FileManagerPage) }));
  mountedApps.push(app);
  app.use(createPinia());
  app.use(i18n);
  app.mount(container);
  await flushPage();
  return container;
}

async function mountPageHandle() {
  const container = document.createElement("div");
  document.body.append(container);
  const app = createApp(FileManagerPage);
  mountedApps.push(app);
  app.use(createPinia());
  app.use(i18n);
  const page = app.mount(container) as unknown as {
    openConnectionById: (connectionId: string) => Promise<void>;
  };
  await flushPage();
  return page;
}

function buttonWithTitle(title: string): HTMLButtonElement | undefined {
  return Array.from(document.querySelectorAll<HTMLButtonElement>("button")).find((button) => button.title === title);
}

afterEach(() => {
  for (const app of mountedApps.splice(0)) app.unmount();
  document.body.innerHTML = "";
  listFilePath.mockClear();
  uploadFile.mockClear();
  downloadFile.mockClear();
  deleteFilePath.mockClear();
  copyFilePath.mockClear();
  renameFilePath.mockClear();
});

describe("FileManagerPage browsing", () => {
  it("exposes one layout root so parent visibility directives apply", async () => {
    const container = await mountPage();

    expect(container.childElementCount).toBe(1);
    expect(container.firstElementChild?.classList.contains("flex")).toBe(true);
    expect(container.firstElementChild?.classList.contains("min-h-0")).toBe(true);
    expect(container.firstElementChild?.classList.contains("flex-1")).toBe(true);
  });

  it("opens the connection root, displays metadata, navigates into a directory, and returns to root", async () => {
    await mountPage();
    buttonWithTitle("Open")?.click();
    await flushPage();

    expect(listFilePath).toHaveBeenLastCalledWith("ftp-local", "");
    expect(document.body.textContent).toContain("fixture.txt");
    expect(document.body.textContent).toContain("1.5 KiB");
    expect(document.body.textContent).toContain("/");

    const folderButton = Array.from(document.querySelectorAll<HTMLButtonElement>("button")).find((button) => button.textContent?.includes("folder"));
    folderButton?.click();
    await flushPage();
    expect(listFilePath).toHaveBeenLastCalledWith("ftp-local", "folder");
    expect(document.body.textContent).toContain("/folder");

    buttonWithTitle("Up")?.click();
    await flushPage();
    expect(listFilePath).toHaveBeenLastCalledWith("ftp-local", "");
  });

  it("keeps root as the highest visible path", () => {
    expect(parentFilePath("folder")).toBe("");
    expect(parentFilePath("folder/child")).toBe("folder");
    expect(displayFilePath("")).toBe("/");
  });

  it("reports a stale sidebar connection instead of silently ignoring it", async () => {
    const page = await mountPageHandle();

    await expect(page.openConnectionById("missing")).rejects.toThrow("File connection no longer exists");
  });

  it("requires explicit Replace before retrying an existing upload", async () => {
    uploadFile.mockRejectedValueOnce({ code: "already_exists", message: "redacted" }).mockResolvedValueOnce(11);
    await mountPage();
    buttonWithTitle("Open")?.click();
    await flushPage();

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
    await mountPage();
    buttonWithTitle("Open")?.click();
    await flushPage();

    buttonWithTitle("Delete")?.click();
    await flushPage();
    expect(deleteFilePath).not.toHaveBeenCalled();

    const deleteButton = Array.from(document.querySelectorAll<HTMLButtonElement>("button")).find((button) => button.textContent?.trim() === "Delete");
    deleteButton?.click();
    await flushPage();
    expect(deleteFilePath).toHaveBeenCalledWith("ftp-local", "folder");
  });

  it("shows capability-driven Copy only for files and sends one connection ID", async () => {
    copyFilePath.mockRejectedValueOnce({ code: "already_exists", message: "redacted" }).mockResolvedValueOnce(undefined);
    await mountPage();
    buttonWithTitle("Open")?.click();
    await flushPage();

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
    await mountPage();
    buttonWithTitle("Open")?.click();
    await flushPage();

    buttonWithTitle("Rename or move")?.click();
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
});
