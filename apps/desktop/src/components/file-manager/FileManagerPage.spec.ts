// @vitest-environment happy-dom

import { createApp, defineComponent, h, nextTick, type App } from "vue";
import { createPinia } from "pinia";
import { afterEach, describe, expect, it, vi } from "vitest";
import i18n from "@/i18n";
import FileManagerPage from "./FileManagerPage.vue";
import { displayFilePath, parentFilePath } from "./filePath";

const { listFilePath } = vi.hoisted(() => ({
  listFilePath: vi.fn(async (_connectionId: string, path: string) =>
    path
      ? []
      : [
          { path: "folder", name: "folder", kind: "directory", size: 0 },
          { path: "fixture.txt", name: "fixture.txt", kind: "file", size: 1536, modifiedAt: "2026-07-27T00:00:00Z" },
        ],
  ),
}));

vi.mock("@/lib/backend/api", () => ({
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
  saveFileConnection: vi.fn(),
  deleteFileConnection: vi.fn(),
  testFileConnection: vi.fn(),
  listFilePath,
  statFilePath: vi.fn(),
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
}

function buttonWithTitle(title: string): HTMLButtonElement | undefined {
  return Array.from(document.querySelectorAll<HTMLButtonElement>("button")).find((button) => button.title === title);
}

afterEach(() => {
  for (const app of mountedApps.splice(0)) app.unmount();
  document.body.innerHTML = "";
  listFilePath.mockClear();
});

describe("FileManagerPage browsing", () => {
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
});
