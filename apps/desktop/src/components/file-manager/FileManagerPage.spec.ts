// @vitest-environment happy-dom

import { createApp, nextTick, type App } from "vue";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import i18n from "@/i18n";

const api = vi.hoisted(() => ({
  listFileConnections: vi.fn(),
  listFileEntries: vi.fn(),
  listFileEntriesNext: vi.fn(),
  closeFileListCursor: vi.fn(),
  statFileEntry: vi.fn(),
  saveFileConnection: vi.fn(),
  deleteFileConnection: vi.fn(),
  testFileConnection: vi.fn(),
}));

vi.mock("@/lib/backend/api", () => api);
vi.mock("@/composables/useToast", () => ({
  useToast: () => ({ toast: vi.fn() }),
}));

import FileManagerPage from "@/components/file-manager/FileManagerPage.vue";

const mountedApps: App[] = [];
const connection = (id: string, name: string) => ({
  id,
  name,
  config: {
    type: "ftp" as const,
    endpoint: "ftp://localhost:21",
    root: "/",
    username: "dbx",
  },
  revision: 1,
  createdAt: "2026-07-24T00:00:00Z",
  updatedAt: "2026-07-24T00:00:00Z",
  hasPassword: true,
});
const directory = {
  path: "docs",
  name: "docs",
  kind: "directory" as const,
  size: 0,
  lastModified: "2026-07-24T00:00:00Z",
};
const file = {
  path: "notes.txt",
  name: "notes.txt",
  kind: "file" as const,
  size: 42,
  lastModified: "2026-07-24T00:00:00Z",
};

async function flush() {
  await nextTick();
  await new Promise((resolve) => setTimeout(resolve, 0));
  await nextTick();
}

async function mountPage() {
  const container = document.createElement("div");
  document.body.append(container);
  const app = createApp(FileManagerPage);
  mountedApps.push(app);
  app.use(i18n);
  app.mount(container);
  await flush();
}

function buttonWithText(value: string): HTMLButtonElement {
  const button = Array.from(document.querySelectorAll("button")).find((candidate) => candidate.textContent?.trim().includes(value));
  if (!button) throw new Error(`Button not found: ${value}`);
  return button;
}

beforeEach(() => {
  vi.clearAllMocks();
  i18n.global.locale.value = "en";
  api.listFileConnections.mockResolvedValue([connection("ftp-1", "Primary"), connection("ftp-2", "Secondary")]);
  api.closeFileListCursor.mockResolvedValue(undefined);
  api.statFileEntry.mockResolvedValue({
    ...file,
    etag: '"fixture-etag"',
    contentType: "text/plain",
    userMetadata: { owner: "dbx" },
  });
});

afterEach(() => {
  for (const app of mountedApps.splice(0)) app.unmount();
  document.body.innerHTML = "";
});

describe("FileManagerPage paginated directory browsing", () => {
  it("closes the current cursor before refresh, connection switch, and directory switch", async () => {
    api.listFileEntries
      .mockResolvedValueOnce({ entries: [directory], cursor: "cursor-root-1" })
      .mockResolvedValueOnce({ entries: [directory], cursor: "cursor-root-2" })
      .mockResolvedValueOnce({ entries: [directory], cursor: "cursor-secondary" })
      .mockResolvedValueOnce({ entries: [file], cursor: "cursor-docs" });
    await mountPage();

    (document.querySelector('[aria-label="Refresh"]') as HTMLButtonElement).click();
    await flush();
    expect(api.closeFileListCursor).toHaveBeenNthCalledWith(1, "ftp-1", "cursor-root-1");

    buttonWithText("Secondary").click();
    await flush();
    expect(api.closeFileListCursor).toHaveBeenNthCalledWith(2, "ftp-1", "cursor-root-2");

    const row = Array.from(document.querySelectorAll("tr")).find((candidate) => candidate.textContent?.includes("docs"));
    row?.dispatchEvent(new MouseEvent("dblclick", { bubbles: true }));
    await flush();
    expect(api.closeFileListCursor).toHaveBeenNthCalledWith(3, "ftp-2", "cursor-secondary");
    expect(api.listFileEntries).toHaveBeenLastCalledWith("ftp-2", "docs", { pageSize: 200 });
  });

  it("appends continuation pages and surfaces CursorExpired without silently restarting", async () => {
    api.listFileEntries.mockResolvedValue({ entries: [file], cursor: "cursor-1" });
    api.listFileEntriesNext
      .mockResolvedValueOnce({
        entries: [{ ...file, path: "second.txt", name: "second.txt" }],
        cursor: "cursor-2",
      })
      .mockRejectedValueOnce(new Error("CursorExpired: directory listing expired; refresh required"));
    await mountPage();

    buttonWithText("Load more").click();
    await flush();
    expect(document.body.textContent).toContain("second.txt");
    expect(api.listFileEntriesNext).toHaveBeenCalledWith("ftp-1", "cursor-1", "", { pageSize: 200 });

    buttonWithText("Load more").click();
    await flush();
    expect(document.querySelector('[role="alert"]')?.textContent).toContain("directory view expired");
    expect(api.listFileEntries).toHaveBeenCalledTimes(1);
  });

  it("loads backend stat metadata when an entry is selected", async () => {
    api.listFileEntries.mockResolvedValue({ entries: [file], cursor: null });
    await mountPage();

    const row = Array.from(document.querySelectorAll("tr")).find((candidate) => candidate.textContent?.includes("notes.txt"));
    row?.click();
    await flush();

    expect(api.statFileEntry).toHaveBeenCalledWith("ftp-1", "notes.txt");
    const metadata = document.querySelector("[data-file-manager-metadata]");
    expect(metadata?.textContent).toContain('"fixture-etag"');
    expect(metadata?.textContent).toContain("text/plain");
    expect(metadata?.textContent).toContain("owner");
  });

  it("closes a cursor returned by an initial list request after the view changed", async () => {
    let resolveFirst!: (page: { entries: (typeof file)[]; cursor: string }) => void;
    const firstPage = new Promise<{ entries: (typeof file)[]; cursor: string }>((resolve) => {
      resolveFirst = resolve;
    });
    api.listFileEntries.mockReturnValueOnce(firstPage).mockResolvedValueOnce({ entries: [file], cursor: "secondary-cursor" });
    await mountPage();

    buttonWithText("Secondary").click();
    await flush();
    resolveFirst({ entries: [file], cursor: "stale-primary-cursor" });
    await flush();

    expect(api.closeFileListCursor).toHaveBeenCalledWith("ftp-1", "stale-primary-cursor");
    expect(document.body.textContent).toContain("Secondary");
  });
});
