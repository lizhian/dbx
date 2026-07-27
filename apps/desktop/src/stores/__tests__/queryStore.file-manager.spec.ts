import { createPinia, setActivePinia } from "pinia";
import { beforeEach, describe, expect, it, vi } from "vitest";

function installLocalStorage() {
  const data = new Map<string, string>();
  vi.stubGlobal("localStorage", {
    getItem: vi.fn((key: string) => data.get(key) ?? null),
    setItem: vi.fn((key: string, value: string) => data.set(key, value)),
    removeItem: vi.fn((key: string) => data.delete(key)),
  });
}

describe("queryStore file manager tabs", () => {
  beforeEach(() => {
    vi.resetModules();
    vi.unstubAllGlobals();
    installLocalStorage();
    setActivePinia(createPinia());
  });

  it("deduplicates the same connection and keeps different connections open", async () => {
    const { useQueryStore } = await import("@/stores/queryStore");
    const store = useQueryStore();

    const first = store.openFileManagerTab("file-1", "SFTP Files");
    const duplicate = store.openFileManagerTab("file-1", "Renamed SFTP Files");
    const second = store.openFileManagerTab("file-2", "S3 Files");

    expect(duplicate).toBe(first);
    expect(second).not.toBe(first);
    expect(store.tabs.filter((tab) => tab.mode === "file-manager")).toHaveLength(2);
    expect(store.tabs.find((tab) => tab.id === first)?.title).toBe("Renamed SFTP Files");
    expect(store.activeTabId).toBe(second);
  });

  it("closes a file manager tab without an unsaved SQL prompt", async () => {
    const { useSettingsStore } = await import("@/stores/settingsStore");
    const { useQueryStore } = await import("@/stores/queryStore");
    useSettingsStore().editorSettings.confirmUnsavedSqlClose = true;
    const store = useQueryStore();
    const tabId = store.openFileManagerTab("file-1", "SFTP Files");
    store.tabs.find((tab) => tab.id === tabId)!.sql = "non-sql transient state";

    store.closeTab(tabId);

    expect(store.showCloseConfirm).toBe(false);
    expect(store.tabs.some((tab) => tab.id === tabId)).toBe(false);
  });
});
