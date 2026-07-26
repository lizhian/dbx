import { createPinia, setActivePinia } from "pinia";
import { beforeEach, describe, expect, it, vi } from "vitest";
import * as api from "@/lib/backend/api";
import { useFileConnectionStore } from "@/stores/fileConnectionStore";

vi.mock("@/lib/backend/api", () => ({
  listFileConnections: vi.fn(),
  saveFileConnection: vi.fn(),
  deleteFileConnection: vi.fn(),
  testFileConnection: vi.fn(),
}));

describe("fileConnectionStore", () => {
  beforeEach(() => {
    setActivePinia(createPinia());
    vi.clearAllMocks();
  });

  it("shares an in-flight initial load with concurrent consumers", async () => {
    let resolveLoad!: (connections: []) => void;
    vi.mocked(api.listFileConnections).mockReturnValue(
      new Promise((resolve) => {
        resolveLoad = resolve;
      }),
    );
    const store = useFileConnectionStore();

    const sidebarLoad = store.load();
    const pageLoad = store.load();
    expect(api.listFileConnections).toHaveBeenCalledTimes(1);
    expect(store.loading).toBe(true);

    resolveLoad([]);
    await Promise.all([sidebarLoad, pageLoad]);
    expect(store.loaded).toBe(true);
    expect(store.loading).toBe(false);
  });
});
