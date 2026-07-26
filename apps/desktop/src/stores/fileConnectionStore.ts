import { defineStore } from "pinia";
import { ref } from "vue";
import * as api from "@/lib/backend/api";
import type { FileConnection, SaveFileConnectionRequest, TestFileConnectionRequest } from "@/types/fileManager";

export const useFileConnectionStore = defineStore("fileConnections", () => {
  const connections = ref<FileConnection[]>([]);
  const loaded = ref(false);
  const loading = ref(false);
  let loadPromise: Promise<void> | null = null;

  function load(force = false): Promise<void> {
    if (loaded.value && !force) return Promise.resolve();
    if (loadPromise) return loadPromise;
    loading.value = true;
    loadPromise = api
      .listFileConnections()
      .then((saved) => {
        connections.value = saved;
        loaded.value = true;
      })
      .finally(() => {
        loading.value = false;
        loadPromise = null;
      });
    return loadPromise;
  }

  async function save(request: SaveFileConnectionRequest) {
    const saved = await api.saveFileConnection(request);
    const index = connections.value.findIndex((connection) => connection.id === saved.id);
    if (index < 0) connections.value.push(saved);
    else connections.value[index] = saved;
    connections.value.sort((left, right) => left.name.localeCompare(right.name));
    loaded.value = true;
    return saved;
  }

  async function remove(id: string) {
    await api.deleteFileConnection(id);
    connections.value = connections.value.filter((connection) => connection.id !== id);
  }

  async function test(request: TestFileConnectionRequest) {
    await api.testFileConnection(request);
  }

  return { connections, loaded, loading, load, save, remove, test };
});
