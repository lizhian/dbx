import { defineStore } from "pinia";
import { ref } from "vue";
import * as api from "@/lib/backend/api";
import type { FileConnection, SaveFileConnectionRequest, TestFileConnectionRequest } from "@/types/fileManager";

export const useFileConnectionStore = defineStore("fileConnections", () => {
  const connections = ref<FileConnection[]>([]);
  const loaded = ref(false);
  const loading = ref(false);

  async function load(force = false) {
    if ((loaded.value && !force) || loading.value) return;
    loading.value = true;
    try {
      connections.value = await api.listFileConnections();
      loaded.value = true;
    } finally {
      loading.value = false;
    }
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
