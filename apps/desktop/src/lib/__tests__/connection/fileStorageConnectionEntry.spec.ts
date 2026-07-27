import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

const connectionDialogSource = readFileSync(new URL("../../../components/connection/ConnectionDialog.vue", import.meta.url), "utf8");
const connectionStoreSource = readFileSync(new URL("../../../stores/connectionStore.ts", import.meta.url), "utf8");
const databaseTypesSource = readFileSync(new URL("../../../types/database.ts", import.meta.url), "utf8");
const databaseIconSource = readFileSync(new URL("../../../components/icons/DatabaseIcon.vue", import.meta.url), "utf8");

describe("file storage connection entry", () => {
  it("offers file protocols as driver profiles under the shared file DatabaseType", () => {
    const registryCategory = connectionDialogSource.indexOf('key: "registry_config"');
    const fileCategory = connectionDialogSource.indexOf('key: "file"');
    expect(registryCategory).toBeGreaterThan(-1);
    expect(fileCategory).toBeGreaterThan(registryCategory);
    expect(databaseTypesSource).toContain('| "file"');
    expect(databaseTypesSource).not.toContain('| "ftp"');
    for (const protocol of ["ftp", "sftp", "s3", "webdav", "webhdfs", "hdfs-native"]) {
      const profileKey = protocol === "hdfs-native" ? `"${protocol}"` : protocol;
      expect(connectionDialogSource).toContain(`${profileKey}: { type: "file"`);
      expect(databaseIconSource).toContain(protocol === "hdfs-native" ? "hdfs_native" : protocol);
    }
  });

  it("saves file config and explicit secret updates through the generic connection store", () => {
    expect(connectionDialogSource).toContain("<FileConnectionFields v-if=\"form.db_type === 'file'\"");
    expect(connectionDialogSource).toContain("applyFileConnectionProjection(config, request.config)");
    expect(connectionDialogSource).toContain("await store.addConnection(config, undefined, fileSecrets)");
    expect(connectionDialogSource).toContain("await store.updateConnection(updated, fileSecrets)");
    expect(connectionStoreSource).toContain("fileSecretUpdates?: FileSecretUpdates");
    expect(connectionStoreSource).toContain("await api.saveConnections(nextConnections, fileSecretUpdates)");
  });
});
