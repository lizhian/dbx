import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

const connectionDialogSource = readFileSync(new URL("../../../components/connection/ConnectionDialog.vue", import.meta.url), "utf8");
const fileConnectionFieldsSource = readFileSync(new URL("../../../components/connection/FileConnectionFields.vue", import.meta.url), "utf8");
const connectionStoreSource = readFileSync(new URL("../../../stores/connectionStore.ts", import.meta.url), "utf8");
const databaseTypesSource = readFileSync(new URL("../../../types/database.ts", import.meta.url), "utf8");
const databaseIconSource = readFileSync(new URL("../../../components/icons/DatabaseIcon.vue", import.meta.url), "utf8");
const fileManagerPageSource = readFileSync(new URL("../../../components/file-manager/FileManagerPage.vue", import.meta.url), "utf8");
const appSource = readFileSync(new URL("../../../App.vue", import.meta.url), "utf8");
const appToolbarSource = readFileSync(new URL("../../../components/layout/AppToolbar.vue", import.meta.url), "utf8");
const sidebarConnectionMutationSource = readFileSync(new URL("../../../composables/useSidebarConnectionMutationRuntime.ts", import.meta.url), "utf8");
const databaseFeatureSupportSource = readFileSync(new URL("../../database/databaseFeatureSupport.ts", import.meta.url), "utf8");
const httpBackendSource = readFileSync(new URL("../../backend/http.ts", import.meta.url), "utf8");
const sidebarLayoutSource = readFileSync(new URL("../../sidebar/sidebarLayout.ts", import.meta.url), "utf8");

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
    expect(fileConnectionFieldsSource).toContain('id="file-connection-private-key" v-model="draft.privateKey"');
    expect(connectionDialogSource).toContain("applyFileConnectionProjection(config, request.config)");
    expect(connectionDialogSource).toContain("await store.addConnection(config, undefined, fileSecrets)");
    expect(connectionDialogSource).toContain("await store.updateConnection(updated, fileSecrets)");
    expect(connectionStoreSource).toContain("fileSecretUpdates?: FileSecretUpdates");
    expect(connectionStoreSource).toContain("await api.saveConnections(nextConnections, fileSecretUpdates)");
    expect(connectionDialogSource).toContain("form.db_type === 'mongodb' || form.db_type === 'file'");
    expect(connectionDialogSource).toContain('!isSingleDatabase(form.value.db_type) && form.value.db_type !== "file"');
    expect(connectionDialogSource).toContain("v-if=\"form.db_type !== 'file'\"");
    expect(connectionDialogSource).toContain('(isSingleDatabase(config.db_type) || config.db_type === "file")');
  });

  it("tests and exports file credentials through the generic connection lifecycle", () => {
    expect(connectionDialogSource).toContain("testConnectionWithTimeout(config, runId, request.secrets)");
    expect(connectionDialogSource).not.toContain("api.testFileConnection");
    expect(connectionStoreSource).toContain("await api.exportFileConnectionSecrets(fileConnectionIds)");
    expect(connectionStoreSource).toContain("fileSecretUpdatesFromExport(importedFileSecrets[importedId])");
    expect(connectionStoreSource).toContain("await addConnection(normalized, undefined, fileSecretUpdates)");
    expect(connectionStoreSource).toContain("await api.exportFileConnectionSecrets([connectionId])");
    expect(sidebarConnectionMutationSource).toContain("await connectionStore.fileSecretUpdatesForConnection(target.connectionId)");
    expect(httpBackendSource).toContain('configs.some((config) => config.db_type === "file")');
  });

  it("uses one connection store and one ordinary sidebar entry type", () => {
    expect(fileManagerPageSource).toContain('connection.db_type === "file"');
    expect(fileManagerPageSource).not.toContain("useFileConnectionStore");
    expect(connectionStoreSource).not.toContain("syncFileConnections");
    expect(databaseTypesSource).not.toContain('| "file-connection"');
    expect(sidebarLayoutSource).not.toContain('"file-connection"');
    expect(databaseFeatureSupportSource).toContain('dbType !== "nacos" && dbType !== "file"');
    expect(sidebarConnectionMutationSource).toContain('databaseType !== "file"');
    expect(fileManagerPageSource).toContain("executeWithProductionContextGuard");
  });

  it("has no standalone file manager entry or connection index", () => {
    expect(appToolbarSource).not.toContain('"open-file-manager"');
    expect(appToolbarSource).not.toContain("showFileManager");
    expect(appSource).not.toContain("@open-file-manager");
    expect(appSource).toContain('@open-file-connection="openFileConnection"');
    expect(fileManagerPageSource).not.toContain("fileManager.newConnection");
    expect(fileManagerPageSource).not.toContain("removeConnection");
    expect(fileManagerPageSource).toContain("defineExpose({ openConnectionById })");
  });
});
