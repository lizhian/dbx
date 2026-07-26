import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

const connectionDialogSource = readFileSync(new URL("../../../components/connection/ConnectionDialog.vue", import.meta.url), "utf8");
const appSource = readFileSync(new URL("../../../App.vue", import.meta.url), "utf8");
const sidebarSource = readFileSync(new URL("../../../components/layout/AppSidebar.vue", import.meta.url), "utf8");
const connectionStoreSource = readFileSync(new URL("../../../stores/connectionStore.ts", import.meta.url), "utf8");
const databaseTypesSource = readFileSync(new URL("../../../types/database.ts", import.meta.url), "utf8");

describe("file storage connection entry", () => {
  it("offers file storage as a first-class connection category without adding a DatabaseType", () => {
    expect(connectionDialogSource).toContain('"file-storage"');
    expect(connectionDialogSource).toContain('emit("create-file-connection", implementation)');
    for (const protocol of ["ftp", "sftp", "s3", "webdav", "webhdfs", "hdfs-native"]) {
      expect(connectionDialogSource).toContain(`value: "${protocol}"`);
    }
    expect(appSource).toContain('@create-file-connection="openNewFileConnection"');
    expect(appSource).toContain("pendingFileManagerAction");
    expect(databaseTypesSource).not.toContain('| "ftp"');
  });

  it("renders file connections in the shared connection tree and layout groups", () => {
    expect(sidebarSource).toContain("useFileConnectionStore");
    expect(sidebarSource).toContain("{ deep: true, immediate: true }");
    expect(sidebarSource).toContain("moveFileConnectionToGroup");
    expect(connectionStoreSource).toContain("syncFileConnections");
    expect(connectionStoreSource).toContain("moveFileConnectionToGroup");
    expect(databaseTypesSource).toContain('{ type: "file-connection"; id: string }');
  });
});
