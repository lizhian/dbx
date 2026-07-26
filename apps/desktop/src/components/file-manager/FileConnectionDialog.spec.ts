// @vitest-environment happy-dom

import { createApp, defineComponent, h, nextTick, type App } from "vue";
import { createPinia } from "pinia";
import { afterEach, describe, expect, it, vi } from "vitest";
import i18n from "@/i18n";
import type { FileConnection } from "@/types/fileManager";
import FileConnectionDialog from "./FileConnectionDialog.vue";
import { createFileConnectionDraft, createFtpConnectionDraft, fileConnectionRequestFromDraft, ftpPasswordUpdate, hdfsDelegationTokenUpdate, s3AccessKeyUpdate, s3SecretKeyUpdate, s3SessionTokenUpdate, sftpPrivateKeyUpdate, webdavBearerTokenUpdate } from "./fileConnectionDraft";

vi.mock("@/lib/backend/api", () => ({
  listFileConnections: vi.fn(async () => []),
  saveFileConnection: vi.fn(),
  deleteFileConnection: vi.fn(),
  testFileConnection: vi.fn(),
}));

vi.mock("@tauri-apps/plugin-dialog", () => ({
  open: vi.fn(async () => "/tmp/id_ed25519"),
}));

const mountedApps: App[] = [];

const connection: FileConnection = {
  id: "ftp-local",
  name: "Local FTP",
  config: {
    protocol: "ftp",
    endpoint: "127.0.0.1",
    port: 2121,
    root: "/ftp/dbx/",
    username: "dbx",
  },
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
};

const sftpConnection: FileConnection = {
  id: "sftp-local",
  name: "Local SFTP",
  config: {
    protocol: "sftp",
    endpoint: "127.0.0.1",
    port: 2222,
    root: "/config",
    username: "dbx",
    authentication: { method: "private_key" },
  },
  capabilities: {
    read: true,
    write: true,
    stat: true,
    list: true,
    delete: true,
    copy: true,
    rename: true,
    nativeCopy: true,
    nativeRename: true,
    atomicRename: true,
    atomicNoClobber: false,
    copyMode: "native",
    renameMode: "native",
  },
  secretStatus: {
    password: false,
    privateKey: true,
    accessKey: false,
    secretKey: false,
    sessionToken: false,
    bearerToken: false,
    delegationToken: false,
  },
};

const s3Connection: FileConnection = {
  id: "s3-local",
  name: "Local S3",
  config: {
    protocol: "s3",
    endpoint: "http://127.0.0.1:9000",
    region: "us-east-1",
    bucket: "dbx",
    root: "/root/",
    pathStyle: true,
  },
  capabilities: {
    read: true,
    write: true,
    stat: true,
    list: true,
    delete: true,
    copy: true,
    rename: true,
    nativeCopy: true,
    nativeRename: false,
    atomicRename: false,
    atomicNoClobber: false,
    copyMode: "native",
    renameMode: "copy_delete",
  },
  secretStatus: {
    password: false,
    privateKey: false,
    accessKey: true,
    secretKey: true,
    sessionToken: true,
    bearerToken: false,
    delegationToken: false,
  },
};

const webdavBasicConnection: FileConnection = {
  id: "webdav-basic",
  name: "Local WebDAV",
  config: {
    protocol: "webdav",
    endpoint: "http://127.0.0.1:8080",
    root: "/",
    authentication: { method: "basic", username: "dbx" },
  },
  capabilities: {
    read: true,
    write: true,
    stat: true,
    list: true,
    delete: true,
    copy: true,
    rename: true,
    nativeCopy: true,
    nativeRename: true,
    atomicRename: true,
    atomicNoClobber: false,
    copyMode: "native",
    renameMode: "native",
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
};

const webdavBearerConnection: FileConnection = {
  ...webdavBasicConnection,
  id: "webdav-bearer",
  config: {
    protocol: "webdav",
    endpoint: "https://dav.example.test",
    root: "/files/",
    authentication: { method: "bearer" },
  },
  secretStatus: {
    ...webdavBasicConnection.secretStatus,
    password: false,
    bearerToken: true,
  },
};

const webhdfsConnection: FileConnection = {
  id: "webhdfs-local",
  name: "Local HDFS",
  config: {
    protocol: "hdfs",
    implementation: "webhdfs",
    endpoint: "http://127.0.0.1:9870",
    root: "/",
    simpleUser: "dbx",
    useDelegationToken: false,
  },
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
    password: false,
    privateKey: false,
    accessKey: false,
    secretKey: false,
    sessionToken: false,
    bearerToken: false,
    delegationToken: false,
  },
};

async function mountDialog(selectedConnection: FileConnection = connection) {
  const container = document.createElement("div");
  document.body.append(container);
  const app = createApp(
    defineComponent({
      setup: () => () => h(FileConnectionDialog, { open: true, connection: selectedConnection }),
    }),
  );
  mountedApps.push(app);
  app.use(createPinia());
  app.use(i18n);
  app.mount(container);
  await nextTick();
  await new Promise((resolve) => setTimeout(resolve, 0));
}

afterEach(() => {
  for (const app of mountedApps.splice(0)) app.unmount();
  document.body.innerHTML = "";
});

describe("FileConnectionDialog FTP lifecycle form", () => {
  it("shows only FTP fields, keeps the saved password hidden, and warns about plaintext transport", async () => {
    await mountDialog();

    expect(document.querySelector<HTMLInputElement>("#file-connection-endpoint")?.value).toBe("127.0.0.1");
    expect(document.querySelector<HTMLInputElement>("#file-connection-port")?.value).toBe("2121");
    expect(document.querySelector<HTMLInputElement>("#file-connection-root")?.value).toBe("/ftp/dbx/");
    expect(document.querySelector<HTMLInputElement>('input[type="password"]')?.value).toBe("");
    expect(document.body.textContent).toContain("without encryption");
    expect(document.body.textContent).not.toContain("Access key");
    expect(document.body.textContent).not.toContain("Bucket");
  });

  it("models keep, set, and explicit clear as distinct secret updates", () => {
    const draft = createFtpConnectionDraft(connection);
    expect(ftpPasswordUpdate(draft)).toEqual({ action: "keep" });

    draft.password = "replacement";
    expect(ftpPasswordUpdate(draft)).toEqual({ action: "set", value: "replacement" });

    draft.clearPassword = true;
    expect(ftpPasswordUpdate(draft)).toEqual({ action: "clear" });
  });
});

describe("FileConnectionDialog SFTP lifecycle form", () => {
  it("shows only supported authentication choices and keeps the private key path secret", async () => {
    await mountDialog(sftpConnection);

    expect(document.querySelector<HTMLInputElement>("#file-connection-endpoint")?.value).toBe("127.0.0.1");
    expect(document.querySelector<HTMLInputElement>("#file-connection-port")?.value).toBe("2222");
    expect(document.querySelector<HTMLInputElement>("#file-connection-root")?.value).toBe("/config");
    expect(document.querySelector<HTMLInputElement>('input[type="password"]')).toBeNull();
    expect(document.body.textContent).toContain("Password authentication");
    expect(document.body.textContent).toContain("Windows");

    const privateKey = document.querySelector<HTMLInputElement>("#file-connection-private-key");
    expect(privateKey?.value).toBe("");
    expect(privateKey?.placeholder).toContain("remains unchanged");
    const chooseButton = Array.from(document.querySelectorAll<HTMLButtonElement>("button")).find((button) => button.title === "Select private key file");
    chooseButton?.click();
    await nextTick();
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(privateKey?.value).toBe("/tmp/id_ed25519");
  });

  it("models private key Set, Keep, Clear, and auth switching without putting the path in config", () => {
    const draft = createFileConnectionDraft(sftpConnection);
    expect(sftpPrivateKeyUpdate(draft)).toEqual({ action: "keep" });
    expect(fileConnectionRequestFromDraft(draft).secrets?.privateKey).toEqual({ action: "keep" });

    draft.privateKey = "/tmp/id_ed25519";
    expect(sftpPrivateKeyUpdate(draft)).toEqual({ action: "set", value: "/tmp/id_ed25519" });
    const request = fileConnectionRequestFromDraft(draft);
    expect(request.secrets?.privateKey).toEqual({ action: "set", value: "/tmp/id_ed25519" });
    expect(JSON.stringify(request.config)).not.toContain("/tmp/id_ed25519");

    draft.clearPrivateKey = true;
    expect(sftpPrivateKeyUpdate(draft)).toEqual({ action: "clear" });
    draft.authentication = "ssh_agent";
    expect(fileConnectionRequestFromDraft(draft).secrets?.privateKey).toEqual({ action: "clear" });
  });
});

describe("FileConnectionDialog S3 lifecycle form", () => {
  it("shows only S3 configuration and keeps all three saved credentials hidden", async () => {
    await mountDialog(s3Connection);

    expect(document.querySelector<HTMLInputElement>("#file-connection-endpoint")?.value).toBe("http://127.0.0.1:9000");
    expect(document.querySelector<HTMLInputElement>("#file-connection-region")?.value).toBe("us-east-1");
    expect(document.querySelector<HTMLInputElement>("#file-connection-bucket")?.value).toBe("dbx");
    expect(document.querySelector("#file-connection-port")).toBeNull();
    expect(document.querySelector("#file-connection-username")).toBeNull();
    expect(document.querySelector("#file-connection-password")).toBeNull();
    expect(document.querySelector<HTMLInputElement>("input#file-connection-access-key")?.value).toBe("");
    expect(document.querySelector<HTMLInputElement>("input#file-connection-secret-key")?.value).toBe("");
    expect(document.querySelector<HTMLInputElement>("input#file-connection-session-token")?.value).toBe("");
    expect(document.body.textContent).not.toContain("SSH agent");
  });

  it("round-trips path-style and models Set, Keep, and Clear for S3 credentials", () => {
    const draft = createFileConnectionDraft(s3Connection);
    expect(draft.pathStyle).toBe(true);
    expect(s3AccessKeyUpdate(draft)).toEqual({ action: "keep" });
    expect(s3SecretKeyUpdate(draft)).toEqual({ action: "keep" });
    expect(s3SessionTokenUpdate(draft)).toEqual({ action: "keep" });

    draft.accessKey = "replacement-access";
    draft.secretKey = "replacement-secret";
    draft.clearSessionToken = true;
    const request = fileConnectionRequestFromDraft(draft);
    expect(request.config).toMatchObject({ protocol: "s3", pathStyle: true });
    expect(request.secrets).toEqual({
      accessKey: { action: "set", value: "replacement-access" },
      secretKey: { action: "set", value: "replacement-secret" },
      sessionToken: { action: "clear" },
    });
    const config = JSON.stringify(request.config);
    expect(config).not.toContain("replacement-access");
    expect(config).not.toContain("replacement-secret");
  });
});

describe("FileConnectionDialog WebDAV lifecycle form", () => {
  it("shows only endpoint, root, username, and password for Basic authentication", async () => {
    await mountDialog(webdavBasicConnection);

    expect(document.querySelector<HTMLInputElement>("#file-connection-endpoint")?.value).toBe("http://127.0.0.1:8080");
    expect(document.querySelector<HTMLInputElement>("#file-connection-root")?.value).toBe("/");
    expect(document.querySelector<HTMLInputElement>("#file-connection-username")?.value).toBe("dbx");
    expect(document.querySelector<HTMLInputElement>("input#file-connection-password")?.value).toBe("");
    expect(document.querySelector("#file-connection-port")).toBeNull();
    expect(document.querySelector("#file-connection-region")).toBeNull();
    expect(document.querySelector("#file-connection-bearer-token")).toBeNull();
  });

  it("round-trips Bearer authentication with token Set, Keep, and Clear semantics", async () => {
    await mountDialog(webdavBearerConnection);
    expect(document.querySelector("#file-connection-username")).toBeNull();
    expect(document.querySelector("#file-connection-password")).toBeNull();
    expect(document.querySelector<HTMLInputElement>("input#file-connection-bearer-token")?.value).toBe("");

    const draft = createFileConnectionDraft(webdavBearerConnection);
    expect(webdavBearerTokenUpdate(draft)).toEqual({ action: "keep" });
    draft.bearerToken = "replacement-bearer";
    let request = fileConnectionRequestFromDraft(draft);
    expect(request.config).toEqual({
      protocol: "webdav",
      endpoint: "https://dav.example.test",
      root: "/files/",
      authentication: { method: "bearer" },
    });
    expect(request.secrets).toEqual({
      password: { action: "clear" },
      bearerToken: { action: "set", value: "replacement-bearer" },
    });
    expect(JSON.stringify(request.config)).not.toContain("replacement-bearer");

    draft.bearerToken = "";
    draft.clearBearerToken = true;
    request = fileConnectionRequestFromDraft(draft);
    expect(request.secrets?.bearerToken).toEqual({ action: "clear" });
  });
});

describe("FileConnectionDialog HDFS lifecycle form", () => {
  it("shows the shared HDFS discriminator and only WebHDFS simple-user fields", async () => {
    await mountDialog(webhdfsConnection);

    expect(document.querySelector<HTMLInputElement>("#file-connection-endpoint")?.value).toBe("http://127.0.0.1:9870");
    expect(document.querySelector<HTMLInputElement>("#file-connection-root")?.value).toBe("/");
    expect(document.querySelector<HTMLInputElement>("#file-connection-simple-user")?.value).toBe("dbx");
    expect(document.body.textContent).toContain("WebHDFS");
    expect(document.querySelector("#file-connection-port")).toBeNull();
    expect(document.querySelector("#file-connection-username")).toBeNull();
    expect(document.querySelector("#file-connection-delegation-token")).toBeNull();
  });

  it("models simple user and delegation token as exclusive structured authentication", () => {
    const draft = createFileConnectionDraft(webhdfsConnection);
    let request = fileConnectionRequestFromDraft(draft);
    expect(request.config).toEqual({
      protocol: "hdfs",
      implementation: "webhdfs",
      endpoint: "http://127.0.0.1:9870",
      root: "/",
      simpleUser: "dbx",
      useDelegationToken: false,
    });
    expect(request.secrets?.delegationToken).toEqual({ action: "clear" });

    draft.useDelegationToken = true;
    expect(hdfsDelegationTokenUpdate(draft)).toEqual({ action: "keep" });
    draft.delegationToken = "replacement-delegation";
    request = fileConnectionRequestFromDraft(draft);
    expect(request.config).toMatchObject({
      protocol: "hdfs",
      implementation: "webhdfs",
      simpleUser: "",
      useDelegationToken: true,
    });
    expect(request.secrets?.delegationToken).toEqual({ action: "set", value: "replacement-delegation" });
    expect(JSON.stringify(request.config)).not.toContain("replacement-delegation");

    draft.delegationToken = "";
    draft.clearDelegationToken = true;
    expect(hdfsDelegationTokenUpdate(draft)).toEqual({ action: "clear" });
  });
});
