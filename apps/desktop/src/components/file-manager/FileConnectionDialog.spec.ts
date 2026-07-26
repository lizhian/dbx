// @vitest-environment happy-dom

import { createApp, defineComponent, h, nextTick, type App } from "vue";
import { createPinia } from "pinia";
import { afterEach, describe, expect, it, vi } from "vitest";
import i18n from "@/i18n";
import type { FileConnection } from "@/types/fileManager";
import FileConnectionDialog from "./FileConnectionDialog.vue";
import { createFileConnectionDraft, createFtpConnectionDraft, fileConnectionRequestFromDraft, ftpPasswordUpdate, sftpPrivateKeyUpdate } from "./fileConnectionDraft";

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
