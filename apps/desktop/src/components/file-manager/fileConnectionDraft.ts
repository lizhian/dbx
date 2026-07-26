import type { FileConnection, FileProtocol, FtpFileConnectionConfig, SaveFileConnectionRequest, SecretUpdate, SftpFileConnectionConfig } from "@/types/fileManager";

export type SftpAuthenticationMethod = "ssh_config" | "ssh_agent" | "private_key";

export interface FileConnectionDraft {
  id: string;
  name: string;
  protocol: Extract<FileProtocol, "ftp" | "sftp">;
  endpoint: string;
  port: number;
  root: string;
  username: string;
  password: string;
  clearPassword: boolean;
  authentication: SftpAuthenticationMethod;
  privateKey: string;
  clearPrivateKey: boolean;
}

export type FtpConnectionDraft = FileConnectionDraft;

export function createFileConnectionDraft(connection?: FileConnection): FileConnectionDraft {
  const config = connection?.config;
  if (config?.protocol === "sftp") {
    return {
      id: connection?.id ?? crypto.randomUUID(),
      name: connection?.name ?? "",
      protocol: "sftp",
      endpoint: config.endpoint,
      port: config.port,
      root: config.root,
      username: config.username,
      password: "",
      clearPassword: false,
      authentication: config.authentication.method,
      privateKey: "",
      clearPrivateKey: false,
    };
  }
  const ftp = config?.protocol === "ftp" ? config : undefined;
  return {
    id: connection?.id ?? crypto.randomUUID(),
    name: connection?.name ?? "",
    protocol: "ftp",
    endpoint: ftp?.endpoint ?? "127.0.0.1",
    port: ftp?.port ?? 21,
    root: ftp?.root ?? "/",
    username: ftp?.username ?? "",
    password: "",
    clearPassword: false,
    authentication: "ssh_config",
    privateKey: "",
    clearPrivateKey: false,
  };
}

export function createFtpConnectionDraft(connection?: FileConnection): FtpConnectionDraft {
  return createFileConnectionDraft(connection);
}

export function createProtocolDraft(protocol: Extract<FileProtocol, "ftp" | "sftp">, current: Pick<FileConnectionDraft, "id" | "name">): FileConnectionDraft {
  const draft = createFileConnectionDraft();
  draft.id = current.id;
  draft.name = current.name;
  draft.protocol = protocol;
  if (protocol === "sftp") {
    draft.port = 22;
    draft.root = "/";
    draft.username = "";
  }
  return draft;
}

function secretUpdate(value: string, clear: boolean): SecretUpdate {
  if (clear) return { action: "clear" };
  if (value) return { action: "set", value };
  return { action: "keep" };
}

export function ftpPasswordUpdate(draft: Pick<FileConnectionDraft, "password" | "clearPassword">): SecretUpdate {
  return secretUpdate(draft.password, draft.clearPassword);
}

export function sftpPrivateKeyUpdate(draft: Pick<FileConnectionDraft, "privateKey" | "clearPrivateKey">): SecretUpdate {
  return secretUpdate(draft.privateKey, draft.clearPrivateKey);
}

export function fileConnectionRequestFromDraft(draft: FileConnectionDraft): SaveFileConnectionRequest {
  if (draft.protocol === "sftp") {
    const config: SftpFileConnectionConfig = {
      protocol: "sftp",
      endpoint: draft.endpoint.trim(),
      port: draft.port,
      root: draft.root.trim(),
      username: draft.username.trim(),
      authentication: { method: draft.authentication },
    };
    return {
      id: draft.id,
      name: draft.name.trim(),
      config,
      secrets: {
        privateKey: draft.authentication === "private_key" ? sftpPrivateKeyUpdate(draft) : { action: "clear" },
      },
    };
  }
  return ftpRequestFromDraft(draft);
}

export function ftpRequestFromDraft(draft: FileConnectionDraft): SaveFileConnectionRequest {
  const config: FtpFileConnectionConfig = {
    protocol: "ftp",
    endpoint: draft.endpoint.trim(),
    port: draft.port,
    root: draft.root.trim(),
    username: draft.username.trim(),
  };
  return {
    id: draft.id,
    name: draft.name.trim(),
    config,
    secrets: { password: ftpPasswordUpdate(draft) },
  };
}
