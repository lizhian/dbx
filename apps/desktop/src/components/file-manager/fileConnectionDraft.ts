import type { FileConnection, FtpFileConnectionConfig, SaveFileConnectionRequest, SecretUpdate } from "@/types/fileManager";

export interface FtpConnectionDraft {
  id: string;
  name: string;
  endpoint: string;
  port: number;
  root: string;
  username: string;
  password: string;
  clearPassword: boolean;
}

export function createFtpConnectionDraft(connection?: FileConnection): FtpConnectionDraft {
  const config = connection?.config.protocol === "ftp" ? connection.config : undefined;
  return {
    id: connection?.id ?? crypto.randomUUID(),
    name: connection?.name ?? "",
    endpoint: config?.endpoint ?? "127.0.0.1",
    port: config?.port ?? 21,
    root: config?.root ?? "/",
    username: config?.username ?? "",
    password: "",
    clearPassword: false,
  };
}

export function ftpPasswordUpdate(draft: Pick<FtpConnectionDraft, "password" | "clearPassword">): SecretUpdate {
  if (draft.clearPassword) return { action: "clear" };
  if (draft.password) return { action: "set", value: draft.password };
  return { action: "keep" };
}

export function ftpRequestFromDraft(draft: FtpConnectionDraft): SaveFileConnectionRequest {
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
