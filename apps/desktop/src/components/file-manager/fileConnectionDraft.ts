import type { FileConnection, FileProtocol, FtpFileConnectionConfig, NativeHdfsFileConnectionConfig, S3FileConnectionConfig, SaveFileConnectionRequest, SecretUpdate, SftpFileConnectionConfig, WebdavFileConnectionConfig, WebhdfsFileConnectionConfig } from "@/types/fileManager";

export type SftpAuthenticationMethod = "ssh_config" | "ssh_agent" | "private_key";
export type WebdavAuthenticationMethod = "basic" | "bearer";
export type HdfsImplementation = "webhdfs" | "native";
export type SupportedFileProtocol = Extract<FileProtocol, "ftp" | "sftp" | "s3" | "webdav" | "hdfs">;

export interface FileConnectionDraft {
  id: string;
  name: string;
  protocol: SupportedFileProtocol;
  endpoint: string;
  port: number;
  root: string;
  username: string;
  password: string;
  clearPassword: boolean;
  authentication: SftpAuthenticationMethod;
  privateKey: string;
  clearPrivateKey: boolean;
  region: string;
  bucket: string;
  pathStyle: boolean;
  accessKey: string;
  clearAccessKey: boolean;
  secretKey: string;
  clearSecretKey: boolean;
  sessionToken: string;
  clearSessionToken: boolean;
  webdavAuthentication: WebdavAuthenticationMethod;
  bearerToken: string;
  clearBearerToken: boolean;
  hdfsImplementation: HdfsImplementation;
  simpleUser: string;
  useDelegationToken: boolean;
  delegationToken: string;
  clearDelegationToken: boolean;
  nameNodeUri: string;
  hadoopConfigDirectory: string;
}

export type FtpConnectionDraft = FileConnectionDraft;

function emptyDraft(connection?: FileConnection): FileConnectionDraft {
  return {
    id: connection?.id ?? crypto.randomUUID(),
    name: connection?.name ?? "",
    protocol: "ftp",
    endpoint: "127.0.0.1",
    port: 21,
    root: "/",
    username: "",
    password: "",
    clearPassword: false,
    authentication: "ssh_config",
    privateKey: "",
    clearPrivateKey: false,
    region: "us-east-1",
    bucket: "",
    pathStyle: true,
    accessKey: "",
    clearAccessKey: false,
    secretKey: "",
    clearSecretKey: false,
    sessionToken: "",
    clearSessionToken: false,
    webdavAuthentication: "basic",
    bearerToken: "",
    clearBearerToken: false,
    hdfsImplementation: "webhdfs",
    simpleUser: "dbx",
    useDelegationToken: false,
    delegationToken: "",
    clearDelegationToken: false,
    nameNodeUri: "hdfs://127.0.0.1:19000",
    hadoopConfigDirectory: "",
  };
}

export function createFileConnectionDraft(connection?: FileConnection): FileConnectionDraft {
  const draft = emptyDraft(connection);
  const config = connection?.config;
  if (config?.protocol === "s3") {
    return {
      ...draft,
      protocol: "s3",
      endpoint: config.endpoint,
      root: config.root,
      region: config.region,
      bucket: config.bucket,
      pathStyle: config.pathStyle,
    };
  }
  if (config?.protocol === "webdav") {
    return {
      ...draft,
      protocol: "webdav",
      endpoint: config.endpoint,
      root: config.root,
      username: config.authentication.method === "basic" ? config.authentication.username : "",
      webdavAuthentication: config.authentication.method,
    };
  }
  if (config?.protocol === "hdfs" && config.implementation === "webhdfs") {
    return {
      ...draft,
      protocol: "hdfs",
      endpoint: config.endpoint,
      root: config.root,
      hdfsImplementation: "webhdfs",
      simpleUser: config.simpleUser,
      useDelegationToken: config.useDelegationToken,
    };
  }
  if (config?.protocol === "hdfs" && config.implementation === "native") {
    return {
      ...draft,
      protocol: "hdfs",
      root: config.root,
      hdfsImplementation: "native",
      nameNodeUri: config.nameNodeUri,
      hadoopConfigDirectory: config.hadoopConfigDirectory,
    };
  }
  if (config?.protocol === "sftp") {
    return {
      ...draft,
      protocol: "sftp",
      endpoint: config.endpoint,
      port: config.port,
      root: config.root,
      username: config.username,
      authentication: config.authentication.method,
    };
  }
  const ftp = config?.protocol === "ftp" ? config : undefined;
  return {
    ...draft,
    protocol: "ftp",
    endpoint: ftp?.endpoint ?? "127.0.0.1",
    port: ftp?.port ?? 21,
    root: ftp?.root ?? "/",
    username: ftp?.username ?? "",
  };
}

export function createFtpConnectionDraft(connection?: FileConnection): FtpConnectionDraft {
  return createFileConnectionDraft(connection?.config.protocol === "ftp" ? connection : undefined);
}

export function createProtocolDraft(protocol: SupportedFileProtocol, current: Pick<FileConnectionDraft, "id" | "name">): FileConnectionDraft {
  const draft = emptyDraft();
  draft.id = current.id;
  draft.name = current.name;
  draft.protocol = protocol;
  if (protocol === "sftp") {
    draft.port = 22;
  } else if (protocol === "s3") {
    draft.endpoint = "http://127.0.0.1:9000";
  } else if (protocol === "webdav") {
    draft.endpoint = "http://127.0.0.1:8080";
  } else if (protocol === "hdfs") {
    draft.endpoint = "http://127.0.0.1:9870";
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

export function s3AccessKeyUpdate(draft: Pick<FileConnectionDraft, "accessKey" | "clearAccessKey">): SecretUpdate {
  return secretUpdate(draft.accessKey, draft.clearAccessKey);
}

export function s3SecretKeyUpdate(draft: Pick<FileConnectionDraft, "secretKey" | "clearSecretKey">): SecretUpdate {
  return secretUpdate(draft.secretKey, draft.clearSecretKey);
}

export function s3SessionTokenUpdate(draft: Pick<FileConnectionDraft, "sessionToken" | "clearSessionToken">): SecretUpdate {
  return secretUpdate(draft.sessionToken, draft.clearSessionToken);
}

export function webdavBearerTokenUpdate(draft: Pick<FileConnectionDraft, "bearerToken" | "clearBearerToken">): SecretUpdate {
  return secretUpdate(draft.bearerToken, draft.clearBearerToken);
}

export function hdfsDelegationTokenUpdate(draft: Pick<FileConnectionDraft, "delegationToken" | "clearDelegationToken">): SecretUpdate {
  return secretUpdate(draft.delegationToken, draft.clearDelegationToken);
}

export function fileConnectionRequestFromDraft(draft: FileConnectionDraft): SaveFileConnectionRequest {
  if (draft.protocol === "s3") {
    const config: S3FileConnectionConfig = {
      protocol: "s3",
      endpoint: draft.endpoint.trim(),
      region: draft.region.trim(),
      bucket: draft.bucket.trim(),
      root: draft.root.trim(),
      pathStyle: draft.pathStyle,
    };
    return {
      id: draft.id,
      name: draft.name.trim(),
      config,
      secrets: {
        accessKey: s3AccessKeyUpdate(draft),
        secretKey: s3SecretKeyUpdate(draft),
        sessionToken: s3SessionTokenUpdate(draft),
      },
    };
  }
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
  if (draft.protocol === "webdav") {
    const config: WebdavFileConnectionConfig = {
      protocol: "webdav",
      endpoint: draft.endpoint.trim(),
      root: draft.root.trim(),
      authentication: draft.webdavAuthentication === "basic" ? { method: "basic", username: draft.username.trim() } : { method: "bearer" },
    };
    return {
      id: draft.id,
      name: draft.name.trim(),
      config,
      secrets: {
        password: draft.webdavAuthentication === "basic" ? ftpPasswordUpdate(draft) : { action: "clear" },
        bearerToken: draft.webdavAuthentication === "bearer" ? webdavBearerTokenUpdate(draft) : { action: "clear" },
      },
    };
  }
  if (draft.protocol === "hdfs") {
    if (draft.hdfsImplementation === "native") {
      const config: NativeHdfsFileConnectionConfig = {
        protocol: "hdfs",
        implementation: "native",
        nameNodeUri: draft.nameNodeUri.trim(),
        root: draft.root.trim(),
        hadoopConfigDirectory: draft.hadoopConfigDirectory.trim(),
      };
      return {
        id: draft.id,
        name: draft.name.trim(),
        config,
        secrets: { delegationToken: { action: "clear" } },
      };
    }
    const config: WebhdfsFileConnectionConfig = {
      protocol: "hdfs",
      implementation: "webhdfs",
      endpoint: draft.endpoint.trim(),
      root: draft.root.trim(),
      simpleUser: draft.useDelegationToken ? "" : draft.simpleUser.trim(),
      useDelegationToken: draft.useDelegationToken,
    };
    return {
      id: draft.id,
      name: draft.name.trim(),
      config,
      secrets: {
        delegationToken: draft.useDelegationToken ? hdfsDelegationTokenUpdate(draft) : { action: "clear" },
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
