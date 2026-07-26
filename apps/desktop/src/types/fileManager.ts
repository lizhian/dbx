export type FileProtocol = "ftp" | "sftp" | "s3" | "webdav" | "hdfs";

export interface FtpFileConnectionConfig {
  protocol: "ftp";
  endpoint: string;
  port: number;
  root: string;
  username: string;
}

export type FileConnectionConfig =
  | FtpFileConnectionConfig
  | {
      protocol: "sftp";
      endpoint: string;
      port: number;
      root: string;
      username: string;
      authentication: { method: "ssh_config" | "ssh_agent" | "private_key" };
    }
  | {
      protocol: "s3";
      endpoint: string;
      region: string;
      bucket: string;
      root: string;
      pathStyle: boolean;
    }
  | {
      protocol: "webdav";
      endpoint: string;
      root: string;
      authentication: { method: "basic"; username: string } | { method: "bearer" };
    }
  | {
      protocol: "hdfs";
      implementation: "webhdfs";
      endpoint: string;
      root: string;
      simpleUser: string;
      useDelegationToken: boolean;
    }
  | {
      protocol: "hdfs";
      implementation: "native";
      nameNodeUri: string;
      root: string;
      hadoopConfigDirectory: string;
    };

export interface FileCapabilities {
  read: boolean;
  write: boolean;
  stat: boolean;
  list: boolean;
  delete: boolean;
  copy: boolean;
  rename: boolean;
  nativeCopy: boolean;
  nativeRename: boolean;
  atomicRename: boolean;
  atomicNoClobber: boolean;
  copyMode: "native" | "stream_relay";
  renameMode: "native" | "copy_delete";
}

export interface FileSecretStatus {
  password: boolean;
  privateKey: boolean;
  accessKey: boolean;
  secretKey: boolean;
  sessionToken: boolean;
  bearerToken: boolean;
  delegationToken: boolean;
}

export interface FileConnection {
  id: string;
  name: string;
  config: FileConnectionConfig;
  capabilities: FileCapabilities;
  secretStatus: FileSecretStatus;
}

export type SecretUpdate = { action: "keep" } | { action: "set"; value: string } | { action: "clear" };

export interface FileSecretUpdates {
  password?: SecretUpdate;
  privateKey?: SecretUpdate;
  accessKey?: SecretUpdate;
  secretKey?: SecretUpdate;
  sessionToken?: SecretUpdate;
  bearerToken?: SecretUpdate;
  delegationToken?: SecretUpdate;
}

export interface SaveFileConnectionRequest {
  id: string;
  name: string;
  config: FileConnectionConfig;
  secrets?: FileSecretUpdates;
}

export interface TestFileConnectionRequest {
  id?: string;
  config: FileConnectionConfig;
  secrets?: FileSecretUpdates;
}

export interface FileEntry {
  path: string;
  name: string;
  kind: "file" | "directory" | "unknown";
  size: number;
  modifiedAt?: string;
}

export interface FileTransferRequest {
  connectionId: string;
  remotePath: string;
  localPath: string;
  replace?: boolean;
}

export interface FileRemoteOperationRequest {
  connectionId: string;
  sourcePath: string;
  destinationPath: string;
  replace?: boolean;
}
