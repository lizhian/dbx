export type FileProtocol = "ftp" | "sftp" | "s3" | "webdav" | "hdfs";
export type FileConnectionImplementation = "ftp" | "sftp" | "s3" | "webdav" | "webhdfs" | "hdfs-native";

export interface FtpFileConnectionConfig {
  protocol: "ftp";
  endpoint: string;
  port: number;
  root: string;
  username: string;
}

export interface SftpFileConnectionConfig {
  protocol: "sftp";
  endpoint: string;
  port: number;
  root: string;
  username: string;
  authentication: { method: "ssh_config" | "ssh_agent" | "private_key" };
}

export interface S3FileConnectionConfig {
  protocol: "s3";
  endpoint: string;
  region: string;
  bucket: string;
  root: string;
  pathStyle: boolean;
}

export interface WebdavFileConnectionConfig {
  protocol: "webdav";
  endpoint: string;
  root: string;
  authentication: { method: "basic"; username: string } | { method: "bearer" };
}

export interface WebhdfsFileConnectionConfig {
  protocol: "hdfs";
  implementation: "webhdfs";
  endpoint: string;
  root: string;
  simpleUser: string;
  useDelegationToken: boolean;
}

export interface NativeHdfsFileConnectionConfig {
  protocol: "hdfs";
  implementation: "native";
  nameNodeUri: string;
  root: string;
  hadoopConfigDirectory: string;
}

export type HdfsFileConnectionConfig = WebhdfsFileConnectionConfig | NativeHdfsFileConnectionConfig;

export type FileConnectionConfig = FtpFileConnectionConfig | SftpFileConnectionConfig | S3FileConnectionConfig | WebdavFileConnectionConfig | HdfsFileConnectionConfig;

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

export interface FileSecretExportValues {
  password?: string;
  private_key?: string;
  access_key?: string;
  secret_key?: string;
  session_token?: string;
  bearer_token?: string;
  delegation_token?: string;
}

export interface SaveFileConnectionRequest {
  id: string;
  name: string;
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

export interface FileTransferProgress {
  bytesTransferred: number;
  totalBytes: number;
}

export interface FileCreateDirectoryRequest {
  connectionId: string;
  path: string;
}

export interface FileRemoteOperationRequest {
  connectionId: string;
  sourcePath: string;
  destinationPath: string;
  replace?: boolean;
}
