export type FileDownloadStatus = "downloading" | "waiting" | "completed" | "failed" | "cancelled";

export interface FileDownloadTask {
  id: string;
  connectionId: string;
  remotePath: string;
  fileName: string;
  localPath: string;
  bytesTransferred: number;
  totalBytes: number;
  status: FileDownloadStatus;
  error?: string;
}

export function fileDownloadProgressPercent(task: FileDownloadTask): number {
  if (task.status === "completed") return 100;
  if (task.totalBytes <= 0) return 0;
  return Math.min(100, Math.round((task.bytesTransferred / task.totalBytes) * 100));
}
