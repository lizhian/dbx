import { describe, expect, it } from "vitest";
import { createFileConnectionImplementationDraft, fileConnectionRequestFromDraft, hdfsDelegationTokenUpdate, s3AccessKeyUpdate, s3SecretKeyUpdate, s3SessionTokenUpdate, sftpPrivateKeyUpdate, webdavBearerTokenUpdate } from "./fileConnectionDraft";

const implementations = [
  ["ftp", { protocol: "ftp" }],
  ["sftp", { protocol: "sftp", port: 22 }],
  ["s3", { protocol: "s3" }],
  ["webdav", { protocol: "webdav" }],
  ["webhdfs", { protocol: "hdfs", hdfsImplementation: "webhdfs" }],
  ["hdfs-native", { protocol: "hdfs", hdfsImplementation: "native" }],
] as const;

describe("file connection drafts", () => {
  it.each(implementations)("maps %s to the shared typed discriminator", (implementation, expected) => {
    expect(createFileConnectionImplementationDraft(implementation, { id: "files", name: "Files" })).toMatchObject(expected);
  });

  it("keeps S3 secrets outside external config and uses Set or Keep", () => {
    const draft = createFileConnectionImplementationDraft("s3", { id: "s3", name: "S3" });
    expect(s3AccessKeyUpdate(draft)).toEqual({ action: "keep" });
    expect(s3SecretKeyUpdate(draft)).toEqual({ action: "keep" });
    expect(s3SessionTokenUpdate(draft)).toEqual({ action: "keep" });
    expect(draft).not.toHaveProperty("clearAccessKey");
    expect(draft).not.toHaveProperty("clearSecretKey");
    expect(draft).not.toHaveProperty("clearSessionToken");

    draft.accessKey = "replacement-access";
    draft.secretKey = "replacement-secret";
    const request = fileConnectionRequestFromDraft(draft);
    expect(request.secrets).toEqual({
      accessKey: { action: "set", value: "replacement-access" },
      secretKey: { action: "set", value: "replacement-secret" },
      sessionToken: { action: "keep" },
    });
    expect(JSON.stringify(request.config)).not.toContain("replacement-");
  });

  it("clears credentials that no longer apply when authentication changes", () => {
    const sftp = createFileConnectionImplementationDraft("sftp", { id: "sftp", name: "SFTP" });
    sftp.authentication = "private_key";
    sftp.privateKey = "/private/id_ed25519";
    expect(sftpPrivateKeyUpdate(sftp)).toEqual({ action: "set", value: "/private/id_ed25519" });
    sftp.authentication = "ssh_agent";
    expect(fileConnectionRequestFromDraft(sftp).secrets?.privateKey).toEqual({ action: "clear" });

    const webdav = createFileConnectionImplementationDraft("webdav", { id: "dav", name: "WebDAV" });
    webdav.webdavAuthentication = "bearer";
    webdav.bearerToken = "bearer-secret";
    expect(webdavBearerTokenUpdate(webdav)).toEqual({ action: "set", value: "bearer-secret" });
    expect(JSON.stringify(fileConnectionRequestFromDraft(webdav).config)).not.toContain("bearer-secret");
  });

  it("keeps WebHDFS delegation tokens explicit and out of typed config", () => {
    const draft = createFileConnectionImplementationDraft("webhdfs", { id: "hdfs", name: "HDFS" });
    draft.useDelegationToken = true;
    expect(hdfsDelegationTokenUpdate(draft)).toEqual({ action: "keep" });
    draft.delegationToken = "delegation-secret";
    const request = fileConnectionRequestFromDraft(draft);
    expect(request.secrets?.delegationToken).toEqual({ action: "set", value: "delegation-secret" });
    expect(request.config).toMatchObject({
      protocol: "hdfs",
      implementation: "webhdfs",
      simpleUser: "",
      useDelegationToken: true,
    });
    expect(JSON.stringify(request.config)).not.toContain("delegation-secret");
  });
});
