import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "vitest";

const tauriBackend = readFileSync(new URL("../../apps/desktop/src/lib/backend/tauri.ts", import.meta.url), "utf8");
const page = readFileSync(new URL("../../apps/desktop/src/components/file-manager/FileManagerPage.vue", import.meta.url), "utf8");
const app = readFileSync(new URL("../../apps/desktop/src/App.vue", import.meta.url), "utf8");
const toolbar = readFileSync(new URL("../../apps/desktop/src/components/layout/AppToolbar.vue", import.meta.url), "utf8");
const tabBar = readFileSync(new URL("../../apps/desktop/src/components/layout/AppTabBar.vue", import.meta.url), "utf8");
const english = readFileSync(new URL("../../apps/desktop/src/i18n/locales/en.ts", import.meta.url), "utf8");

test("file manager frontend uses the dedicated Tauri command contract", () => {
  for (const command of ["list_file_connections", "save_file_connection", "delete_file_connection", "test_file_connection", "list_file_entries", "list_file_entries_next", "close_file_list_cursor", "stat_file_entry", "create_file_directory", "delete_file_entry"]) {
    assert.match(tauriBackend, new RegExp(`invoke\\("${command}"`));
  }
  assert.doesNotMatch(tauriBackend, /list_file_root|listFileRoot/);
});

test("connection editor exposes CRUD, staged testing, root browsing, and protocol-level security guidance", () => {
  for (const key of ["ftpSecurity", "sftpSecurity", "s3Security", "webdavSecurity", "hdfsSecurity", "webhdfsSecurity"]) {
    assert.match(page, new RegExp(`${key}: t\\("fileManager\\.${key}"\\)`));
  }
  assert.match(english, /ftpSecurity: "FTP is unencrypted\. Usernames, passwords, and file contents travel over the network in plaintext\."/);

  const securitySelector = page.match(/const connectionSecurityText = computed\(\(\) =>([\s\S]*?)\n\);/)?.[1];
  assert.ok(securitySelector, "connectionSecurityText selector must exist");
  assert.match(securitySelector, /form\.value\.type === "ftp"\s*\?\s*text\.value\.ftpSecurity/);
  assert.match(securitySelector, /form\.value\.type === "sftp"\s*\?\s*sftpSupported\s*\?\s*text\.value\.sftpSecurity\s*:\s*text\.value\.sftpUnsupported/);
  assert.match(securitySelector, /form\.value\.type === "s3"\s*\?\s*text\.value\.s3Security/);
  assert.match(securitySelector, /form\.value\.type === "hdfs"\s*\?\s*form\.value\.hdfsImplementation === "webhdfs"\s*\?\s*text\.value\.webhdfsSecurity\s*:\s*text\.value\.hdfsSecurity/);
  assert.match(securitySelector, /:\s*text\.value\.webdavSecurity/);

  assert.match(page, /configuration: t\("fileManager.stageConfiguration"\)/);
  assert.match(page, /authentication: t\("fileManager.stageAuthentication"\)/);
  assert.match(page, /api\.saveFileConnection/);
  assert.match(page, /api\.deleteFileConnection/);
  assert.match(page, /api\.testFileConnection/);
  assert.match(page, /api\.listFileEntries/);
  assert.match(page, /api\.listFileEntriesNext/);
  assert.match(page, /api\.closeFileListCursor/);
  assert.match(page, /api\.statFileEntry/);
  assert.match(page, /api\.createFileDirectory/);
  assert.match(page, /api\.deleteFileEntry/);
});

test("file manager is mounted as an independent special page", () => {
  assert.match(app, /fileManagerTabOpen/);
  assert.match(app, /<FileManagerPage/);
  assert.match(app, /@open-file-manager="openFileManagerPage"/);
  assert.match(app, /"fileManager" \| "welcome"/);
  assert.match(app, /settingsReturnSurface\.value === "fileManager"/);
  assert.match(app, /if \(!isDesktop\) return/);
  assert.match(toolbar, /<Button v-if="isDesktop"[^>]+open-file-manager/);
});

test("file manager reports root errors, prevents duplicate deletion, and exposes an accessible tab", () => {
  assert.match(page, /rootError/);
  assert.match(page, /v-else-if="rootError" role="alert"/);
  assert.match(page, /if \(!selectedId\.value \|\| deleting\.value\) return/);
  assert.match(page, /:disabled="deleting"/);
  assert.match(tabBar, /data-file-manager-tab[\s\S]+role="tab"/);
  assert.match(tabBar, /@keydown\.enter\.self\.prevent="emit\('activate-file-manager'\)"/);
  assert.match(tabBar, /@keydown\.space\.self\.prevent="emit\('activate-file-manager'\)"/);
});

test("file mutations expose accessible create and guarded delete controls", () => {
  assert.match(page, /:aria-label="text\.createDirectory"/);
  assert.match(page, /:aria-label="`\$\{text\.deleteEntry\}: \$\{entry\.name\}`"/);
  assert.match(page, /api\.deleteFileEntry\(connectionId, entry\.path, false, entry\.kind\)/);
  assert.match(page, /!directoryPath\.value \|\| mutating/);
  assert.match(english, /Only files and empty directories can be deleted/);
});
