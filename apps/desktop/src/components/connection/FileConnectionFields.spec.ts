// @vitest-environment happy-dom

import { createApp, type App } from "vue";
import { afterEach, describe, expect, it } from "vitest";
import i18n from "@/i18n";
import { createFileConnectionImplementationDraft } from "@/components/file-manager/fileConnectionDraft";
import type { FileConnectionImplementation } from "@/types/fileManager";
import FileConnectionFields from "./FileConnectionFields.vue";

const mountedApps: App[] = [];

function mountFields(implementation: FileConnectionImplementation): HTMLElement {
  const container = document.createElement("div");
  document.body.append(container);
  const app = createApp(FileConnectionFields, {
    draft: createFileConnectionImplementationDraft(implementation),
  });
  mountedApps.push(app);
  app.use(i18n);
  app.mount(container);
  return container.querySelector<HTMLElement>("[data-file-connection-fields]")!;
}

afterEach(() => {
  for (const app of mountedApps.splice(0)) app.unmount();
  document.body.innerHTML = "";
});

describe("FileConnectionFields layout", () => {
  it.each<FileConnectionImplementation>(["ftp", "sftp", "s3", "webdav", "webhdfs", "hdfs-native"])("uses the shared left-label and right-control rows for %s", (implementation) => {
    const fields = mountFields(implementation);
    const rows = Array.from(fields.children) as HTMLElement[];

    expect(rows.length).toBeGreaterThan(0);
    expect(rows.every((row) => row.classList.contains("grid-cols-4"))).toBe(true);
    expect(rows.every((row) => row.querySelector(":scope > label, :scope > span"))).toBe(true);
    expect(rows.every((row) => row.querySelector(":scope > .col-span-3"))).toBe(true);
  });

  it("keeps FTP/SFTP endpoint and port controls together on the right", () => {
    const fields = mountFields("ftp");
    const endpoint = fields.querySelector<HTMLInputElement>("#file-connection-endpoint")!;
    const port = fields.querySelector<HTMLInputElement>("#file-connection-port")!;
    const controls = endpoint.parentElement!;

    expect(controls.classList.contains("col-span-3")).toBe(true);
    expect(controls.className).toContain("grid-cols-[minmax(0,1fr)_104px]");
    expect(port.parentElement).toBe(controls);
  });

  it("keeps S3 secret helpers under their right-side controls", () => {
    const fields = mountFields("s3");
    const accessKey = fields.querySelector<HTMLInputElement>("#file-connection-access-key")!;
    const controls = accessKey.closest<HTMLElement>(".col-span-3")!;

    expect(controls).not.toBeNull();
    expect(controls.parentElement?.classList.contains("grid-cols-4")).toBe(true);
    expect(fields.querySelector("label[for='file-connection-access-key']")).not.toBeNull();
  });
});
