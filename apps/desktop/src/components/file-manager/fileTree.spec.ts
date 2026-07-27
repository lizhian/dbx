import { describe, expect, it } from "vitest";
import type { FileEntry } from "@/types/fileManager";
import { flattenVisibleFileTree, normalizeFileListing } from "./fileTree";

const directory = (path: string, name = path.split("/").pop() || "/"): FileEntry => ({
  path,
  name,
  kind: "directory",
  size: 0,
});

const file = (path: string): FileEntry => ({
  path,
  name: path.split("/").pop() || path,
  kind: "file",
  size: 1,
});

describe("file manager tree", () => {
  it("removes root and current-directory pseudo entries from protocol listings", () => {
    expect(normalizeFileListing([directory("/", "/"), directory("folder/", "/"), directory("folder/"), file("fixture.txt")], "")).toEqual([directory("folder", "folder"), file("fixture.txt")]);
    expect(normalizeFileListing([directory("folder/", "folder"), file("folder/child.txt")], "folder")).toEqual([file("folder/child.txt")]);
  });

  it("flattens only expanded directory branches with stable depth", () => {
    const roots = [directory("folder"), file("root.txt")];
    const children = new Map<string, FileEntry[]>([
      ["folder", [directory("folder/nested"), file("folder/child.txt")]],
      ["folder/nested", [file("folder/nested/deep.txt")]],
    ]);

    expect(flattenVisibleFileTree(roots, new Set(["folder", "folder/nested"]), children).map((row) => [row.entry.path, row.depth])).toEqual([
      ["folder", 0],
      ["folder/nested", 1],
      ["folder/nested/deep.txt", 2],
      ["folder/child.txt", 1],
      ["root.txt", 0],
    ]);
  });
});
