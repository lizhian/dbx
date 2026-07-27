import type { FileEntry } from "@/types/fileManager";

export interface FileTreeRow {
  entry: FileEntry;
  depth: number;
}

function normalizedRemotePath(path: string): string {
  return path.replace(/\/+$/, "");
}

export function normalizeFileListing(entries: readonly FileEntry[], listedPath: string): FileEntry[] {
  const normalizedListedPath = normalizedRemotePath(listedPath);
  const seen = new Set<string>();
  const result: FileEntry[] = [];

  for (const entry of entries) {
    const path = normalizedRemotePath(entry.path);
    if (!path || path === normalizedListedPath || seen.has(path)) continue;
    seen.add(path);
    result.push({
      ...entry,
      path,
      name: entry.name === "/" || !entry.name ? (path.split("/").pop() ?? path) : entry.name,
    });
  }

  return result;
}

export function flattenVisibleFileTree(roots: readonly FileEntry[], expandedPaths: ReadonlySet<string>, childrenByPath: ReadonlyMap<string, readonly FileEntry[]>): FileTreeRow[] {
  const rows: FileTreeRow[] = [];
  const visited = new Set<string>();

  function append(entries: readonly FileEntry[], depth: number) {
    for (const entry of entries) {
      if (visited.has(entry.path)) continue;
      visited.add(entry.path);
      rows.push({ entry, depth });
      if (entry.kind === "directory" && expandedPaths.has(entry.path)) {
        append(childrenByPath.get(entry.path) ?? [], depth + 1);
      }
    }
  }

  append(roots, 0);
  return rows;
}
