import type { ConnectionConfig, ConnectionGroup, SidebarLayout, SidebarOrderEntry, TreeNode } from "@/types/database";
import type { FileConnection } from "@/types/fileManager";
import { uuid } from "@/lib/common/utils";
import { orderPinnedTreeNodes } from "@/lib/app/pinnedItems";

export function emptyLayout(): SidebarLayout {
  return { groups: [], order: [] };
}

function folderPathSegments(path: string | undefined): string[] {
  return (path ?? "").split("/").filter((segment) => segment.length > 0);
}

export function buildSidebarLayoutFromFolderPaths(connectionIds: string[], folderPaths: Iterable<string>, connectionFolderPaths: ReadonlyMap<string, string>): SidebarLayout | undefined {
  const groups: ConnectionGroup[] = [];
  const order: SidebarOrderEntry[] = [];
  const groupEntries = new Map<string, Extract<SidebarOrderEntry, { type: "group" }>>();

  const ensureFolder = (path: string | undefined) => {
    const segments = folderPathSegments(path);
    let parentEntry: Extract<SidebarOrderEntry, { type: "group" }> | undefined;
    let currentPath = "";

    for (const segment of segments) {
      currentPath = currentPath ? `${currentPath}/${segment}` : segment;
      let entry = groupEntries.get(currentPath);
      if (!entry) {
        const groupId = uuid();
        entry = { type: "group", id: groupId, children: [] };
        groupEntries.set(currentPath, entry);
        groups.push({ id: groupId, name: segment, collapsed: false });
        if (parentEntry) parentEntry.children!.push(entry);
        else order.push(entry);
      }
      parentEntry = entry;
    }

    return parentEntry;
  };

  for (const folderPath of folderPaths) ensureFolder(folderPath);

  for (const connectionId of connectionIds) {
    const connectionEntry: SidebarOrderEntry = { type: "connection", id: connectionId };
    const folderEntry = ensureFolder(connectionFolderPaths.get(connectionId));
    if (folderEntry) folderEntry.children!.push(connectionEntry);
    else order.push(connectionEntry);
  }

  return groups.length ? { groups, order } : undefined;
}

function entryChildren(entry: Extract<SidebarOrderEntry, { type: "group" }>): SidebarOrderEntry[] {
  return entry.children ?? entry.connectionIds?.map((id) => ({ type: "connection" as const, id })) ?? [];
}

function normalizeEntry(entry: SidebarOrderEntry, validConnectionIds: Set<string>, validFileConnectionIds: Set<string>, validGroups: Set<string>, seenEntries: Set<string>, seenGroups: Set<string>): SidebarOrderEntry | null {
  if (entry.type !== "group") {
    const validIds = entry.type === "connection" ? validConnectionIds : validFileConnectionIds;
    const key = sidebarEntryTreeId(entry);
    if (!validIds.has(entry.id) || seenEntries.has(key)) return null;
    seenEntries.add(key);
    return { type: entry.type, id: entry.id };
  }

  if (!validGroups.has(entry.id) || seenGroups.has(entry.id)) return null;
  seenGroups.add(entry.id);

  const children = entryChildren(entry)
    .map((child) => normalizeEntry(child, validConnectionIds, validFileConnectionIds, validGroups, seenEntries, seenGroups))
    .filter(Boolean) as SidebarOrderEntry[];
  return { type: "group", id: entry.id, children };
}

function collectFileConnectionIds(entries: SidebarOrderEntry[], ids: Set<string>) {
  for (const entry of entries) {
    if (entry.type === "file-connection") ids.add(entry.id);
    else if (entry.type === "group") collectFileConnectionIds(entryChildren(entry), ids);
  }
}

export function reconcileLayout(connectionIds: string[], layout: SidebarLayout | null, fileConnectionIds?: string[]): SidebarLayout {
  if (!layout) {
    return {
      groups: [],
      order: [...connectionIds.map((id) => ({ type: "connection" as const, id })), ...(fileConnectionIds ?? []).map((id) => ({ type: "file-connection" as const, id }))],
    };
  }

  const validConnectionIds = new Set(connectionIds);
  const validFileConnectionIds = new Set(fileConnectionIds);
  if (fileConnectionIds === undefined) collectFileConnectionIds(layout.order, validFileConnectionIds);
  const validGroups = new Set(layout.groups.map((group) => group.id));
  const seenEntries = new Set<string>();
  const seenGroups = new Set<string>();
  const order = layout.order.map((entry) => normalizeEntry(entry, validConnectionIds, validFileConnectionIds, validGroups, seenEntries, seenGroups)).filter(Boolean) as SidebarOrderEntry[];

  for (const id of connectionIds) {
    if (!seenEntries.has(id)) {
      order.push({ type: "connection", id });
    }
  }
  if (fileConnectionIds) {
    for (const id of fileConnectionIds) {
      if (!seenEntries.has(fileConnectionTreeNodeId(id))) {
        order.push({ type: "file-connection", id });
      }
    }
  }

  const usedGroupIds = new Set<string>();
  const collectGroups = (entries: SidebarOrderEntry[]) => {
    for (const entry of entries) {
      if (entry.type !== "group") continue;
      usedGroupIds.add(entry.id);
      collectGroups(entryChildren(entry));
    }
  };
  collectGroups(order);

  const groups = layout.groups.filter((group) => usedGroupIds.has(group.id));
  return { groups, order };
}

export function remapSidebarLayoutConnectionIds(layout: SidebarLayout, connectionIdMap: Map<string, string>): SidebarLayout {
  const remapEntries = (entries: SidebarOrderEntry[]): SidebarOrderEntry[] =>
    entries.flatMap((entry): SidebarOrderEntry[] => {
      if (entry.type === "connection") {
        const id = connectionIdMap.get(entry.id);
        return id ? [{ type: "connection", id }] : [];
      }
      if (entry.type === "file-connection") return [];

      const children = entryChildren(entry).flatMap((child): SidebarOrderEntry[] => remapEntries([child]));
      return [{ type: "group", id: entry.id, children }];
    });

  return {
    groups: layout.groups.map((group) => ({ ...group })),
    order: remapEntries(layout.order),
  };
}

function makeConnectionNode(config: ConnectionConfig, pinned: boolean): TreeNode {
  return {
    id: config.id,
    label: config.name,
    type: "connection",
    connectionId: config.id,
    isExpanded: false,
    children: [],
    pinned,
  };
}

export function fileConnectionTreeNodeId(connectionId: string): string {
  return `file-connection:${connectionId}`;
}

function sidebarEntryTreeId(entry: Exclude<SidebarOrderEntry, { type: "group" }>): string {
  return entry.type === "file-connection" ? fileConnectionTreeNodeId(entry.id) : entry.id;
}

function makeFileConnectionNode(connection: FileConnection): TreeNode {
  return {
    id: fileConnectionTreeNodeId(connection.id),
    label: connection.name,
    type: "file-connection",
    fileConnectionId: connection.id,
    fileProtocol: connection.config.protocol,
    isExpanded: false,
    children: [],
  };
}

export function buildTreeNodesFromLayout(layout: SidebarLayout, connections: ConnectionConfig[], pinnedIds: Set<string>, fileConnections: FileConnection[] = []): TreeNode[] {
  const configMap = new Map(connections.map((connection) => [connection.id, connection]));
  const fileConnectionMap = new Map(fileConnections.map((connection) => [connection.id, connection]));
  const groupMap = new Map(layout.groups.map((group) => [group.id, group]));

  const build = (entries: SidebarOrderEntry[]): TreeNode[] => {
    const nodes: TreeNode[] = [];
    for (const entry of entries) {
      if (entry.type === "connection") {
        const config = configMap.get(entry.id);
        if (config) nodes.push(makeConnectionNode(config, pinnedIds.has(entry.id)));
        continue;
      }
      if (entry.type === "file-connection") {
        const connection = fileConnectionMap.get(entry.id);
        if (connection) nodes.push(makeFileConnectionNode(connection));
        continue;
      }

      const group = groupMap.get(entry.id);
      if (!group) continue;
      nodes.push({
        id: group.id,
        label: group.name,
        type: "connection-group",
        pinned: pinnedIds.has(group.id),
        isExpanded: !group.collapsed,
        children: orderPinnedTreeNodes(build(entryChildren(entry))),
      });
    }
    return nodes;
  };

  return orderPinnedTreeNodes(build(layout.order));
}

export function findConnectionLocation(layout: SidebarLayout, connectionId: string): { entries: SidebarOrderEntry[]; entryIndex: number; groupId?: string } | null {
  return findSidebarEntryLocation(layout, "connection", connectionId);
}

export function findFileConnectionLocation(layout: SidebarLayout, connectionId: string): { entries: SidebarOrderEntry[]; entryIndex: number; groupId?: string } | null {
  return findSidebarEntryLocation(layout, "file-connection", connectionId);
}

function findSidebarEntryLocation(layout: SidebarLayout, type: "connection" | "file-connection", connectionId: string): { entries: SidebarOrderEntry[]; entryIndex: number; groupId?: string } | null {
  const visit = (entries: SidebarOrderEntry[], groupId?: string): { entries: SidebarOrderEntry[]; entryIndex: number; groupId?: string } | null => {
    for (let i = 0; i < entries.length; i++) {
      const entry = entries[i];
      if (entry.type === type && entry.id === connectionId) return { entries, entryIndex: i, groupId };
      if (entry.type === "group") {
        const found = visit(entryChildren(entry), entry.id);
        if (found) return found;
      }
    }
    return null;
  };
  return visit(layout.order);
}

/**
 * Returns the display-name path for a connection's containing groups.
 * A top-level connection returns an empty path; an absent connection returns null.
 */
export function findConnectionGroupPath(layout: SidebarLayout, connectionId: string): string[] | null {
  const groupMap = new Map(layout.groups.map((group) => [group.id, group]));

  const visit = (entries: SidebarOrderEntry[], path: string[]): string[] | null => {
    for (const entry of entries) {
      if (entry.type !== "group") {
        if (entry.type === "connection" && entry.id === connectionId) return path;
        continue;
      }

      const group = groupMap.get(entry.id);
      if (!group) continue;
      const found = visit(entryChildren(entry), [...path, group.name]);
      if (found) return found;
    }
    return null;
  };

  return visit(layout.order, []);
}

/** Build all connection group paths in one traversal for list rendering. */
export function buildConnectionGroupPathMap(layout: SidebarLayout): Map<string, string[]> {
  const groupMap = new Map(layout.groups.map((group) => [group.id, group]));
  const paths = new Map<string, string[]>();

  const visit = (entries: SidebarOrderEntry[], path: string[]) => {
    for (const entry of entries) {
      if (entry.type !== "group") {
        if (entry.type === "file-connection") continue;
        paths.set(entry.id, path);
        continue;
      }

      const group = groupMap.get(entry.id);
      if (!group) continue;
      visit(entryChildren(entry), [...path, group.name]);
    }
  };

  visit(layout.order, []);
  return paths;
}

function findGroupEntry(entries: SidebarOrderEntry[], groupId: string): Extract<SidebarOrderEntry, { type: "group" }> | null {
  for (const entry of entries) {
    if (entry.type !== "group") continue;
    if (entry.id === groupId) return entry;
    const found = findGroupEntry(entryChildren(entry), groupId);
    if (found) return found;
  }
  return null;
}

function cloneEntries(entries: SidebarOrderEntry[]): SidebarOrderEntry[] {
  return entries.map((entry) => (entry.type === "group" ? { type: "group", id: entry.id, children: cloneEntries(entryChildren(entry)) } : { ...entry }));
}

function fileConnectionEntries(entries: SidebarOrderEntry[]): SidebarOrderEntry[] {
  return entries.flatMap((entry): SidebarOrderEntry[] => {
    if (entry.type === "connection") return [];
    if (entry.type === "file-connection") return [{ ...entry }];
    const children = fileConnectionEntries(entryChildren(entry));
    return children.length > 0 ? [{ type: "group", id: entry.id, children }] : [];
  });
}

function findGroupEntryInTree(entries: SidebarOrderEntry[], groupId: string): Extract<SidebarOrderEntry, { type: "group" }> | null {
  for (const entry of entries) {
    if (entry.type !== "group") continue;
    if (entry.id === groupId) return entry;
    const nested = findGroupEntryInTree(entry.children ?? [], groupId);
    if (nested) return nested;
  }
  return null;
}

function containsFileConnection(entries: SidebarOrderEntry[], connectionId: string): boolean {
  return entries.some((entry) => {
    if (entry.type === "file-connection") return entry.id === connectionId;
    return entry.type === "group" && containsFileConnection(entry.children ?? [], connectionId);
  });
}

export function preserveFileConnectionLayout(nextLayout: SidebarLayout, currentLayout: SidebarLayout): SidebarLayout {
  const order = cloneEntries(nextLayout.order);

  const mergeEntries = (target: SidebarOrderEntry[], source: SidebarOrderEntry[]) => {
    for (const entry of source) {
      if (entry.type === "file-connection") {
        if (!containsFileConnection(order, entry.id)) target.push({ ...entry });
        continue;
      }
      if (entry.type === "connection") continue;

      const existingGroup = findGroupEntryInTree(order, entry.id);
      if (existingGroup) {
        mergeEntries(existingGroup.children ?? (existingGroup.children = []), entry.children ?? []);
      } else {
        target.push({
          type: "group",
          id: entry.id,
          children: cloneEntries(entry.children ?? []),
        });
      }
    }
  };

  mergeEntries(order, fileConnectionEntries(currentLayout.order));
  const groupIds = new Set(nextLayout.groups.map((group) => group.id));
  const groups = [...nextLayout.groups.map((group) => ({ ...group })), ...currentLayout.groups.filter((group) => !groupIds.has(group.id)).map((group) => ({ ...group }))];
  return { groups, order };
}

function removeEntry(entries: SidebarOrderEntry[], id: string): SidebarOrderEntry | null {
  for (let i = 0; i < entries.length; i++) {
    const entry = entries[i];
    if ((entry.type === "group" && entry.id === id) || (entry.type !== "group" && sidebarEntryTreeId(entry) === id)) {
      entries.splice(i, 1);
      return entry;
    }
    if (entry.type === "group") {
      const removed = removeEntry(entry.children ?? [], id);
      if (removed) return removed;
    }
  }
  return null;
}

function removeConnectionFromEntries(entries: SidebarOrderEntry[], connectionId: string): SidebarOrderEntry[] {
  const next = cloneEntries(entries);
  removeTypedEntry(next, "connection", connectionId);
  return next;
}

function removeTypedEntry(entries: SidebarOrderEntry[], type: "connection" | "file-connection", id: string): SidebarOrderEntry | null {
  for (let i = 0; i < entries.length; i++) {
    const entry = entries[i];
    if (entry.type === type && entry.id === id) {
      entries.splice(i, 1);
      return entry;
    }
    if (entry.type === "group") {
      const removed = removeTypedEntry(entry.children ?? [], type, id);
      if (removed) return removed;
    }
  }
  return null;
}

function moveTypedConnectionToGroup(layout: SidebarLayout, type: "connection" | "file-connection", connectionId: string, targetGroupId: string | null): SidebarLayout {
  const order = cloneEntries(layout.order);
  removeTypedEntry(order, type, connectionId);
  const entry: SidebarOrderEntry = { type, id: connectionId };

  if (targetGroupId) {
    const group = findGroupEntry(order, targetGroupId);
    if (group) {
      group.children = [...(group.children ?? []), entry];
      return { ...expandGroup(layout, targetGroupId), order };
    }
  }

  order.push(entry);
  return { ...layout, order };
}

function containsGroup(entry: SidebarOrderEntry, groupId: string): boolean {
  if (entry.type !== "group") return false;
  if (entry.id === groupId) return true;
  return entryChildren(entry).some((child) => containsGroup(child, groupId));
}

function expandGroup(layout: SidebarLayout, groupId: string): SidebarLayout {
  return {
    ...layout,
    groups: layout.groups.map((group) => (group.id === groupId ? { ...group, collapsed: false } : group)),
  };
}

export function moveConnectionToGroup(layout: SidebarLayout, connectionId: string, targetGroupId: string | null): SidebarLayout {
  return moveTypedConnectionToGroup(layout, "connection", connectionId, targetGroupId);
}

export function moveFileConnectionToGroup(layout: SidebarLayout, connectionId: string, targetGroupId: string | null): SidebarLayout {
  return moveTypedConnectionToGroup(layout, "file-connection", connectionId, targetGroupId);
}

export type DropPosition = "before" | "after" | "inside";

export function reorderEntry(layout: SidebarLayout, draggedId: string, targetId: string, position: DropPosition): SidebarLayout {
  if (draggedId === targetId) return layout;

  const order = cloneEntries(layout.order);
  const dragged = removeEntry(order, draggedId);
  if (!dragged) return layout;

  if (dragged.type === "group" && containsGroup(dragged, targetId)) return layout;

  if (position === "inside") {
    const targetGroup = findGroupEntry(order, targetId);
    if (targetGroup) {
      targetGroup.children = [...(targetGroup.children ?? []), dragged];
      return { ...layout, order };
    }
  }

  const insertNear = (entries: SidebarOrderEntry[]): boolean => {
    for (let i = 0; i < entries.length; i++) {
      const entry = entries[i];
      if ((entry.type === "group" && entry.id === targetId) || (entry.type !== "group" && sidebarEntryTreeId(entry) === targetId)) {
        entries.splice(position === "after" ? i + 1 : i, 0, dragged);
        return true;
      }
      if (entry.type === "group" && insertNear(entry.children ?? [])) return true;
    }
    return false;
  };

  if (!insertNear(order)) order.push(dragged);
  return { ...layout, order };
}

export function createGroup(layout: SidebarLayout, name: string, parentGroupId?: string | null): { layout: SidebarLayout; groupId: string } {
  const groupId = uuid();
  const group: ConnectionGroup = { id: groupId, name, collapsed: false };
  const order = cloneEntries(layout.order);
  const entry: SidebarOrderEntry = { type: "group", id: groupId, children: [] };
  let parentFound = false;

  if (parentGroupId) {
    const parent = findGroupEntry(order, parentGroupId);
    if (parent) {
      parent.children = [...(parent.children ?? []), entry];
      parentFound = true;
    } else {
      order.push(entry);
    }
  } else {
    order.push(entry);
  }

  return {
    groupId,
    layout: {
      groups: [...layout.groups, group].map((current) => (parentFound && current.id === parentGroupId ? { ...current, collapsed: false } : current)),
      order,
    },
  };
}

export function renameGroup(layout: SidebarLayout, groupId: string, name: string): SidebarLayout {
  return {
    ...layout,
    groups: layout.groups.map((group) => (group.id === groupId ? { ...group, name } : group)),
  };
}

export function deleteGroup(layout: SidebarLayout, groupId: string): SidebarLayout {
  const order = cloneEntries(layout.order);
  const removeGroup = (entries: SidebarOrderEntry[]): boolean => {
    for (let i = 0; i < entries.length; i++) {
      const entry = entries[i];
      if (entry.type === "group" && entry.id === groupId) {
        entries.splice(i, 1, ...entryChildren(entry));
        return true;
      }
      if (entry.type === "group") {
        const removed = removeGroup(entry.children ?? []);
        if (removed) return true;
      }
    }
    return false;
  };

  const removed = removeGroup(order);
  return {
    groups: removed ? layout.groups.filter((group) => group.id !== groupId) : layout.groups,
    order,
  };
}

export function toggleGroupCollapsed(layout: SidebarLayout, groupId: string): SidebarLayout {
  return {
    ...layout,
    groups: layout.groups.map((group) => (group.id === groupId ? { ...group, collapsed: !group.collapsed } : group)),
  };
}

export function collapseAllGroups(layout: SidebarLayout): SidebarLayout {
  return {
    ...layout,
    groups: layout.groups.map((group) => (group.collapsed ? group : { ...group, collapsed: true })),
  };
}

export function removeConnectionFromSidebarLayout(layout: SidebarLayout, connectionId: string): SidebarLayout {
  return { ...layout, order: removeConnectionFromEntries(layout.order, connectionId) };
}

export function removeFileConnectionFromSidebarLayout(layout: SidebarLayout, connectionId: string): SidebarLayout {
  const order = cloneEntries(layout.order);
  removeTypedEntry(order, "file-connection", connectionId);
  return { ...layout, order };
}

export function appendConnectionToLayout(layout: SidebarLayout, connectionId: string, groupId?: string | null): SidebarLayout {
  return moveConnectionToGroup(layout, connectionId, groupId ?? null);
}

export function appendFileConnectionToLayout(layout: SidebarLayout, connectionId: string, groupId?: string | null): SidebarLayout {
  return moveFileConnectionToGroup(layout, connectionId, groupId ?? null);
}
