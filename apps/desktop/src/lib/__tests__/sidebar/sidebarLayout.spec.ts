import { describe, expect, it } from "vitest";
import { buildTreeNodesFromLayout, fileConnectionTreeNodeId, findConnectionGroupPath, moveFileConnectionToGroup, preserveFileConnectionLayout, reconcileLayout } from "@/lib/sidebar/sidebarLayout";
import type { ConnectionConfig, SidebarLayout } from "@/types/database";
import type { FileConnection } from "@/types/fileManager";

const layout: SidebarLayout = {
  groups: [
    { id: "project", name: "Project", collapsed: false },
    { id: "staging", name: "Staging", collapsed: false },
  ],
  order: [
    {
      type: "group",
      id: "project",
      children: [
        {
          type: "group",
          id: "staging",
          children: [{ type: "connection", id: "nested" }],
        },
        { type: "connection", id: "grouped" },
      ],
    },
    { type: "connection", id: "root" },
  ],
};

describe("findConnectionGroupPath", () => {
  it("returns every containing group from root to leaf", () => {
    expect(findConnectionGroupPath(layout, "nested")).toEqual(["Project", "Staging"]);
    expect(findConnectionGroupPath(layout, "grouped")).toEqual(["Project"]);
  });

  it("distinguishes a top-level connection from a missing connection", () => {
    expect(findConnectionGroupPath(layout, "root")).toEqual([]);
    expect(findConnectionGroupPath(layout, "missing")).toBeNull();
  });
});

describe("mixed database and file connection layout", () => {
  const mixedLayout: SidebarLayout = {
    groups: [{ id: "project", name: "Project", collapsed: false }],
    order: [
      {
        type: "group",
        id: "project",
        children: [
          { type: "connection", id: "shared-id" },
          { type: "file-connection", id: "shared-id" },
        ],
      },
    ],
  };

  it("keeps typed leaves distinct even when their raw ids match", () => {
    const databaseConnection = { id: "shared-id", name: "Database" } as ConnectionConfig;
    const fileConnection = {
      id: "shared-id",
      name: "Files",
      config: { protocol: "ftp", endpoint: "127.0.0.1", port: 21, root: "/", username: "" },
      capabilities: {},
      secretStatus: {},
    } as FileConnection;

    const [group] = buildTreeNodesFromLayout(mixedLayout, [databaseConnection], new Set(), [fileConnection]);
    expect(group.children?.map((node) => [node.type, node.id])).toEqual([
      ["connection", "shared-id"],
      ["file-connection", fileConnectionTreeNodeId("shared-id")],
    ]);
  });

  it("preserves file entries until their store is authoritative, then cleans stale entries", () => {
    expect(reconcileLayout(["shared-id"], mixedLayout).order).toEqual(mixedLayout.order);
    const reconciled = reconcileLayout(["shared-id"], mixedLayout, []);
    expect(reconciled.order).toEqual([{ type: "group", id: "project", children: [{ type: "connection", id: "shared-id" }] }]);
  });

  it("moves only the file connection into a group", () => {
    const layout: SidebarLayout = {
      groups: [{ id: "project", name: "Project", collapsed: true }],
      order: [
        { type: "connection", id: "shared-id" },
        { type: "file-connection", id: "shared-id" },
        { type: "group", id: "project", children: [] },
      ],
    };
    const moved = moveFileConnectionToGroup(layout, "shared-id", "project");
    expect(moved.order).toEqual([
      { type: "connection", id: "shared-id" },
      { type: "group", id: "project", children: [{ type: "file-connection", id: "shared-id" }] },
    ]);
    expect(moved.groups[0].collapsed).toBe(false);
  });

  it("preserves file connection groups when a database layout is applied", () => {
    const importedLayout: SidebarLayout = {
      groups: [
        { id: "imported", name: "Imported", collapsed: false },
        { id: "project", name: "Project from import", collapsed: true },
      ],
      order: [
        {
          type: "group",
          id: "imported",
          children: [{ type: "connection", id: "imported-db" }],
        },
        { type: "group", id: "project", children: [] },
      ],
    };
    const merged = preserveFileConnectionLayout(importedLayout, mixedLayout);

    expect(merged.order).toEqual([
      {
        type: "group",
        id: "imported",
        children: [{ type: "connection", id: "imported-db" }],
      },
      {
        type: "group",
        id: "project",
        children: [{ type: "file-connection", id: "shared-id" }],
      },
    ]);
    expect(merged.groups.find((group) => group.id === "project")).toEqual({
      id: "project",
      name: "Project from import",
      collapsed: true,
    });
  });
});
