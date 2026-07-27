import { describe, expect, it } from "vitest";
import { buildTreeNodesFromLayout, findConnectionGroupPath, reconcileLayout } from "@/lib/sidebar/sidebarLayout";
import type { ConnectionConfig, SidebarLayout } from "@/types/database";

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

describe("unified file connection layout", () => {
  const unifiedLayout: SidebarLayout = {
    groups: [{ id: "project", name: "Project", collapsed: false }],
    order: [{ type: "group", id: "project", children: [{ type: "connection", id: "files" }] }],
  };

  it("renders a file config as an ordinary connection node", () => {
    const fileConnection = {
      id: "files",
      name: "Files",
      db_type: "file",
      driver_profile: "ftp",
      external_config: { protocol: "ftp", endpoint: "127.0.0.1", port: 21, root: "/", username: "" },
    } as ConnectionConfig;

    const [group] = buildTreeNodesFromLayout(unifiedLayout, [fileConnection], new Set());
    expect(group.children?.map((node) => [node.type, node.id])).toEqual([["connection", "files"]]);
    expect(reconcileLayout(["files"], unifiedLayout)).toEqual(unifiedLayout);
  });
});
