"use client";

import { useDataDirectories } from "@/hooks/queries/useDataDirectories";
import { columns } from "./columns";
import { DataTable } from "@/components/ui/data-table";
export function DataDirectoryList() {
  const { data } = useDataDirectories();
  return (
    <div className="container mx-auto p-4">
      <h1 className="text-2xl font-bold tracking-tight">Data Directories</h1>
      <DataTable data={data.directories} columns={columns} />
    </div>
  );
}
