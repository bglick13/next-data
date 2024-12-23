"use client";

import { useSuspenseQuery } from "@tanstack/react-query";
import { DataTable } from "./data-table";
import { columns } from "./columns";

export function DataDirectoryList() {
  const { data } = useSuspenseQuery({
    queryKey: ["data_directories"],
    queryFn: () =>
      fetch("http://localhost:8000/api/data_directories").then((res) =>
        res.json()
      ),
    refetchInterval: undefined,
  });
  return (
    <div className="container mx-auto p-4">
      <h1 className="text-2xl font-bold tracking-tight">Data Directories</h1>
      <DataTable data={data.directories} columns={columns} />
    </div>
  );
}
