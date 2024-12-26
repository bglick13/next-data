"use client";

import { Button } from "@/components/ui/button";
import { DataDirectory } from "@/hooks/queries/useDataDirectories";
import { ColumnDef } from "@tanstack/react-table";
import Link from "next/link";

export const columns: ColumnDef<DataDirectory>[] = [
  {
    accessorKey: "name",
    header: "Name",
  },
  {
    accessorKey: "path",
    header: "Path",
  },
  {
    accessorKey: "type",
    header: "Type",
  },
  {
    accessorKey: "actions",
    header: "Actions",
    cell: ({ row }) => {
      return (
        <Button asChild>
          <Link href={`/table/${row.original.name}`}>View</Link>
        </Button>
      );
    },
  },
];
