"use client";

import { Button } from "@/components/ui/button";
import { ColumnDef } from "@tanstack/react-table";
import Link from "next/link";

// This type is used to define the shape of our data.
// You can use a Zod schema here if you want.
export type DataDirectory = {
  id: string;
  name: string;
  path: string;
  type: "directory" | "file";
};

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
