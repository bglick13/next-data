"use client";

import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import { ArrowDown, Check, ChevronsUpDown, EyeOff } from "lucide-react";
import { ColumnDef } from "@tanstack/react-table";
import { Column } from "@tanstack/react-table";

import { ArrowUp } from "lucide-react";
import { z } from "zod";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { DTYPE_TO_ZOD_TYPE, jsTypeToSparkDtype } from "../../util";
import { SchemaInfo } from "../../types";

interface DataTableColumnHeaderProps<TData, TValue>
  extends React.HTMLAttributes<HTMLDivElement> {
  column: Column<TData, TValue>;
  title: string;
  schemaInfo?: SchemaInfo;
}

function DataTableColumnHeader<TData, TValue>({
  column,
  title,
  className,
  schemaInfo,
}: DataTableColumnHeaderProps<TData, TValue>) {
  if (!column.getCanSort()) {
    return <div className={cn(className)}>{title}</div>;
  }
  const dtype = schemaInfo?.headerToDtype.find(
    ([header, dtype]) => header === title
  )?.[1];
  return (
    <div className={cn("flex items-center space-x-2", className)}>
      <DropdownMenu>
        <DropdownMenuTrigger asChild>
          <Button
            variant="ghost"
            size="sm"
            className="-ml-3 h-8 data-[state=open]:bg-accent"
          >
            <span className="capitalize">
              {title}{" "}
              {schemaInfo &&
                `(${dtype ? dtype.toLocaleUpperCase() : "unknown"})`}
            </span>
            {column.getIsSorted() === "desc" ? (
              <ArrowDown />
            ) : column.getIsSorted() === "asc" ? (
              <ArrowUp />
            ) : (
              <ChevronsUpDown />
            )}
          </Button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align="start">
          <DropdownMenuItem onClick={() => column.toggleSorting(false)}>
            <ArrowUp className="h-3.5 w-3.5 text-muted-foreground/70" />
            Asc
          </DropdownMenuItem>
          <DropdownMenuItem onClick={() => column.toggleSorting(true)}>
            <ArrowDown className="h-3.5 w-3.5 text-muted-foreground/70" />
            Desc
          </DropdownMenuItem>
          <DropdownMenuSeparator />
          <DropdownMenuItem onClick={() => column.toggleVisibility(false)}>
            <EyeOff className="h-3.5 w-3.5 text-muted-foreground/70" />
            Hide
          </DropdownMenuItem>
          {schemaInfo && (
            <>
              <DropdownMenuSeparator />
              {Object.entries(DTYPE_TO_ZOD_TYPE).map(([key, value]) => (
                <DropdownMenuItem
                  key={key}
                  onClick={() => {
                    const newHeaderToDtype = schemaInfo.headerToDtype.map(
                      ([header, dtype]) => {
                        if (header === title) {
                          return [
                            header,
                            key as keyof typeof jsTypeToSparkDtype,
                          ];
                        }
                        return [header, dtype];
                      }
                    ) as [string, keyof typeof jsTypeToSparkDtype][];
                    schemaInfo.setHeaderToDtype(newHeaderToDtype);
                  }}
                >
                  <span className="flex items-center gap-2">
                    {key.toLocaleUpperCase()}{" "}
                    {key.toLocaleUpperCase() === dtype ? (
                      <Check className="h-3.5 w-3.5 text-muted-foreground/70" />
                    ) : undefined}
                  </span>
                </DropdownMenuItem>
              ))}
            </>
          )}
        </DropdownMenuContent>
      </DropdownMenu>
    </div>
  );
}

// This type is used to define the shape of our data.
// You can use a Zod schema here if you want.
export function makeColumnsFromSchema(
  schema: z.ZodObject<any>,
  schemaInfo?: SchemaInfo
): ColumnDef<any>[] {
  const shape = schema.shape;
  return Object.entries(shape).map(([key]) => ({
    accessorKey: key,
    header: ({ column }) => (
      <DataTableColumnHeader
        column={column}
        title={key}
        schemaInfo={schemaInfo}
      />
    ),
  }));
}
