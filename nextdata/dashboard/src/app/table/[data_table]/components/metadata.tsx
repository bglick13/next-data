"use client";

import { useQuery } from "@tanstack/react-query";
import { Skeleton } from "@/components/ui/skeleton";
import {
  Card,
  CardContent,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import type { TableMetadata } from "../types";

function MetadataLoading() {
  return (
    <div className="flex flex-col gap-2">
      <Skeleton className="h-4 w-24" />
      <Skeleton className="h-4 w-24" />
      <Skeleton className="h-4 w-24" />
    </div>
  );
}

function MetadataError() {
  return <div>Error</div>;
}

function MetadataContent({ data }: { data: TableMetadata }) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>Row Count: {data?.row_count}</CardTitle>
      </CardHeader>
      <CardContent>
        <CardDescription>Schema</CardDescription>
        <ul>
          {data?.schema?.map((row) => (
            <li key={row[0]}>
              <span className="font-bold">{row[0]}:</span> {row[1]}
            </li>
          ))}
        </ul>
      </CardContent>
    </Card>
  );
}

export function Metadata({ data_table }: { data_table: string }) {
  const { data, isLoading, error } = useQuery<TableMetadata>({
    queryKey: ["table", data_table],
    queryFn: () =>
      fetch(`http://localhost:8000/api/table/${data_table}/metadata`).then(
        (res) => res.json()
      ),
  });
  return (
    <div className="flex flex-col gap-2 items-center justify-center">
      {isLoading ? (
        <MetadataLoading />
      ) : error ? (
        <MetadataError />
      ) : data ? (
        <MetadataContent data={data} />
      ) : (
        <MetadataError />
      )}
    </div>
  );
}
