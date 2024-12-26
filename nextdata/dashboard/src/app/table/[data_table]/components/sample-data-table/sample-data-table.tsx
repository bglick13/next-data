"use client";

import { DataTable, DataTableLoading } from "@/components/ui/data-table";
import { tableMetadataToZodSchema, transformSampleData } from "../../util";
import { makeColumnsFromSchema } from "./columns";
import { useTableMetadata } from "@/hooks/queries/useTableMetadata";
import { useTableData } from "@/hooks/queries/useTableData";

export function SampleDataTable({ data_table }: { data_table: string }) {
  const { data: metadata, isLoading: metadata_is_loading } =
    useTableMetadata(data_table);
  const {
    data,
    isLoading: data_is_loading,
    isError,
  } = useTableData(data_table, 10, 0);
  const schema = tableMetadataToZodSchema(metadata);
  const columns = makeColumnsFromSchema(schema);
  if (metadata_is_loading || data_is_loading) {
    return <DataTableLoading />;
  }
  if (!data || isError) {
    return null;
  }
  const transformed = transformSampleData(data, schema);
  return (
    <div className="container mx-auto p-4">
      <h1 className="text-2xl font-bold tracking-tight">Data Directories</h1>
      <DataTable data={transformed} columns={columns} />
    </div>
  );
}
