import { Metadata } from "./components/metadata";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { SampleDataTable } from "./components/sample-data-table/sample-data-table";
import { FileUpload } from "./components/file-upload";
import { DataTableLoading } from "@/components/ui/data-table";
import { Suspense } from "react";
import { TableJobs } from "./components/table-jobs";

export default async function Page({
  params,
}: {
  params: Promise<{ data_table: string }>;
}) {
  const { data_table } = await params;
  return (
    <div className="flex flex-col gap-2 items-center justify-center p-4">
      <h1 className="text-2xl font-bold">{data_table}</h1>
      <Tabs defaultValue="metadata" className="w-full">
        <TabsList className="grid w-full grid-cols-4">
          <TabsTrigger value="metadata">Metadata</TabsTrigger>
          <TabsTrigger value="sample-data">Sample Data</TabsTrigger>
          <TabsTrigger value="jobs">Jobs</TabsTrigger>
          <TabsTrigger value="upload">Upload</TabsTrigger>
        </TabsList>
        <TabsContent value="metadata">
          <Metadata data_table={data_table} />
        </TabsContent>
        <TabsContent value="sample-data">
          <Suspense fallback={<DataTableLoading />}>
            <SampleDataTable data_table={data_table} />
          </Suspense>
        </TabsContent>
        <TabsContent value="jobs">
          <Suspense fallback={<DataTableLoading />}>
            <TableJobs data_table={data_table} />
          </Suspense>
        </TabsContent>
        <TabsContent value="upload">
          <FileUpload table_name={data_table} />
        </TabsContent>
      </Tabs>
    </div>
  );
}
