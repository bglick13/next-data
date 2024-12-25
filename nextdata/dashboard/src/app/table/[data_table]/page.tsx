import { FileUpload } from "./components/file-upload";

export default async function Page({
  params,
}: {
  params: { data_table: string };
}) {
  const { data_table } = await params;
  return (
    <div>
      <FileUpload table_name={data_table} />
    </div>
  );
}
