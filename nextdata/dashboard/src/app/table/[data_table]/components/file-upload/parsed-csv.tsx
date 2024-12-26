import { jsTypeToSparkDtype, tableMetadataToZodSchema } from "../../util";
import { usePapaParse } from "react-papaparse";
import { useEffect, useState } from "react";
import { DataTable } from "@/components/ui/data-table";
import { makeColumnsFromSchema } from "../sample-data-table/columns";
import { Button } from "@/components/ui/button";
import { useMutation } from "@tanstack/react-query";

type headerToDtypeRow = [string, keyof typeof jsTypeToSparkDtype];

const inferSchemaFromPapaParseResults = (results: PapaParseResult) => {
  const {
    data,
    meta: { fields },
  } = results;
  const headerToDtype: headerToDtypeRow[] = [];
  fields.forEach((field, index) => {
    const samples = data.slice(0, 10).map((row) => row[field]);
    let dtype: keyof typeof jsTypeToSparkDtype = "string";
    if (samples.every((sample) => typeof sample === "number")) {
      dtype = "number";
    } else if (samples.every((sample) => typeof sample === "boolean")) {
      dtype = "boolean";
    }
    // check if all samples are dates or timestamps
    else if (
      samples.every((sample) => new Date(sample).toString() !== "Invalid Date")
    ) {
      dtype = "timestamp";
    }
    headerToDtype.push([
      field,
      jsTypeToSparkDtype[dtype] as keyof typeof jsTypeToSparkDtype,
    ]);
  });
  return { headerToDtype };
};

type PapaParseResult<T = unknown> = {
  data: any[];
  errors: any[];
  meta: {
    delimiter: string;
    linebreak: string;
    aborted: boolean;
    truncated: boolean;
    fields: string[];
  };
};

const uploadFile = async (
  file: File,
  table_name: string,
  headerToDtype: headerToDtypeRow[]
) => {
  const formData = new FormData();
  formData.append("file", file);
  const schema = headerToDtype.reduce((acc, [col, dtype]) => {
    acc[col] = dtype.toLocaleUpperCase();
    return acc;
  }, {} as Record<string, string>);
  formData.append("data", JSON.stringify({ schema: { schema }, table_name }));
  const response = await fetch("http://localhost:8000/api/upload_csv", {
    method: "POST",
    body: formData,
  });
  return response.json();
};

export function ParsedCSV({
  file,
  table_name,
}: {
  file: File;
  table_name: string;
}) {
  const { mutate, isPending, isSuccess, isError } = useMutation({
    mutationFn: (file: File) => {
      return uploadFile(file, table_name, headerToDtype);
    },
  });
  const { readRemoteFile } = usePapaParse();
  const [data, setData] = useState<PapaParseResult<unknown> | null>(null);
  const [headerToDtype, setHeaderToDtype] = useState<headerToDtypeRow[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const handleReadRemoteFile = async () => {
    setIsLoading(true);
    const fileUrl = URL.createObjectURL(file);
    readRemoteFile(fileUrl, {
      complete: (results) => {
        console.log({ results });
        setIsLoading(false);
        setData(results as PapaParseResult<unknown>);
      },
      header: true,
      download: true,
      dynamicTyping: true,
    });
  };

  useEffect(() => {
    handleReadRemoteFile();
  }, [file]);

  useEffect(() => {
    if (data) {
      const { headerToDtype } = inferSchemaFromPapaParseResults(data);
      setHeaderToDtype(headerToDtype);
    }
  }, [data]);

  if (isLoading) return <div>Loading...</div>;
  if (!data) return <div>No data</div>;
  if (isPending) return <div>Uploading...</div>;
  if (isSuccess) return <div>Uploaded</div>;
  if (isError) return <div>Error</div>;
  const zodSchema = tableMetadataToZodSchema({ schema: headerToDtype });
  const columns = makeColumnsFromSchema(zodSchema, {
    headerToDtype,
    setHeaderToDtype,
  });
  return (
    <div className="flex flex-col ">
      <h1 className="text-2xl font-bold">Parsed CSV Sample</h1>
      <p className="text-sm text-muted-foreground">
        {data.data.length} rows, {data.meta.fields.length} columns - review
        schema before continuing
      </p>
      <DataTable data={data.data.slice(0, 10)} columns={columns} />
      <Button className="mt-4 w-1/2 self-center" onClick={() => mutate(file)}>
        Continue
      </Button>
    </div>
  );
}
