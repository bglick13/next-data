"use client";

import { useState } from "react";
import { UploadComponent } from "./upload-component";
import { ParsedCSV } from "./parsed-csv";

export function FileUpload({ table_name }: { table_name: string }) {
  /* 
  File upload flow:
  - User selects a file
  - File is uploaded to client and CSV is parsed
  - CSV is validated against the table schema
  - User can edit column names and types
  - User clicks submit
  - CSV is sent to backend with edited schema
  - CSV is uploaded to S3
  - CSV is added to the table
  */
  const [file, setFile] = useState<File | null>(null);

  if (!file) {
    return <UploadComponent setFile={setFile} file={file} />;
  }

  return <ParsedCSV file={file} table_name={table_name} />;
}
