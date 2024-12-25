"use client";

import { useState } from "react";
import { useMutation } from "@tanstack/react-query";

const uploadFile = async (file: File, table_name: string) => {
  const formData = new FormData();
  formData.append("file", file);
  formData.append("table_name", table_name);

  const response = await fetch("http://localhost:8000/api/upload_csv", {
    method: "POST",
    body: formData,
  });
  return response.json();
};

export function FileUpload({ table_name }: { table_name: string }) {
  const [file, setFile] = useState<File | null>(null);

  const { mutate, isPending } = useMutation({
    mutationFn: (file: File) => {
      return uploadFile(file, table_name);
    },
  });

  return (
    <div
      className="m-8 border-2 border-dashed border-gray-300 rounded-lg p-12 text-center hover:border-gray-400 transition-colors"
      onDragOver={(e) => {
        e.preventDefault();
        e.stopPropagation();
      }}
      onDrop={(e) => {
        e.preventDefault();
        e.stopPropagation();
        const droppedFile = e.dataTransfer.files[0];
        if (droppedFile) {
          setFile(droppedFile);
          mutate(droppedFile);
        }
      }}
    >
      <div className="space-y-2">
        <div className="text-gray-600">
          {isPending ? (
            "Uploading..."
          ) : (
            <>
              Drag and drop your CSV file here, or{" "}
              <label className="text-blue-500 hover:text-blue-600 cursor-pointer">
                browse
                <input
                  type="file"
                  className="hidden"
                  accept=".csv"
                  onChange={(e) => {
                    const selectedFile = e.target.files?.[0];
                    if (selectedFile) {
                      setFile(selectedFile);
                      mutate(selectedFile);
                    }
                  }}
                />
              </label>
            </>
          )}
        </div>
        {file && !isPending && (
          <div className="text-sm text-gray-500">
            Selected file: {file.name}
          </div>
        )}
      </div>
    </div>
  );
}
