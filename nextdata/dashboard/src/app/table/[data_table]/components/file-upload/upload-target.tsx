interface UploadTargetProps {
  setFile: (file: File) => void;
}

export function UploadTarget({ setFile }: UploadTargetProps) {
  return (
    <div>
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
              // mutate(selectedFile);
            }
          }}
        />
      </label>
    </div>
  );
}
