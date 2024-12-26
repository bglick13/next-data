import { UploadTarget } from "./upload-target";

interface UploadComponentProps {
  setFile: (file: File) => void;
  file: File | null;
}

export function UploadComponent({ setFile, file }: UploadComponentProps) {
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
        }
      }}
    >
      <div className="space-y-2">
        <div className="text-gray-600">
          <UploadTarget setFile={setFile} />
        </div>
        {file && (
          <div className="text-sm text-gray-500">
            Selected file: {file.name}
          </div>
        )}
      </div>
    </div>
  );
}
