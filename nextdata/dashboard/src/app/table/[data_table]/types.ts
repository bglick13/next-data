import type { jsTypeToSparkDtype } from "./util";

export interface TableMetadata {
  schema?: string[][]; // [column_name, column_type]
  row_count?: number;
}

export interface SchemaInfo {
  headerToDtype: [string, keyof typeof jsTypeToSparkDtype][];
  setHeaderToDtype: (
    headerToDtype: [string, keyof typeof jsTypeToSparkDtype][]
  ) => void;
}
