import { useSuspenseQuery } from "@tanstack/react-query";

interface TableMetadata {
  row_count: number;
  schema: string[][];
}

export function useTableMetadata(data_table: string) {
  return useSuspenseQuery<TableMetadata>({
    queryKey: ["table_metadata", data_table],
    queryFn: () =>
      fetch(`http://localhost:8000/api/table/${data_table}/metadata`).then(
        (res) => res.json()
      ),
  });
}
