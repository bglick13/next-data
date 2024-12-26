import { useSuspenseQuery } from "@tanstack/react-query";

type TableData = string[][];

export function useTableData(
  data_table: string,
  limit: number = 10,
  offset: number = 0
) {
  return useSuspenseQuery<TableData>({
    queryKey: ["table_data", data_table, limit, offset],
    queryFn: async () => {
      const response = await fetch(
        `http://localhost:8000/api/table/${data_table}/data?limit=${limit}&offset=${offset}`
      );
      if (!response.ok) {
        return null;
      }
      return response.json();
    },
  });
}
