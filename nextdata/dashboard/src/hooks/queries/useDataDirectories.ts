"use client";

import { useSuspenseQuery } from "@tanstack/react-query";

export interface DataDirectory {
  id: string;
  name: string;
}

interface DataDirectoriesResponse {
  directories: DataDirectory[];
}

export function useDataDirectories() {
  return useSuspenseQuery<DataDirectoriesResponse>({
    queryKey: ["data_directories"],
    queryFn: () =>
      fetch("http://localhost:8000/api/data_directories").then((res) =>
        res.json()
      ),
    // refetchInterval: 1000,
  });
}
