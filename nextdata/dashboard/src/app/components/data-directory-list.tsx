"use client";

import { useSuspenseQuery } from "@tanstack/react-query";

export function DataDirectoryList() {
  const { data } = useSuspenseQuery({
    queryKey: ["data_directories"],
    queryFn: () =>
      fetch("http://localhost:8000/api/data_directories").then((res) =>
        res.json()
      ),
    refetchInterval: 1000,
  });
  return (
    <div>
      {data.directories.map((d: string) => (
        <div key={d}>{d}</div>
      ))}
    </div>
  );
}
