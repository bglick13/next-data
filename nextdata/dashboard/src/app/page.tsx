import { DataDirectoryList } from "./components/data-directory-list";
import { getQueryClient } from "./get-query-client";
import { HydrationBoundary, dehydrate } from "@tanstack/react-query";

export default function Home() {
  const queryClient = getQueryClient();
  void queryClient.prefetchQuery({
    queryKey: ["data_directories"],
    queryFn: () =>
      fetch("http://localhost:8000/api/data_directories").then((res) =>
        res.json()
      ),
  });
  return (
    <HydrationBoundary state={dehydrate(queryClient)}>
      <h1>Hello World</h1>
      <DataDirectoryList />
    </HydrationBoundary>
  );
}
