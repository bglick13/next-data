import { searchBooks } from "@workspace/db/src/queries";
import { Suspense } from "react";
import { fetchMoreSearchResults } from "../actions";
import { BooksGrid, BooksGridSkeleton } from "@/components/books/books-grid";
import { SearchInput } from "./search-input";
import { searchQueryCache } from "./searchParams";
import type { SearchParams } from "nuqs/server";

type PageProps = {
  searchParams: Promise<SearchParams>; // Next.js 15+: async searchParams prop
};

async function SearchResults({ query }: { query: string }) {
  const { data: initialBooks, hasMore } = await searchBooks({
    query,
    offset: 0,
    limit: 12,
  });

  return (
    <BooksGrid
      initialBooks={initialBooks}
      hasMore={hasMore}
      fetchMore={async (page) => {
        "use server";
        return fetchMoreSearchResults(query, page);
      }}
      uiContext="search"
    />
  );
}

export default async function SearchPage({ searchParams }: PageProps) {
  const { q } = await searchQueryCache.parse(searchParams);
  return (
    <div className="p-8">
      <div className="max-w-2xl mx-auto mb-8">
        <h1 className="text-4xl font-bold mb-4">Search Books</h1>
        <SearchInput />
      </div>

      {q ? (
        <Suspense fallback={<BooksGridSkeleton />}>
          <SearchResults query={q} />
        </Suspense>
      ) : (
        <div className="text-center text-muted-foreground">
          Enter a search term to find books
        </div>
      )}
    </div>
  );
}
