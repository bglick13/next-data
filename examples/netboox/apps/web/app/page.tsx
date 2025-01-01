import { getRandomUnreadBooks } from "@portfolio/db/queries";
import { Suspense } from "react";
import { fetchMoreRandomBooks } from "./actions";
import { BooksGrid, BooksGridSkeleton } from "@/components/books/books-grid";

async function Books() {
  const testUserId = "189835";
  const { data: initialBooks, hasMore } = await getRandomUnreadBooks({
    userId: testUserId,
  });

  return (
    <BooksGrid
      initialBooks={initialBooks}
      hasMore={hasMore}
      fetchMore={fetchMoreRandomBooks}
    />
  );
}

export default function Home() {
  return (
    <div className="p-8">
      <h1 className="text-4xl font-bold mb-8">Discover Books</h1>
      <Suspense fallback={<BooksGridSkeleton />}>
        <Books />
      </Suspense>
    </div>
  );
}