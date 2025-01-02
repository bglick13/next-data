import { Suspense } from "react";
import { fetchMoreRandomBooks } from "./actions";
import { BooksGrid, BooksGridSkeleton } from "@/components/books/books-grid";
import { getRandomUnreadBooks } from "@workspace/db/src/queries";
import { connection } from "next/server";

async function Books() {
  // "use cache";
  await connection();
  const testUserId = 189835;
  const { data: initialBooks, hasMore } = await getRandomUnreadBooks({
    userId: testUserId,
    offset: 0,
    limit: 12,
  });

  return (
    <BooksGrid
      initialBooks={initialBooks}
      hasMore={hasMore}
      fetchMore={fetchMoreRandomBooks}
      uiContext="explore"
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
