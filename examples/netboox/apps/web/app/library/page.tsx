import { getUserRatings } from "@workspace/db/src/queries";
import { Suspense } from "react";
import { fetchMoreUserRatings } from "../actions";
import { BooksGrid, BooksGridSkeleton } from "@/components/books/books-grid";
import { connection } from "next/server";
async function Books() {
  await connection();
  const testUserId = 189835;
  const { data: initialBooks, hasMore } = await getUserRatings({
    userId: testUserId,
    offset: 0,
    limit: 12,
  });

  return (
    <BooksGrid
      initialBooks={initialBooks}
      hasMore={hasMore}
      fetchMore={fetchMoreUserRatings}
      uiContext="library"
    />
  );
}

export default function LibraryPage() {
  return (
    <div className="p-8">
      <h1 className="text-4xl font-bold mb-8">My Library</h1>
      <Suspense fallback={<BooksGridSkeleton />}>
        <Books />
      </Suspense>
    </div>
  );
}
