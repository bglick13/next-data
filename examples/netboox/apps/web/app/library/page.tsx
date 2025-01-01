import { getUserRatings } from "@portfolio/db/queries";
import { Suspense } from "react";
import { fetchMoreUserRatings } from "../actions";
import { BooksGrid, BooksGridSkeleton } from "@/components/books/books-grid";

async function Books() {
  const testUserId = "189835";
  const { data: initialBooks, hasMore } = await getUserRatings({
    userId: testUserId,
  });

  return (
    <BooksGrid
      initialBooks={initialBooks}
      hasMore={hasMore}
      fetchMore={fetchMoreUserRatings}
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
