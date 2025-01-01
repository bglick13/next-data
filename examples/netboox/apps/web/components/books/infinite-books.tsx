"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { BookCard } from "./book-card";
import { books } from "@workspace/db/src/schema.js";

interface Props {
  initialBooks: (typeof books.$inferSelect & {
    rating: number;
  })[];
  hasMore: boolean;
  fetchMore: (page: number) => Promise<
    (typeof books.$inferSelect & {
      rating: number;
    })[]
  >;
}

export function InfiniteBooks({
  initialBooks,
  hasMore: initialHasMore,
  fetchMore,
}: Props) {
  const [books, setBooks] = useState(initialBooks);
  const [hasMore, setHasMore] = useState(initialHasMore);
  const [isLoading, setIsLoading] = useState(false);
  const page = useRef(1);
  const observerTarget = useRef(null);

  // Reset state when initialBooks changes
  useEffect(() => {
    setBooks(initialBooks);
    setHasMore(initialHasMore);
    page.current = 1;
  }, [initialBooks, initialHasMore]);

  const loadMore = useCallback(async () => {
    if (isLoading || !hasMore) return;

    setIsLoading(true);
    try {
      const newBooks = await fetchMore(page.current + 1);
      if (newBooks.length > 0) {
        setBooks((prev) => [...prev, ...newBooks]);
        page.current += 1;
      } else {
        setHasMore(false);
      }
    } catch (error) {
      console.error("Error loading more books:", error);
    } finally {
      setIsLoading(false);
    }
  }, [fetchMore, hasMore, isLoading]);

  useEffect(() => {
    const observer = new IntersectionObserver(
      (entries) => {
        if (entries[0]?.isIntersecting) {
          loadMore();
        }
      },
      { threshold: 0.1 }
    );

    if (observerTarget.current) {
      observer.observe(observerTarget.current);
    }

    return () => observer.disconnect();
  }, [loadMore]);

  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-4 mb-8">
      {books.map((book) => (
        <BookCard key={book.isbn} book={book} rating={book.rating} />
      ))}
      {hasMore && (
        <div
          ref={observerTarget}
          className="col-span-full flex justify-center p-4"
        >
          <div className="animate-pulse">Loading more books...</div>
        </div>
      )}
    </div>
  );
}