"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { BookCard } from "./book-card";
import { Props } from "./types";

export function InfiniteBooks({
  initialBooks,
  hasMore: initialHasMore,
  fetchMore,
  uiContext,
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
      const { data, hasMore } = await fetchMore(page.current + 1);
      if (data.length > 0) {
        setBooks((prev) => [...prev, ...data]);
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
        <BookCard key={book.isbn} book={book} uiContext={uiContext} />
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
