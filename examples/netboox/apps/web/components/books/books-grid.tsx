"use client";

import { InfiniteBooks } from "./infinite-books";
import { Skeleton } from "@workspace/ui/components/skeleton";
import { Card } from "@workspace/ui/components/card";

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

export function BooksGrid(props: Props) {
  return <InfiniteBooks {...props} />;
}

export function BooksGridSkeleton() {
  return (
    <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-4 mb-8">
      {Array.from({ length: 12 }).map((_, i) => (
        // biome-ignore lint/suspicious/noArrayIndexKey: <explanation>
        <Card key={i} className="overflow-hidden">
          <div className="relative aspect-[2/3]">
            <Skeleton className="h-full w-full" />
            <div className="absolute inset-0 bg-gradient-to-t from-background to-transparent opacity-100">
              <div className="absolute bottom-0 left-0 right-0 p-4 space-y-2">
                <Skeleton className="h-4 w-3/4" />
                <Skeleton className="h-3 w-1/2" />
                <div className="flex items-center mt-1">
                  <Skeleton className="h-4 w-4 rounded-full mr-1" />
                  <Skeleton className="h-3 w-8" />
                </div>
              </div>
            </div>
          </div>
        </Card>
      ))}
    </div>
  );
}
