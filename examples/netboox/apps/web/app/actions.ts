"use server";

import {
  getBookAverageRating,
  getRandomUnreadBooks,
  getUserRatings,
  searchBooks,
} from "@workspace/db/src/queries";

export async function fetchMoreRandomBooks(page: number) {
  const testUserId = 189835;
  const data = await getRandomUnreadBooks({
    userId: testUserId,
    offset: (page - 1) * 12,
    limit: 12,
  });
  return data;
}

export async function fetchMoreUserRatings(page: number) {
  const testUserId = 189835;
  const { data, hasMore } = await getUserRatings({
    userId: testUserId,
    offset: (page - 1) * 12,
    limit: 12,
  });
  return { data, hasMore };
}

export async function fetchMoreSearchResults(query: string, page: number) {
  const { data, hasMore } = await searchBooks({
    query,
    offset: (page - 1) * 12,
    limit: 12,
  });
  return { data, hasMore };
}

export async function fetchBookAverageRating(isbn: string) {
  const bookAverageRating = await getBookAverageRating(isbn);
  return bookAverageRating;
}
