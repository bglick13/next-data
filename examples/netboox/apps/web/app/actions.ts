"use server";

import {
  getRandomUnreadBooks,
  getUserRatings,
  searchBooks,
} from "@portfolio/db/queries";

export async function fetchMoreRandomBooks(page: number) {
  const testUserId = "189835";
  const { data } = await getRandomUnreadBooks({
    userId: testUserId,
    offset: (page - 1) * 12,
  });
  return data;
}

export async function fetchMoreUserRatings(page: number) {
  const testUserId = "189835";
  const { data } = await getUserRatings({
    userId: testUserId,
    offset: (page - 1) * 12,
  });
  return data;
}

export async function fetchMoreSearchResults(query: string, page: number) {
  const { data } = await searchBooks({
    query,
    offset: (page - 1) * 12,
  });
  return data;
}
