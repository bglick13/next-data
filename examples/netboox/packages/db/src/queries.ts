import { getConnection } from "./drizzle";
import { books, ratings } from "./schema";
import { and, eq, ilike, sql, desc, ne } from "drizzle-orm";

export async function getRandomUnreadBooks({
  userId,
  offset,
  limit = 12,
}: {
  userId: string;
  offset: number;
  limit: number;
}) {
  const db = await getConnection();
  const result = await db
    .select({
      isbn: books.isbn,
      book_title: books.book_title,
      book_author: books.book_author,
      image_url_s: books.image_url_s,
      image_url_m: books.image_url_m,
      image_url_l: books.image_url_l,
      avg_rating: sql<number>`avg(${ratings.book_rating})::float`,
      num_ratings: sql<number>`count(${ratings.book_rating})::int`,
    })
    .from(books)
    .leftJoin(ratings, eq(books.isbn, ratings.isbn))
    // Then, ensure this specific user hasn't rated it
    .where(
      sql`NOT EXISTS (
        SELECT 1 FROM ${ratings} r 
        WHERE r.isbn = ${books.isbn} 
        AND r.user_id = ${userId}
      )`
    )
    .groupBy(
      books.isbn,
      books.book_title,
      books.book_author,
      books.image_url_s,
      books.image_url_m,
      books.image_url_l
    )
    .orderBy(sql`random()`)
    .limit(limit + 1)
    .offset(offset);
  return { data: result.slice(0, limit), hasMore: result.length === limit + 1 };
}

export async function getUserRatings({
  userId,
  offset,
  limit = 12,
}: {
  userId: string;
  offset: number;
  limit: number;
}) {
  const db = await getConnection();
  const result = await db
    .select({
      isbn: books.isbn,
      book_title: books.book_title,
      book_author: books.book_author,
      year_of_publication: books.year_of_publication,
      publisher: books.publisher,
      image_url_s: books.image_url_s,
      image_url_m: books.image_url_m,
      image_url_l: books.image_url_l,
      avg_rating: ratings.book_rating,
      num_ratings: sql<number>`count(${ratings.book_rating})::int`,
    })
    .from(ratings)
    .innerJoin(books, eq(books.isbn, ratings.isbn))
    .where(eq(ratings.user_id, userId))
    .orderBy(desc(ratings.book_rating))
    .limit(limit + 1)
    .offset(offset);
  return { data: result.slice(0, limit), hasMore: result.length === limit + 1 };
}

export async function searchBooks({
  query,
  offset,
  limit = 12,
}: {
  query: string;
  offset: number;
  limit: number;
}) {
  const db = await getConnection();
  const result = await db
    .select({
      isbn: books.isbn,
      book_title: books.book_title,
      book_author: books.book_author,
      year_of_publication: books.year_of_publication,
      publisher: books.publisher,
      image_url_s: books.image_url_s,
      image_url_m: books.image_url_m,
      image_url_l: books.image_url_l,
      avg_rating: sql<number>`avg(${ratings.book_rating})::float`,
      num_ratings: sql<number>`count(${ratings.book_rating})::int`,
    })
    .from(books)
    .leftJoin(ratings, eq(books.isbn, ratings.isbn))
    .where(query ? ilike(books.book_title, `%${query}%`) : undefined)
    .groupBy(
      books.isbn,
      books.book_title,
      books.book_author,
      books.image_url_s,
      books.image_url_m,
      books.image_url_l
    )
    .orderBy(desc(ratings.book_rating))
    .limit(limit + 1)
    .offset(offset);
  return { data: result.slice(0, limit), hasMore: result.length === limit + 1 };
}

export async function getBookDetails(isbn: string) {
  const db = await getConnection();
  const result = await db
    .select({
      isbn: books.isbn,
      book_title: books.book_title,
      book_author: books.book_author,
      year_of_publication: books.year_of_publication,
      publisher: books.publisher,
      image_url_s: books.image_url_s,
      image_url_m: books.image_url_m,
      image_url_l: books.image_url_l,
      avg_rating: sql<number>`avg(${ratings.book_rating})::float`,
      num_ratings: sql<number>`count(${ratings.book_rating})::int`,
    })
    .from(books)
    .leftJoin(ratings, eq(books.isbn, ratings.isbn))
    .where(eq(books.isbn, isbn))
    .groupBy(
      books.isbn,
      books.book_title,
      books.book_author,
      books.image_url_s,
      books.image_url_m,
      books.image_url_l
    );
  if (result.length === 0) {
    return null;
  }
  const similarBooks = await db
    .select({
      isbn: books.isbn,
      book_title: books.book_title,
      book_author: books.book_author,
      year_of_publication: books.year_of_publication,
      publisher: books.publisher,
      image_url_s: books.image_url_s,
      image_url_m: books.image_url_m,
      image_url_l: books.image_url_l,
    })
    .from(books)
    .where(
      and(eq(books.book_author, result[0]!.book_author), ne(books.isbn, isbn))
    )
    .limit(4);
  return { ...result[0], similarBooks };
}
