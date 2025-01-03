import "server-only";
import { getConnection } from "./drizzle";
import { books, ratings } from "./schema";
import { and, eq, ilike, sql, desc, ne } from "drizzle-orm";
import { performance } from "perf_hooks";

const IS_DEV = process.env.NODE_ENV === "development";

async function explainQuery(
  db: Awaited<ReturnType<typeof getConnection>>,
  query: any,
  name: string
) {
  const explained = await db.execute(sql`EXPLAIN ANALYZE ${query}`);
  console.log(
    `Query Plan for ${name}:`,
    explained.rows.map((row: any) => row["QUERY PLAN"]).join("\n")
  );
  return explained;
}

export async function getRandomUnreadBooks({
  userId,
  offset,
  limit = 12,
}: {
  userId: number;
  offset: number;
  limit: number;
}) {
  const startTime = performance.now();
  const db = await getConnection();
  console.log("Starting getRandomUnreadBooks at ", startTime);
  // First get the unread books with their ratings
  const query = db
    .select({
      isbn: books.isbn,
      book_title: books.book_title,
      book_author: books.book_author,
      image_url_s: books.image_url_s,
      image_url_m: books.image_url_m,
      image_url_l: books.image_url_l,
      //   avg_rating: sql<number>`COALESCE(avg(${ratings.book_rating})::float, 0)`,
      //   num_ratings: sql<number>`count(${ratings.book_rating})::int`,
    })
    .from(books)
    // .leftJoin(ratings, eq(books.isbn, ratings.isbn))
    .where(
      sql`${books.isbn} NOT IN (
        SELECT isbn FROM ${ratings}
        WHERE user_id = ${userId}
      )`
    )
    // .groupBy(
    //   books.isbn,
    //   books.book_title,
    //   books.book_author,
    //   books.image_url_s,
    //   books.image_url_m,
    //   books.image_url_l
    // )
    // .orderBy(sql`avg(${ratings.book_rating}) DESC NULLS LAST`)
    .offset(offset)
    .limit(limit + 1);
  if (IS_DEV) {
    await explainQuery(db, query, "getRandomUnreadBooks");
  }
  const result = await query;
  const endTime = performance.now();
  console.log(`getRandomUnreadBooksTime: ${endTime - startTime}ms`);
  return { data: result.slice(0, limit), hasMore: result.length === limit + 1 };
}

export async function getUserRatings({
  userId,
  offset,
  limit = 12,
}: {
  userId: number;
  offset: number;
  limit: number;
}) {
  const startTime = performance.now();
  const db = await getConnection();
  const query = db
    .select({
      isbn: books.isbn,
      book_title: books.book_title,
      book_author: books.book_author,
      year_of_publication: books.year_of_publication,
      publisher: books.publisher,
      image_url_s: books.image_url_s,
      image_url_m: books.image_url_m,
      image_url_l: books.image_url_l,
      avg_rating: sql`sorted_ratings.book_rating`,
    })
    .from(
      db
        .select()
        .from(ratings)
        .where(eq(ratings.user_id, userId))
        .orderBy(desc(ratings.book_rating))
        .offset(offset)
        .limit(limit + 1)
        .as("sorted_ratings")
    )
    .innerJoin(books, eq(books.isbn, sql`sorted_ratings.isbn`));

  if (IS_DEV) {
    await explainQuery(db, query, "getUserRatings");
  }
  const result = await query;
  const endTime = performance.now();
  console.log(`getUserRatingsTime: ${endTime - startTime}ms`);
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
  const startTime = performance.now();
  const db = await getConnection();
  const dbQuery = db
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
    .where(query ? ilike(books.book_title, `%${query}%`) : undefined)
    // .where(query ? ilike(books.book_title_truncated, `%${query}%`) : undefined)
    .offset(offset)
    .limit(limit + 1);
  if (IS_DEV) {
    await explainQuery(db, dbQuery, "searchBooks");
  }
  const result = await dbQuery;
  const endTime = performance.now();
  console.log(`searchBooksTime: ${endTime - startTime}ms`);
  return { data: result.slice(0, limit), hasMore: result.length === limit + 1 };
}

export async function getBookDetails(isbn: string) {
  const startTime = performance.now();
  const db = await getConnection();
  const dbQuery = db
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
    .where(eq(books.isbn, isbn));
  if (IS_DEV) {
    await explainQuery(db, dbQuery, "getBookDetails");
  }
  const result = await dbQuery;
  const getBookDetailsTime = performance.now() - startTime;
  console.log(`getBookDetailsTime: ${getBookDetailsTime}ms`);
  if (result.length === 0) {
    return null;
  }
  return { ...result[0] };
}

export async function getSimilarBooks(isbn: string) {
  const db = await getConnection();
  const dbQuery = db
    .select({
      isbn: books.isbn,
      book_title: books.book_title,
      book_author: books.book_author,
      image_url_s: books.image_url_s,
      image_url_m: books.image_url_m,
      image_url_l: books.image_url_l,
    })
    .from(books)
    .where(
      and(
        eq(
          books.book_author,
          db
            .select({ book_author: books.book_author })
            .from(books)
            .where(eq(books.isbn, isbn))
            .limit(1)
        ),
        ne(books.isbn, isbn)
      )
    )
    .limit(4);
  if (IS_DEV) {
    await explainQuery(db, dbQuery, "getSimilarBooks");
  }
  const result = await dbQuery;
  return result;
}

export async function getBookAverageRating(isbn: string) {
  const db = await getConnection();
  const dbQuery = db
    .select({
      avg_rating: sql<number>`avg(${ratings.book_rating})::float`,
      num_ratings: sql<number>`count(${ratings.book_rating})::int`,
    })
    .from(ratings)
    .where(eq(ratings.isbn, isbn));
  if (IS_DEV) {
    await explainQuery(db, dbQuery, "getBookAverageRating");
  }
  const result = await dbQuery;
  return result[0]!;
}
