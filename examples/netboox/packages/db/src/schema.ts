import {
  pgTable,
  integer,
  timestamp,
  index,
  text,
  uniqueIndex,
} from "drizzle-orm/pg-core";

export const books = pgTable(
  "books",
  {
    isbn: text("isbn").primaryKey(),
    book_title: text("book_title").notNull(),
    book_title_truncated: text("book_title_truncated").notNull(),
    book_author: text("book_author").notNull().default(""),
    year_of_publication: integer("year_of_publication"),
    publisher: text("publisher").notNull().default(""),
    image_url_s: text("image_url_s"),
    image_url_m: text("image_url_m"),
    image_url_l: text("image_url_l"),
    createdAt: timestamp("created_at").notNull().defaultNow(),
  },
  (table) => [
    index("books_isbn_idx").on(table.isbn),
    index("books_book_title_idx").on(table.book_title),
    index("books_book_author_idx").on(table.book_author),
  ]
);

export const users = pgTable(
  "users",
  {
    user_id: integer("user_id").primaryKey(), // From the dataset
    location: text("location"),
    age: integer("age"),
    createdAt: timestamp("created_at").notNull().defaultNow(),
    updatedAt: timestamp("updated_at").notNull().defaultNow(),
  },
  (table) => [index("users_user_id_idx").on(table.user_id)]
);

export const ratings = pgTable(
  "ratings",
  {
    user_id: integer("user_id")
      .notNull()
      .references(() => users.user_id),
    isbn: text("isbn")
      .notNull()
      .references(() => books.isbn),
    book_rating: integer("book_rating").notNull(),
    createdAt: timestamp("created_at").notNull().defaultNow(),
  },
  (table) => [
    index("ratings_user_id_idx").on(table.user_id),
    index("ratings_isbn_idx").on(table.isbn),
    uniqueIndex("ratings_book_user_idx").on(table.user_id, table.isbn),
  ]
);
