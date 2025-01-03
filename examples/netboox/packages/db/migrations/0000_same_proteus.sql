CREATE TABLE "books" (
	"isbn" text PRIMARY KEY NOT NULL,
	"book_title" text NOT NULL,
	"book_title_truncated" text NOT NULL,
	"book_author" text DEFAULT '' NOT NULL,
	"year_of_publication" integer,
	"publisher" text DEFAULT '' NOT NULL,
	"image_url_s" text,
	"image_url_m" text,
	"image_url_l" text,
	"created_at" timestamp DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "ratings" (
	"user_id" integer NOT NULL,
	"isbn" text NOT NULL,
	"book_rating" integer NOT NULL,
	"created_at" timestamp DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "users" (
	"user_id" integer PRIMARY KEY NOT NULL,
	"location" text,
	"age" integer,
	"created_at" timestamp DEFAULT now() NOT NULL,
	"updated_at" timestamp DEFAULT now() NOT NULL
);
--> statement-breakpoint
-- Book indexes
CREATE INDEX "books_isbn_idx" ON books(isbn);--> statement-breakpoint
CREATE INDEX "books_book_title_idx" ON books(book_title_truncated);--> statement-breakpoint
CREATE INDEX "books_book_author_idx" ON books(book_author);--> statement-breakpoint
CREATE INDEX idx_books_details ON books (isbn, book_title_truncated, book_author, year_of_publication, publisher, image_url_s, image_url_m, image_url_l);
-- Rating indexes
CREATE INDEX "ratings_user_id_idx" ON ratings(user_id);--> statement-breakpoint
CREATE INDEX "ratings_isbn_idx" ON ratings(isbn);--> statement-breakpoint
CREATE UNIQUE INDEX "ratings_book_user_idx" ON ratings(user_id, isbn);--> statement-breakpoint
CREATE INDEX idx_ratings_user_book_rating ON ratings (user_id, book_rating DESC, isbn);

-- User indexes
CREATE INDEX "users_user_id_idx" ON users(user_id);--> statement-breakpoint
