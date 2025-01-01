import {
  getBookDetails,
  getRandomUnreadBooks,
} from "@workspace/db/src/queries";
import { Star } from "lucide-react";
import { Button } from "@workspace/ui/components/button";
import { BookmarkIcon, PlayIcon } from "lucide-react";

type Params = Promise<{
  isbn: string;
}>;

export async function generateMetadata({ params }: { params: Params }) {
  const { isbn } = await params;
  const book = await getBookDetails(isbn);
  if (!book) {
    return {
      title: "Book not found",
    };
  }
  return {
    title: `${book.book_title} by ${book.book_author}`,
  };
}

export async function generateStaticParams() {
  const { data: books } = await getRandomUnreadBooks({
    userId: "189835",
    offset: 0,
    limit: 12,
  });
  return books.map((book) => ({
    isbn: book.isbn,
  }));
}

export default async function BookPage({ params }: { params: Params }) {
  const { isbn } = await params;
  const book = await getBookDetails(isbn);
  if (!book) {
    return <div>Book not found</div>;
  }
  return (
    <div className="relative min-h-screen bg-background w-full">
      {/* Hero Section */}
      <div className="relative h-[70vh] w-full">
        {/* Background Image */}
        <div className="absolute inset-0">
          <img
            src={book.image_url_l ?? ""}
            alt={book.book_title}
            className="h-full w-full object-cover"
          />
          <div className="absolute inset-0 bg-gradient-to-t from-background via-background/80 to-transparent" />
        </div>

        {/* Content */}
        <div className="absolute bottom-0 left-0 right-0 p-8 space-y-4">
          <h1 className="text-4xl font-bold">{book.book_title}</h1>
          <div className="flex items-center gap-4">
            <div className="flex items-center">
              <Star className="w-5 h-5 fill-yellow-400 stroke-yellow-400" />
              <span className="ml-1 text-lg">
                {book?.avg_rating?.toFixed(1) ?? "N/A"} (
                {book?.num_ratings ?? "N/A"} ratings)
              </span>
            </div>
            <span className="text-lg">{book.year_of_publication}</span>
          </div>
          <div className="flex gap-4">
            <Button size="lg" className="gap-2">
              <PlayIcon className="w-5 h-5" /> Read Now
            </Button>
            <Button size="lg" variant="secondary" className="gap-2">
              <BookmarkIcon className="w-5 h-5" /> Add to List
            </Button>
          </div>
        </div>
      </div>

      {/* Details Section */}
      <div className="p-8 space-y-8">
        <div className="grid grid-cols-1 md:grid-cols-[2fr,1fr] gap-8">
          {/* Left Column */}
          <div className="space-y-4">
            <h2 className="text-2xl font-semibold">About this book</h2>
            <p className="text-lg text-muted-foreground">
              A book by {book.book_author}, published by {book.publisher} in{" "}
              {book.year_of_publication}.
            </p>
          </div>

          {/* Right Column */}
          <div className="space-y-4">
            <div>
              <span className="text-sm text-muted-foreground">Author</span>
              <p className="text-lg">{book.book_author}</p>
            </div>
            <div>
              <span className="text-sm text-muted-foreground">Publisher</span>
              <p className="text-lg">{book.publisher}</p>
            </div>
            <div>
              <span className="text-sm text-muted-foreground">ISBN</span>
              <p className="text-lg">{book.isbn}</p>
            </div>
          </div>
        </div>

        {/* Similar Books Section */}
        {book.similarBooks.length > 0 && (
          <div className="space-y-4">
            <h2 className="text-2xl font-semibold">
              More by {book.book_author}
            </h2>
            <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-4">
              {book.similarBooks.map((similarBook) => (
                <a
                  key={similarBook.isbn}
                  href={`/book/${similarBook.isbn}`}
                  className="block group"
                >
                  <div className="relative aspect-[2/3] overflow-hidden rounded-lg">
                    <img
                      src={similarBook.image_url_l ?? ""}
                      alt={similarBook.book_title}
                      className="h-full w-full object-cover transition-transform group-hover:scale-105"
                    />
                    <div className="absolute inset-0 bg-gradient-to-t from-black/60 to-transparent opacity-0 group-hover:opacity-100 transition-opacity">
                      <div className="absolute bottom-0 left-0 right-0 p-4">
                        <p className="text-white font-semibold line-clamp-2">
                          {similarBook.book_title}
                        </p>
                      </div>
                    </div>
                  </div>
                </a>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
