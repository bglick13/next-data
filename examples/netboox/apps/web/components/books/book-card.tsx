import { Card, CardContent } from "@workspace/ui/components/card";
import { Star } from "lucide-react";
import Link from "next/link";
import { BookCardProps } from "./types";

export function BookCard({ book }: BookCardProps) {
  return (
    <Link href={`/book/${book.isbn}`}>
      <Card className="overflow-hidden transition-all hover:scale-105 hover:shadow-xl">
        <CardContent className="p-0">
          <div className="relative aspect-[2/3]">
            <img
              src={book.image_url_l ?? ""}
              alt={`Cover of ${book.book_title}`}
              className="object-cover w-full h-full"
            />
            <div className="absolute inset-0 bg-gradient-to-t from-black to-transparent opacity-0 hover:opacity-100 transition-opacity duration-300">
              <div className="absolute bottom-0 left-0 right-0 p-4 text-white">
                <h3 className="text-lg font-bold truncate">
                  {book.book_title}
                </h3>
                <p className="text-sm truncate">{book.book_author}</p>
                <div className="flex items-center mt-1">
                  <Star className="w-4 h-4 fill-yellow-400 stroke-yellow-400 mr-1" />
                </div>
              </div>
            </div>
          </div>
        </CardContent>
      </Card>
    </Link>
  );
}
