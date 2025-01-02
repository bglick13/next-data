import type {
  fetchMoreRandomBooks,
  fetchMoreSearchResults,
  fetchMoreUserRatings,
} from "@/app/actions";

type UserRatingBook = Awaited<
  ReturnType<typeof fetchMoreUserRatings>
>["data"][number];
type RandomBook = Awaited<
  ReturnType<typeof fetchMoreRandomBooks>
>["data"][number];
type SearchBook = Awaited<
  ReturnType<typeof fetchMoreSearchResults>
>["data"][number];

interface UserRatingsProps {
  uiContext: "library";
  initialBooks: UserRatingBook[];
  hasMore: boolean;
  fetchMore: (
    page: number
  ) => Promise<Awaited<ReturnType<typeof fetchMoreUserRatings>>>;
}

interface AverageRatingProps {
  uiContext: "explore";
  initialBooks: RandomBook[];
  hasMore: boolean;
  fetchMore: (
    page: number
  ) => Promise<Awaited<ReturnType<typeof fetchMoreRandomBooks>>>;
}

interface SearchProps {
  uiContext: "search";
  initialBooks: SearchBook[];
  hasMore: boolean;
  fetchMore: (
    page: number
  ) => Promise<Awaited<ReturnType<typeof fetchMoreSearchResults>>>;
}

export type Props = UserRatingsProps | AverageRatingProps | SearchProps;

export type BookCardProps = {
  book: UserRatingBook | RandomBook | SearchBook;
  uiContext: Props["uiContext"];
};
