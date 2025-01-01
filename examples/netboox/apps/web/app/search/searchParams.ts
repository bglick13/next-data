import { parseAsString, createSearchParamsCache } from "nuqs/server";

export const searchQueryParsers = {
  q: parseAsString,
};

export const searchQueryCache = createSearchParamsCache(searchQueryParsers);
