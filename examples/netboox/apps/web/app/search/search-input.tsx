"use client";

import { Input } from "@workspace/ui/components/input";
import { useQueryStates } from "nuqs";
import { useDebounce } from "@workspace/ui/hooks/use-debounce";
import { useEffect, useState } from "react";
import { searchQueryParsers } from "./searchParams";

export function SearchInput() {
  const [{ q }, setQuery] = useQueryStates(searchQueryParsers);
  const [value, setValue] = useState(q ?? "");
  const debouncedValue = useDebounce(value, 300);

  useEffect(() => {
    setQuery(
      { q: debouncedValue || null },
      {
        history: "push",
        shallow: false,
      }
    );
  }, [debouncedValue, setQuery]);

  return (
    <Input
      type="search"
      placeholder="Search by title..."
      value={value}
      onChange={(e) => setValue(e.target.value)}
      className="w-full"
    />
  );
}
