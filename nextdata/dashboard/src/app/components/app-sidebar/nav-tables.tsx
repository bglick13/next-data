"use client";

import {
  SidebarMenuItem,
  SidebarMenuSub,
  SidebarMenuSubItem,
  SidebarMenuSubButton,
} from "@/components/ui/sidebar";
import { SidebarMenu } from "@/components/ui/sidebar";
import { useDataDirectories } from "@/hooks/queries/useDataDirectories";
import Link from "next/link";

export function NavTables() {
  const { data } = useDataDirectories();
  console.log(data);
  return (
    <SidebarMenuSub>
      {data.directories.map((directory) => (
        <SidebarMenuSubItem key={directory.name}>
          <SidebarMenuSubButton asChild>
            <Link href={`/table/${directory.name}`} className="capitalize">
              {directory.name}
            </Link>
          </SidebarMenuSubButton>
        </SidebarMenuSubItem>
      ))}
    </SidebarMenuSub>
  );
}
