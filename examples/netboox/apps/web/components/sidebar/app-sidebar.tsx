import { Button } from "@workspace/ui/components/button";
import { Input } from "@workspace/ui/components/input";
import {
  Sidebar,
  SidebarContent,
  SidebarGroup,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
} from "@workspace/ui/components/sidebar";
import { BookIcon, HomeIcon, SearchIcon } from "lucide-react";

const navigationItems = [
  {
    title: "Home",
    href: "/",
    icon: HomeIcon,
    activeMatch: "^/(home)?$",
  },
  {
    title: "Library",
    href: "/library",
    icon: BookIcon,
    activeMatch: "/library",
  },
  {
    title: "Search",
    href: "/search",
    icon: SearchIcon,
    activeMatch: "/search",
  },
];

export function AppSidebar() {
  return (
    <Sidebar>
      <SidebarHeader>Netboox</SidebarHeader>
      <SidebarContent>
        <SidebarGroup>
          <SidebarGroupLabel>Navigation</SidebarGroupLabel>
          <SidebarMenu>
            {navigationItems.map((item) => (
              <SidebarMenuItem key={item.href}>
                <SidebarMenuButton asChild>
                  <a href={item.href}>
                    <item.icon />
                    <span>{item.title}</span>
                  </a>
                </SidebarMenuButton>
              </SidebarMenuItem>
            ))}
          </SidebarMenu>
        </SidebarGroup>
        <SidebarGroup>
          <SidebarGroupLabel>Dev Tools</SidebarGroupLabel>
          <SidebarMenu>
            <SidebarMenuItem className="flex items-center gap-2 justify-between">
              <Input type="text" placeholder="User Id" />
              <Button variant="outline" size="sm">
                Go
              </Button>
            </SidebarMenuItem>
          </SidebarMenu>
        </SidebarGroup>
      </SidebarContent>
    </Sidebar>
  );
}
