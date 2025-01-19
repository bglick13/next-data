"use client";

import { useQuery } from "@tanstack/react-query";

import { useState } from "react";
import { CheckCircle, AlertCircle, HelpCircle } from "lucide-react";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import { Button } from "@/components/ui/button";

type HealthStatus = "healthy" | "unhealthy" | "unknown";

interface PulumiResource {
  urn: string;
  custom: boolean;
  type: string;
  // biome-ignore lint/suspicious/noExplicitAny: <explanation>
  inputs: { [key: string]: any };
  // biome-ignore lint/suspicious/noExplicitAny: <explanation>
  outputs?: { [key: string]: any };
  created: string;
  modified: string;
}

interface StackOutputs {
  project_name: string;
  stack_name: string;
  resources: PulumiResource[];
  table_bucket: PulumiResource;
  table_namespace: PulumiResource;
  tables: PulumiResource[];
}

interface HealthCheckResponse {
  status: HealthStatus;
  pulumi_stack: string;
  stack_outputs: StackOutputs;
}

export function HealthCheckIndicator() {
  const { data, isLoading, error } = useQuery<HealthCheckResponse>({
    queryKey: ["health"],
    queryFn: () =>
      fetch("http://localhost:8000/api/health").then((res) => res.json()),
  });
  const [open, setOpen] = useState(false);

  const overallStatus: HealthStatus = data?.status || "unknown";

  const statusIcon = {
    healthy: (
      <div className="h-2 w-2 rounded-full bg-green-500 shadow-[0_0_10px_rgba(34,197,94,0.5)] " />
    ),
    unhealthy: (
      <div className="h-2 w-2 rounded-full bg-red-500 shadow-[0_0_10px_rgba(239,68,68,0.5)] " />
    ),
    unknown: (
      <div className="h-2 w-2 rounded-full bg-yellow-500 shadow-[0_0_10px_rgba(234,179,8,0.5)] " />
    ),
  };

  const statusText = {
    healthy: "All systems operational",
    unhealthy: "Some systems are experiencing issues",
    unknown: "System status unknown",
  };

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          aria-label="Health Check Status"
          variant="ghost"
          className="p-2 hover:bg-transparent rounded-full"
        >
          {statusIcon[overallStatus]}
        </Button>
      </PopoverTrigger>
      <PopoverContent className="w-80">
        <div className="grid gap-4">
          <div className="space-y-2">
            <h4 className="font-medium leading-none">
              {statusText[overallStatus]}
            </h4>
            <p className="text-sm text-muted-foreground">
              Detailed health check information
            </p>
          </div>
          <div className="grid gap-2">
            <h4 className="font-medium leading-none">Stack Name</h4>
            <p className="text-sm text-muted-foreground">
              {data?.stack_outputs?.stack_name}
            </p>
            <h4 className="font-medium leading-none">Project Name</h4>
            <p className="text-sm text-muted-foreground">
              {data?.stack_outputs?.project_name}
            </p>
            <h4 className="font-medium leading-none"># of Resources</h4>
            <p className="text-sm text-muted-foreground">
              {data?.stack_outputs?.resources?.length}
            </p>
            <h4 className="font-medium leading-none">Table Bucket</h4>
            <p className="text-sm text-muted-foreground">
              {data?.stack_outputs?.table_bucket?.outputs?.name}
            </p>
            <h4 className="font-medium leading-none">Table Namespace</h4>
            <p className="text-sm text-muted-foreground">
              {data?.stack_outputs?.table_namespace?.outputs?.namespace}
            </p>
            <h4 className="font-medium leading-none">Tables</h4>
            <p className="text-sm text-muted-foreground">
              {data?.stack_outputs?.tables?.length}
            </p>
          </div>
        </div>
      </PopoverContent>
    </Popover>
  );
}
