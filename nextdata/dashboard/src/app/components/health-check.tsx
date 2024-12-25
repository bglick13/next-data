"use client";

import { useQuery } from "@tanstack/react-query";

import { useState } from "react";
import { CheckCircle, AlertCircle, HelpCircle } from "lucide-react";
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";

type HealthStatus = "healthy" | "unhealthy" | "unknown";

interface PulumiResource {
  urn: string;
  custom: boolean;
  type: string;
  inputs: { [key: string]: any };
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
    healthy: <CheckCircle className="h-6 w-6 text-green-500" />,
    unhealthy: <AlertCircle className="h-6 w-6 text-red-500" />,
    unknown: <HelpCircle className="h-6 w-6 text-yellow-500" />,
  };

  const statusText = {
    healthy: "All systems operational",
    unhealthy: "Some systems are experiencing issues",
    unknown: "System status unknown",
  };

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <button
          className="flex items-center space-x-2 rounded-full bg-white p-2 shadow-md hover:bg-gray-100 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2"
          aria-label="Health Check Status"
        >
          {statusIcon[overallStatus]}
        </button>
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
              {data?.stack_outputs.stack_name}
            </p>
            <h4 className="font-medium leading-none">Project Name</h4>
            <p className="text-sm text-muted-foreground">
              {data?.stack_outputs.project_name}
            </p>
            <h4 className="font-medium leading-none"># of Resources</h4>
            <p className="text-sm text-muted-foreground">
              {data?.stack_outputs.resources.length}
            </p>
            <h4 className="font-medium leading-none">Table Bucket</h4>
            <p className="text-sm text-muted-foreground">
              {data?.stack_outputs.table_bucket.outputs?.name}
            </p>
            <h4 className="font-medium leading-none">Table Namespace</h4>
            <p className="text-sm text-muted-foreground">
              {data?.stack_outputs.table_namespace.outputs?.namespace}
            </p>
            <h4 className="font-medium leading-none">Tables</h4>
            <p className="text-sm text-muted-foreground">
              {data?.stack_outputs.tables.length}
            </p>
          </div>
        </div>
      </PopoverContent>
    </Popover>
  );
}
