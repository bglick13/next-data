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

const healthChecks = [
  { name: "API", status: "healthy" },
  { name: "Database", status: "healthy" },
  { name: "Storage", status: "unhealthy" },
  { name: "Authentication", status: "healthy" },
];

interface HealthCheckResponse {
  status: HealthStatus;
  pulumi_stack: string;
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
            {healthChecks.map((check) => (
              <div
                key={check.name}
                className="grid grid-cols-2 items-center gap-4"
              >
                <span className="text-sm font-medium">{check.name}</span>
                <span
                  className={`justify-self-end text-sm ${
                    check.status === "healthy"
                      ? "text-green-500"
                      : "text-red-500"
                  }`}
                >
                  {check.status === "healthy"
                    ? "Operational"
                    : "Issues Detected"}
                </span>
              </div>
            ))}
          </div>
        </div>
      </PopoverContent>
    </Popover>
  );
}
