"use client";

import { Button } from "@/components/ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import type { EmrJob } from "@/lib/db";
import { useMutation, useQuery } from "@tanstack/react-query";
import { toast } from "sonner";

interface TableJobsProps {
  data_table: string;
}

async function fetchJobs(tableName: string): Promise<EmrJob[]> {
  const response = await fetch(
    `http://localhost:8000/api/table/${tableName}/jobs`
  );
  if (!response.ok) {
    throw new Error("Failed to fetch jobs");
  }
  return response.json();
}

async function triggerJob(jobName: string): Promise<{ jobRunId: string }> {
  const formData = new FormData();
  formData.append("job_name", jobName);

  const response = await fetch("http://localhost:8000/api/jobs/trigger", {
    method: "POST",
    body: formData,
  });

  if (!response.ok) {
    throw new Error("Failed to trigger job");
  }

  return response.json();
}

export function TableJobs({ data_table }: TableJobsProps) {
  const {
    data: jobs = [],
    isLoading,
    error,
  } = useQuery({
    queryKey: ["jobs", data_table],
    queryFn: () => fetchJobs(data_table),
  });

  const jobMutation = useMutation({
    mutationFn: triggerJob,
    onSuccess: (data) => {
      toast.success(`Job started with run ID: ${data.jobRunId}`);
    },
    onError: (error) => {
      toast.error("Failed to start job");
      console.error(error);
    },
  });

  if (error) {
    return (
      <div className="p-4 text-center text-red-500">
        Failed to load jobs. Please try again.
      </div>
    );
  }

  return (
    <div className="rounded-md border">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Name</TableHead>
            <TableHead>Type</TableHead>
            <TableHead>Connection</TableHead>
            <TableHead>Actions</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {jobs.map((job) => (
            <TableRow key={job.id}>
              <TableCell>{job.name}</TableCell>
              <TableCell>{job.jobType}</TableCell>
              <TableCell>{job.connectionName || "None"}</TableCell>
              <TableCell>
                <Button
                  variant="secondary"
                  size="sm"
                  onClick={() => jobMutation.mutate(job.name)}
                  disabled={jobMutation.isPending}
                >
                  {jobMutation.isPending ? "Running..." : "Run Job"}
                </Button>
              </TableCell>
            </TableRow>
          ))}
          {!isLoading && jobs.length === 0 && (
            <TableRow>
              <TableCell colSpan={4} className="text-center">
                No jobs found for this table
              </TableCell>
            </TableRow>
          )}
          {isLoading && (
            <TableRow>
              <TableCell colSpan={4} className="text-center">
                Loading jobs...
              </TableCell>
            </TableRow>
          )}
        </TableBody>
      </Table>
    </div>
  );
}
