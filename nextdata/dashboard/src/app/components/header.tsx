import { HealthCheckIndicator } from "./health-check";

export function Header() {
  return (
    <div className="flex items-center justify-between p-4 border-b">
      <h1 className="text-2xl font-bold tracking-tight">NextData Dashboard</h1>
      <HealthCheckIndicator />
    </div>
  );
}
