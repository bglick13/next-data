import type { Config } from "drizzle-kit";

export default {
  schema: "./src/schema.ts",
  out: "./migrations",
  dialect: "postgresql",
  dbCredentials: {
    host: process.env.DB_CLUSTER_ENDPOINT!,
    user: "admin",
    password: process.env.DB_TOKEN!,
    database: "postgres",
    port: 5432,
    ssl: true,
  },
} satisfies Config;
