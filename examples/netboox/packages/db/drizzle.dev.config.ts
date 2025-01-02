import type { Config } from "drizzle-kit";
import * as dotenv from "dotenv";

// Load environment variables
dotenv.config();

console.log("token: ", process.env.PGPASSWORD);

export default {
  schema: "./src/schema.ts",
  out: "./migrations",
  dialect: "postgresql",
  dbCredentials: {
    host: process.env.DB_CLUSTER_ENDPOINT!,
    user: "admin",
    password: process.env.PGPASSWORD!,
    database: "postgres",
    port: 5432,
    ssl: true,
  },
} satisfies Config;
