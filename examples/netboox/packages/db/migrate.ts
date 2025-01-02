import dotenv from "dotenv";
import path from "path";
import { migrate } from "drizzle-orm/node-postgres/migrator";
import { closeConnection, getConnection } from "./src/drizzle";

dotenv.config();

async function main() {
  const db = await getConnection();
  console.log("db", db);
  console.log("migrationsFolder", path.join(process.cwd(), "migrations"));
  await migrate(db, {
    migrationsFolder: path.join(process.cwd(), "migrations"),
  });
  console.log(`Migrations complete`);
  await closeConnection();
}

main();
