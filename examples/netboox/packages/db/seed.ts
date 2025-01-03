import fs from "fs";
import path from "path";
import csv from "csv-parser";
import { seed } from "drizzle-seed";
import { closeConnection, getConnection } from "./src/drizzle";
import { books, ratings, users } from "./src/schema";
import { config } from "dotenv";

config();

async function parseBooksFromCSV(): Promise<(typeof books.$inferInsert)[]> {
  const booksToInsert: (typeof books.$inferInsert)[] = [];
  const csvFilePath = path.resolve(__dirname, "data/Books_cleaned.csv");

  return new Promise((resolve, reject) => {
    fs.createReadStream(csvFilePath)
      .pipe(csv())
      .on("data", (row) => {
        const title = row.book_title?.trim();
        if (title) {
          booksToInsert.push({
            isbn: row.isbn,
            book_title: row.book_title,
            book_title_truncated:
              row.book_title.length > 200
                ? row.book_title.substring(0, 200)
                : row.book_title,
            book_author: row.book_author,
            year_of_publication: row.year_of_publication,
            publisher: row.publisher,
            image_url_s: row.image_url_s,
            image_url_m: row.image_url_m,
            image_url_l: row.image_url_l,
          });
        }
      })
      .on("end", () => {
        console.log(`Parsed ${booksToInsert.length} books from CSV.`);
        resolve(booksToInsert);
      })
      .on("error", (error) => {
        console.error("Error reading CSV file:", error);
        reject(error);
      });
  });
}

async function parseUsersFromCSV(): Promise<(typeof users.$inferInsert)[]> {
  const usersToInsert: (typeof users.$inferInsert)[] = [];
  const userMap: Map<string, (typeof users.$inferInsert)[]> = new Map();
  const csvFilePath = path.resolve(__dirname, "data/Users_cleaned.csv");

  return new Promise((resolve, reject) => {
    fs.createReadStream(csvFilePath)
      .pipe(csv())
      .on("data", (row) => {
        const userId = row.user_id?.trim();
        if (userId) {
          const user = {
            user_id: parseInt(userId),
            location: row.location,
            age: row.age ? parseInt(row.age) : undefined,
          };

          if (!userMap.has(userId)) {
            userMap.set(userId, []);
          }
          userMap.get(userId)!.push(user);
          usersToInsert.push(user);
        }
      })
      .on("end", () => {
        console.log(`Parsed ${usersToInsert.length} users from CSV.`);
        resolve(usersToInsert);
      })
      .on("error", (error) => {
        console.error("Error reading CSV file:", error);
        reject(error);
      });
  });
}

async function parseRatingsFromCSV(): Promise<(typeof ratings.$inferInsert)[]> {
  const ratingsToInsert: (typeof ratings.$inferInsert)[] = [];
  const insertedRatings: Set<string[]> = new Set();
  const csvFilePath = path.resolve(__dirname, "data/Ratings_cleaned.csv");

  return new Promise((resolve, reject) => {
    fs.createReadStream(csvFilePath)
      .pipe(csv())
      .on("data", (row) => {
        const userId = row.user_id?.trim();
        const isbn = row.isbn?.trim();
        if (userId && isbn && !insertedRatings.has([userId, isbn])) {
          insertedRatings.add([userId, isbn]);
          ratingsToInsert.push({
            user_id: parseInt(userId),
            isbn: isbn,
            book_rating: row.book_rating ? parseInt(row.book_rating) : 0,
          });
        }
      })
      .on("end", () => {
        console.log(`Parsed ${ratingsToInsert.length} ratings from CSV.`);
        resolve(ratingsToInsert);
      })
      .on("error", (error) => {
        console.error("Error reading CSV file:", error);
        reject(error);
      });
  });
}

async function main() {
  const db = await getConnection();
  const insertChunkSize = 1000;

  const booksToInsert = await parseBooksFromCSV();
  for (let i = 0; i < booksToInsert.length; i += insertChunkSize) {
    const chunk = booksToInsert.slice(i, i + insertChunkSize);
    try {
      await db.insert(books).values(chunk);
    } catch (error) {
      console.error("Error inserting books:", error);
      console.error("First problematic record:", chunk[0]);
      console.error("Last problematic record:", chunk[chunk.length - 1]);
      throw error;
    }
  }
  const usersToInsert = await parseUsersFromCSV();
  for (let i = 0; i < usersToInsert.length; i += insertChunkSize) {
    const chunk = usersToInsert.slice(i, i + insertChunkSize);
    try {
      await db.insert(users).values(chunk);
    } catch (error) {
      console.error("Error inserting users:", error);
      console.error("First problematic record:", chunk[0]);
      console.error("Last problematic record:", chunk[chunk.length - 1]);
      throw error;
    }
  }

  const ratingsToInsert = await parseRatingsFromCSV();
  for (let i = 0; i < ratingsToInsert.length; i += insertChunkSize) {
    const chunk = ratingsToInsert.slice(i, i + insertChunkSize);
    try {
      await db.insert(ratings).values(chunk);
    } catch (error) {
      console.error("Error inserting ratings:", error);
      console.error("First problematic record:", chunk[0]);
      console.error("Last problematic record:", chunk[chunk.length - 1]);
      throw error;
    }
  }

  await closeConnection();
  process.exit();
}

main().catch((error) => {
  console.error("Seeding failed:", error);
  if (error.detail) {
    console.error(error.detail);
  }
  process.exit(1);
});
