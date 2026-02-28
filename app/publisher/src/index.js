import express from "express";
import pkg from "pg";
import crypto from "crypto";

const { Pool } = pkg;

const app = express();
app.use(express.json());

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

const randomText = crypto.randomBytes(20).toString("hex");
const randomEventId = crypto.randomUUID();

const publisherJob = async () => {
  console.log(
    `Publishing event ${randomEventId} with random text: ${randomText}`,
  );

  while (true) {
    try {
      await pool.query("INSERT INTO events(type, payload) VALUES($1, $2)", [
        type,
        JSON.stringify({ randomText, randomEventId }),
      ]);
    } catch (err) {
      console.error("Error publishing event:", err);
    } finally {
      setTimeout(() => {
        console.log(
          `Published event done with ${randomEventId} & random text: ${randomText}`,
        );
      }, 1000);
    }

    publisherJob();
  }
};
