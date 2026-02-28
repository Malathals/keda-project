import os from "os";
import pkg from "pg";

const { Pool } = pkg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL, 
});

const podName = process.env.HOSTNAME || os.hostname() || "unknown";
console.log(`Job Consumer ${podName} started`);

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function run() {
  while (true) {
    const client = await pool.connect();

    try {
      await client.query("BEGIN");

      // Pick + lock 1 job, then delete it and return payload
      const { rows } = await client.query(`
        WITH picked AS (
          SELECT id, payload
          FROM jobs
          ORDER BY created_at
          LIMIT 1
          FOR UPDATE SKIP LOCKED
        )
        DELETE FROM jobs j
        USING picked
        WHERE j.id = picked.id
        RETURNING picked.id, picked.payload;
      `);

      if (rows.length === 0) {
        await client.query("COMMIT");
        client.release();
        await sleep(1000);
        continue;
      }

      const { id, payload } = rows[0];
      console.log(`[${podName}] Processing job ${id}:`, payload);

      // Simulate random processing time (2-10 seconds)
      const processMs = 2000 + Math.random() * 8000;
      await sleep(processMs);

      // Mark as done
      await client.query("INSERT INTO done_jobs (id) VALUES ($1)", [id]);

      await client.query("COMMIT");
      console.log(
        `[${podName}] Completed job ${id} in ${(processMs / 1000).toFixed(2)}s`
      );
    } catch (e) {
      try {
        await client.query("ROLLBACK");
      } catch {}
      console.error(`[${podName}] Error:`, e);
      await sleep(2000);
    } finally {
      client.release();
    }
  }
}

run().catch((e) => {
  console.error("Fatal:", e);
  process.exit(1);
});