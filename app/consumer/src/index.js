import os from "os";
import pkg from "pg";

const { Pool } = pkg;

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
});

const podName = process.env.HOSTNAME || os.hostname() || "unknown";
console.log(`Job Consumer ${podName} started`);


const sleep = (ms) => new Promise((r)=> setTimeout(r, ms));

const run = async () => {
  while (true) {
    const client = await pool.connect();

    try {
      await client.query("BEGIN");

      //used a lock to pick one job at a time, and delete it immediately to prevent other consumers from picking the same job
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
        // No jobs available, wait and retry
        await client.query("COMMIT");
        console.log(`[${podName}] No jobs, waiting...`);
        await sleep(2000);
        continue;
      }

      console.log(`[${podName}] Picked job ${rows[0]}`);

      const { id, payload } = rows[0];
      console.log(`[${podName}] Processing job ${id}:`, payload);

      await sleep(2000);

      await client.query("INSERT INTO done_jobs (id) VALUES ($1)", [id]);

      await client.query("COMMIT");
      console.log(`[${podName}] Completed job ${id}`);
    } catch (e) {
      try {
        await client.query("ROLLBACK");
      } catch {}
      console.error(`[${podName}] Error:`, e);
      await sleep(2000);
    } finally {
      // Release the client back to the pool
      client.release();
    }
  }
};

run().catch((e) => {
  console.error("Fatal:", e);
  process.exit(1);
});
