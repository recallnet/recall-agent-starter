import { elizaLogger } from '@elizaos/core';
import { v4, UUID } from 'uuid';

export async function logMemoryPostgres(
  db: any,
  params: {
    userId: UUID;
    agentId: UUID;
    roomId: UUID;
    type: string;
    body: string;
  },
): Promise<void> {
  // First ensure the room exists
  const roomCheck = await db.query('SELECT id FROM rooms WHERE id = $1', [params.roomId]);
  if (roomCheck.rows.length === 0) {
    // If room doesn't exist, create it
    await db.query('INSERT INTO rooms (id) VALUES ($1)', [params.roomId]);
  }

  // Now we can safely insert the log
  await db.query(
    `INSERT INTO logs (id, body, "userId", "agentId", "roomId", type, "isSynced")
       VALUES ($1, $2, $3, $4, $5, $6, FALSE)`,
    [v4(), params.body, params.userId, params.agentId, params.roomId, params.type],
  );
}

export async function logMemorySqlite(
  db: any,
  params: {
    userId: UUID;
    agentId: UUID;
    roomId: UUID;
    type: string;
    body: string;
  },
): Promise<void> {
  const sql = `
            INSERT INTO logs (id, userId, agentId, roomId, type, body, isSynced)
            VALUES (?, ?, ?, ?, ?, ?, 0)
        `;
  db.prepare(sql).run(v4(), params.userId, params.agentId, params.roomId, params.type, params.body);
}

export async function getUnsyncedLogsPostgres(db: any): Promise<
  {
    id: UUID;
    body: string;
    userId: UUID;
    agentId: UUID | null;
    roomId: UUID;
    type: string;
    createdAt: Date;
  }[]
> {
  const { rows } = await db.query(
    `SELECT id, body, "userId", "agentId", "roomId", type, "createdAt"
         FROM logs WHERE "isSynced" = FALSE
         ORDER BY "createdAt" ASC
         LIMIT 100`,
  );

  return rows.map((row) => ({
    ...row,
    body: typeof row.body === 'string' ? row.body : JSON.stringify(row.body),
  }));
}

export async function markLogsAsSyncedPostgres(db: any, logIds: UUID[]): Promise<void> {
  if (logIds.length === 0) {
    elizaLogger.warn('⚠ No log IDs provided for marking as synced.');
    return;
  }

  elizaLogger.info(`✅ Marking logs as synced: ${JSON.stringify(logIds)}`);

  const placeholders = logIds.map((_, i) => `$${i + 1}`).join(', ');
  try {
    await db.query(`UPDATE logs SET "isSynced" = TRUE WHERE id IN (${placeholders})`, logIds);
    elizaLogger.info(`✅ Successfully marked ${logIds.length} logs as synced.`);
  } catch (error) {
    elizaLogger.error(`❌ Failed to mark logs as synced: ${error.message}`, {
      logIds,
      error: error instanceof Error ? error.message : String(error),
    });
  }
}

export async function getUnsyncedLogsSqlite(db: any): Promise<{ id: UUID; body: string }[]> {
  const sql = 'SELECT id, type, body FROM logs WHERE isSynced = 0 ORDER BY createdAt ASC';
  return db.prepare(sql).all() as { id: UUID; body: string }[];
}

export async function markLogsAsSyncedSqlite(db: any, logIds: UUID[]): Promise<void> {
  if (logIds.length === 0) return;
  const placeholders = logIds.map(() => '?').join(', ');
  const sql = `UPDATE logs SET isSynced = 1 WHERE id IN (${placeholders})`;
  db.prepare(sql).run(...logIds);
}
