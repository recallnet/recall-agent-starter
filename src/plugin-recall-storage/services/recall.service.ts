import {
  elizaLogger,
  UUID,
  Service,
  ServiceType,
  DatabaseAdapter,
  IAgentRuntime,
} from '@elizaos/core';
import { ChainName, getChain, testnet } from '@recallnet/chains';
import { AccountInfo } from '@recallnet/sdk/account';
import { ListResult } from '@recallnet/sdk/bucket';
import { RecallClient, walletClientFromPrivateKey } from '@recallnet/sdk/client';
import { CreditAccount } from '@recallnet/sdk/credit';
import { Address, Hex, parseEther, TransactionReceipt } from 'viem';
import {
  getUnsyncedLogsPostgres,
  getUnsyncedLogsSqlite,
  markLogsAsSyncedPostgres,
  markLogsAsSyncedSqlite,
} from '../utils.ts';

type Result<T = unknown> = {
  result: T;
  meta?: {
    tx?: TransactionReceipt;
  };
};

const privateKey = process.env.RECALL_PRIVATE_KEY as Hex;
const envAlias = process.env.RECALL_BUCKET_ALIAS as string;
const envPrefix = process.env.RECALL_COT_LOG_PREFIX as string;
const network = process.env.RECALL_NETWORK as string;
const intervalPeriod = process.env.RECALL_SYNC_INTERVAL as string;
const batchSize = process.env.RECALL_BATCH_SIZE as string;

export class RecallService extends Service {
  static serviceType: ServiceType = 'recall' as ServiceType;
  private client: RecallClient;
  private runtime: IAgentRuntime;
  private syncInterval: NodeJS.Timeout | undefined;
  private alias: string;
  private prefix: string;
  private intervalMs: number;
  private batchSizeKB: number;

  getInstance(): RecallService {
    return RecallService.getInstance();
  }

  async initialize(_runtime: IAgentRuntime): Promise<void> {
    try {
      if (!privateKey) {
        throw new Error('RECALL_PRIVATE_KEY is required');
      }
      if (!envAlias) {
        throw new Error('RECALL_BUCKET_ALIAS is required');
      }
      if (!envPrefix) {
        throw new Error('RECALL_COT_LOG_PREFIX is required');
      }
      const chain = network ? getChain(network as ChainName) : testnet;
      const wallet = walletClientFromPrivateKey(privateKey, chain);
      this.client = new RecallClient({ walletClient: wallet });
      this.alias = envAlias;
      this.prefix = envPrefix;
      this.runtime = _runtime;
      // Use user-defined sync interval and batch size, if provided
      this.intervalMs = intervalPeriod ? parseInt(intervalPeriod, 10) : 2 * 60 * 1000;
      this.batchSizeKB = batchSize ? parseInt(batchSize, 10) : 4;

      // Ensure isSynced column exists in logs table
      await this.ensureRequiredColumns();

      this.startPeriodicSync(this.intervalMs, this.batchSizeKB);
      elizaLogger.success('RecallService initialized successfully, starting periodic sync.');
    } catch (error) {
      elizaLogger.error(`Error initializing RecallService: ${error.message}`);
    }
  }

  /**
   * Ensures that the isSynced column exists in the logs table.
   * If it doesn't exist, it adds the column with a default value of false.
   */
  private async ensureRequiredColumns(): Promise<void> {
    try {
      const db: any = this.runtime.databaseAdapter;

      // First, check for isSynced column
      await this.ensureColumn(db, 'isSynced', 'BOOLEAN', 'INTEGER', 'FALSE', '0');

      // Then, check for agentId column
      await this.ensureColumn(db, 'agentId', 'TEXT', 'TEXT', 'NULL', 'NULL');
    } catch (error) {
      elizaLogger.error(`Error ensuring required columns: ${error.message}`);
      throw error;
    }
  }

  private async ensureColumn(
    db: any,
    columnName: string,
    pgType: string,
    sqliteType: string,
    pgDefault: string,
    sqliteDefault: string,
  ): Promise<void> {
    try {
      // Check if the column exists
      let columnExists = false;

      if ('pool' in db) {
        // PostgreSQL
        const result = await db.pool.query(
          `
          SELECT column_name 
          FROM information_schema.columns 
          WHERE table_name='logs' AND column_name=$1
        `,
          [columnName],
        );
        columnExists = result.rowCount > 0;
      } else if ('db' in db) {
        // SQLite
        const result = db.db.prepare(`PRAGMA table_info(logs)`).all();
        columnExists = result.some((col) => col.name === columnName);
      } else {
        throw new Error('Unsupported database adapter');
      }

      // Add the column if it doesn't exist
      if (!columnExists) {
        elizaLogger.info(`Adding ${columnName} column to logs table`);

        if ('pool' in db) {
          // PostgreSQL
          await db.pool.query(`
            ALTER TABLE logs 
            ADD COLUMN "${columnName}" ${pgType} DEFAULT ${pgDefault}
          `);
        } else if ('db' in db) {
          // SQLite
          await db.db
            .prepare(
              `
            ALTER TABLE logs 
            ADD COLUMN ${columnName} ${sqliteType} DEFAULT ${sqliteDefault}
          `,
            )
            .run();
        }

        elizaLogger.info(`Successfully added ${columnName} column to logs table`);
      } else {
        elizaLogger.info(`${columnName} column already exists in logs table`);
      }
    } catch (error) {
      elizaLogger.error(`Error ensuring ${columnName} column: ${error.message}`);
      throw error;
    }
  }
  /**
   * Utility function to handle timeouts for async operations.
   * @param promise The promise to execute.
   * @param timeoutMs The timeout in milliseconds.
   * @param operationName The name of the operation for logging.
   * @returns The result of the promise.
   */
  async withTimeout<T>(promise: Promise<T>, timeoutMs: number, operationName: string): Promise<T> {
    let timeoutId: NodeJS.Timeout;

    const timeoutPromise = new Promise<T>((_, reject) => {
      timeoutId = setTimeout(() => {
        reject(new Error(`${operationName} operation timed out after ${timeoutMs}ms`));
      }, timeoutMs);
    });

    try {
      const result = await Promise.race([promise, timeoutPromise]);
      clearTimeout(timeoutId!);
      return result;
    } catch (error) {
      clearTimeout(timeoutId!);
      throw error;
    }
  }

  /**
   * Gets the account information for the current user.
   * @returns The account information.
   */
  public async getAccountInfo(): Promise<AccountInfo> | undefined {
    try {
      const info = await this.client.accountManager().info();
      return info.result;
    } catch (error) {
      elizaLogger.error(`Error getting account info: ${error.message}`);
      throw error;
    }
  }

  /**
   * Lists all buckets in Recall.
   * @returns The list of buckets.
   */
  public async listBuckets(): Promise<ListResult> | undefined {
    try {
      const info = await this.client.bucketManager().list();
      return info.result;
    } catch (error) {
      elizaLogger.error(`Error listing buckets: ${error.message}`);
      throw error;
    }
  }

  /**
   * Gets the credit information for the account.
   * @returns The credit information.
   */
  public async getCreditInfo(): Promise<CreditAccount> | undefined {
    try {
      const info = await this.client.creditManager().getAccount();
      return info.result;
    } catch (error) {
      elizaLogger.error(`Error getting credit info: ${error.message}`);
      throw error;
    }
  }

  /**
   * Buys credit for the account.
   * @param amount The amount of credit to buy.
   * @returns The result of the buy operation.
   */
  public async buyCredit(amount: string): Promise<Result> {
    try {
      const info = await this.client.creditManager().buy(parseEther(amount));
      return info; // Return the full Result object
    } catch (error) {
      elizaLogger.error(`Error buying credit: ${error.message}`);
      throw error;
    }
  }

  /**
   * Gets or creates a log bucket in Recall.
   * @param bucketAlias The alias of the bucket to retrieve or create.
   * @returns The address of the log bucket.
   */
  public async getOrCreateBucket(bucketAlias: string): Promise<Address> {
    try {
      elizaLogger.info(`Looking for bucket with alias: ${bucketAlias}`);

      // Try to find the bucket by alias
      const buckets = await this.client.bucketManager().list();
      if (buckets?.result) {
        const bucket = buckets.result.find((b) => b.metadata?.alias === bucketAlias);
        if (bucket) {
          elizaLogger.info(`Found existing bucket "${bucketAlias}" at ${bucket.addr}`);
          return bucket.addr; // Return existing bucket address
        } else {
          elizaLogger.info(`Bucket with alias "${bucketAlias}" not found, creating a new one.`);
        }
      }

      // Ensure bucketAlias is correctly passed during creation
      const query = await this.client.bucketManager().create({
        metadata: { alias: bucketAlias },
      });

      const newBucket = query.result;
      if (!newBucket) {
        elizaLogger.error(`Failed to create new bucket with alias: ${bucketAlias}`);
        throw new Error(`Failed to create bucket: ${bucketAlias}`);
      }

      elizaLogger.info(`Successfully created new bucket "${bucketAlias}" at ${newBucket.bucket}`);
      return newBucket.bucket;
    } catch (error) {
      elizaLogger.error(`Error in getOrCreateBucket: ${error.message}`);
      throw error;
    }
  }

  /**
   * Adds an object to a bucket.
   * @param bucket The address of the bucket.
   * @param key The key under which to store the object.
   * @param data The data to store (string, File, or Uint8Array).
   * @param options Optional parameters:
   *   - overwrite: Whether to overwrite existing object with same key (default: false)
   *   - ttl: Time-to-live in seconds (must be >= MIN_TTL if specified)
   *   - metadata: Additional metadata key-value pairs
   * @returns A Result object containing:
   *   - result: Empty object ({})
   *   - meta: Optional metadata including transaction receipt
   * @throws {InvalidValue} If object size exceeds MAX_OBJECT_SIZE or TTL is invalid
   * @throws {ActorNotFound} If the bucket or actor is not found
   * @throws {AddObjectError} If the object addition fails
   */
  public async addObject(
    bucket: Address,
    key: string,
    data: string | File | Uint8Array,
    options?: { overwrite?: boolean },
  ): Promise<Result> {
    try {
      const info = await this.client.bucketManager().add(bucket, key, data, {
        overwrite: options?.overwrite ?? false,
      });
      return info; // Return the full Result object
    } catch (error) {
      elizaLogger.error(`Error adding object: ${error.message}`);
      throw error;
    }
  }

  /**
   * Gets an object from a bucket.
   * @param bucket The address of the bucket.
   * @param key The key under which the object is stored.
   * @returns The data stored under the specified key.
   */
  public async getObject(bucket: Address, key: string): Promise<Uint8Array | undefined> {
    try {
      const info = await this.client.bucketManager().get(bucket, key);
      return info.result;
    } catch (error) {
      elizaLogger.warn(`Error getting object: ${error.message}`);
      // Return undefined instead of throwing to allow graceful handling of missing objects
      return undefined;
    }
  }

  /**
   * Fetches unsynchronized logs based on the isSynced field.
   * @returns Array of logs that haven't been synced yet.
   */
  async getUnsyncedLogs(): Promise<any[]> {
    try {
      const db: any = this.runtime.databaseAdapter;

      if ('pool' in db) {
        // PostgreSQL
        return await getUnsyncedLogsPostgres(db.pool);
      } else if ('db' in db) {
        // SQLite
        return await getUnsyncedLogsSqlite(db.db);
      } else {
        throw new Error('Unsupported database adapter');
      }
    } catch (error) {
      elizaLogger.error(`Error getting unsynced logs: ${error.message}`);
      return [];
    }
  }

  /**
   * Marks logs as synced in the database.
   * @param logIds The IDs of the logs to mark as synced.
   * @returns Whether the operation was successful.
   */
  async markLogsAsSynced(logIds: string[]): Promise<boolean> {
    if (logIds.length === 0) {
      return true;
    }

    try {
      const db: any = this.runtime.databaseAdapter;

      if ('pool' in db) {
        // PostgreSQL
        await markLogsAsSyncedPostgres(db.pool, logIds);
      } else if ('db' in db) {
        // SQLite
        await markLogsAsSyncedSqlite(db.db, logIds);
      } else {
        throw new Error('Unsupported database adapter');
      }

      return true;
    } catch (error) {
      elizaLogger.error(`Error marking logs as synced: ${error.message}`);
      return false;
    }
  }

  async runRawQuery<R>(dbAdapter: DatabaseAdapter, query: string, params?: any[]): Promise<R[]> {
    if ('pool' in dbAdapter) {
      // PostgreSQL (uses pool.query)
      return (await (dbAdapter as any).pool.query(query, params)).rows as R[];
    } else if ('db' in dbAdapter) {
      // SQLite (uses prepare + all)
      return (dbAdapter as any).db.prepare(query).all(...(params || [])) as R[];
    } else {
      throw new Error('Unsupported database adapter');
    }
  }

  /**
   * Stores a batch of logs to Recall.
   * @param bucketAddress The address of the bucket to store logs.
   * @param batch The batch of logs to store.
   * @param timestamp The timestamp to use in the key.
   * @returns The key under which the logs were stored.
   */
  async storeBatchToRecall(
    bucketAddress: Address,
    batch: string[],
    timestamp: string,
  ): Promise<string | undefined> {
    try {
      const nextLogKey = `${this.prefix}${timestamp}.jsonl`;
      const batchData = batch.join('\n');

      // Add 30 second timeout to the add operation
      const addObject = await this.withTimeout(
        this.client
          .bucketManager()
          .add(bucketAddress, nextLogKey, new TextEncoder().encode(batchData)),
        30000, // 30 second timeout
        'Recall batch storage',
      );

      if (!addObject?.meta?.tx) {
        // Check for transaction receipt instead of result
        elizaLogger.error('Recall API returned invalid response for batch storage');
        return undefined;
      }

      elizaLogger.info(`Successfully stored batch at key: ${nextLogKey}`);
      return nextLogKey;
    } catch (error) {
      if (error.message.includes('timed out')) {
        elizaLogger.error(`Recall API timed out while storing batch`);
      } else {
        elizaLogger.error(`Error storing JSONL logs in Recall: ${error.message}`);
      }
      return undefined;
    }
  }

  /**
   * Syncs logs to Recall in batches.
   * @param bucketAlias The alias of the bucket to store logs.
   * @param batchSizeKB The maximum size of each batch in kilobytes.
   */
  async syncLogsToRecall(bucketAlias: string, batchSizeKB = 4): Promise<void> {
    try {
      // Add timeout to bucket creation/retrieval
      const bucketAddress = await this.withTimeout(
        this.getOrCreateBucket(bucketAlias),
        15000, // 15 second timeout
        'Get/Create bucket',
      );

      // Get logs that haven't been synced yet
      const unsyncedLogs = await this.getUnsyncedLogs();

      if (unsyncedLogs.length === 0) {
        elizaLogger.info('No unsynced logs to process.');
        return;
      }

      elizaLogger.info(`Found ${unsyncedLogs.length} unsynced logs.`);

      let batch: string[] = [];
      let batchSize = 0;
      let processedLogIds: string[] = [];
      let batchTimestamp = Date.now().toString();

      // Sort logs by createdAt to ensure we process in chronological order
      unsyncedLogs.sort(
        (a, b) => new Date(a.createdAt).getTime() - new Date(b.createdAt).getTime(),
      );

      for (const log of unsyncedLogs) {
        try {
          const parsedLog = JSON.parse(log.body);
          const jsonlEntry = JSON.stringify({
            userId: parsedLog.userId,
            agentId: parsedLog.agentId,
            userMessage: parsedLog.userMessage,
            log: parsedLog.log,
            createdAt: log.createdAt, // Include createdAt in the stored log
          });

          const logSize = new TextEncoder().encode(jsonlEntry).length;
          elizaLogger.info(`Processing log entry of size: ${logSize} bytes`);
          elizaLogger.info(`New batch size: ${batchSize + logSize} bytes`);

          // If this log would make the batch exceed the size limit, store the current batch first
          if (batchSize > 0 && batchSize + logSize > batchSizeKB * 1024) {
            elizaLogger.info(
              `Batch size ${batchSize + logSize} bytes exceeds ${batchSizeKB} KB limit. Attempting sync...`,
            );

            const logFileKey = await this.storeBatchToRecall(bucketAddress, batch, batchTimestamp);

            if (logFileKey) {
              elizaLogger.info(`Successfully synced batch of ${batch.length} logs`);

              // Mark logs as synced
              if (processedLogIds.length > 0) {
                const success = await this.markLogsAsSynced(processedLogIds);
                if (!success) {
                  elizaLogger.warn(`Failed to mark logs as synced - will retry on next sync`);
                }
              }
            } else {
              elizaLogger.warn(
                `Failed to sync batch of ${batch.length} logs - will retry on next sync`,
              );
              // Don't clear processedLogIds if sync failed
              continue;
            }

            // Start a new batch
            batch = [];
            batchSize = 0;
            processedLogIds = [];
            batchTimestamp = Date.now().toString();
          }

          batch.push(jsonlEntry);
          batchSize += logSize;
          processedLogIds.push(log.id);
        } catch (error) {
          elizaLogger.error(`Error processing log entry ${log.id}: ${error.message}`);
        }
      }

      // Store any remaining logs in the batch
      if (batch.length > 0) {
        elizaLogger.info(`Storing final batch of ${batch.length} logs (${batchSize} bytes)`);
        const logFileKey = await this.storeBatchToRecall(bucketAddress, batch, batchTimestamp);

        if (logFileKey) {
          // Mark logs as synced
          if (processedLogIds.length > 0) {
            const success = await this.markLogsAsSynced(processedLogIds);
            if (!success) {
              elizaLogger.warn(`Failed to mark logs as synced - will retry on next sync`);
            }
          }
        } else {
          elizaLogger.warn(`Failed to sync final batch of ${batch.length} logs`);
        }
      }

      const logSyncInterval =
        this.intervalMs < 60000
          ? `${this.intervalMs / 1000} seconds`
          : `${this.intervalMs / 1000 / 60} minutes`;
      elizaLogger.info(`Sync cycle complete. Next sync in ${logSyncInterval}.`);
    } catch (error) {
      if (error.message.includes('timed out')) {
        elizaLogger.error(`Recall sync operation timed out: ${error.message}`);
      } else {
        elizaLogger.error(`Error in syncLogsToRecall: ${error.message}`);
      }
    }
  }

  /**
   * Retrieve and order all chain-of-thought logs from Recall.
   * @param bucketAlias The alias of the bucket to query.
   * @returns An array of ordered chain-of-thought logs.
   */
  async retrieveOrderedChainOfThoughtLogs(bucketAlias: string): Promise<any[]> {
    try {
      const bucketAddress = await this.getOrCreateBucket(bucketAlias);
      elizaLogger.info(`Retrieving chain-of-thought logs from bucket: ${bucketAddress}`);

      // Query all objects with the designated prefix
      const queryResult = await this.client
        .bucketManager()
        .query(bucketAddress, { prefix: this.prefix }); // Remove the extra '/'

      if (!queryResult.result?.objects.length) {
        elizaLogger.info(`No chain-of-thought logs found in bucket: ${bucketAlias}`);
        return [];
      }

      // Extract log filenames and sort by timestamp
      const logFiles = queryResult.result.objects
        .map((obj) => obj.key)
        .filter((key) => key.startsWith(this.prefix) && key.endsWith('.jsonl'))
        .sort((a, b) => {
          // Extract timestamps by removing prefix and .jsonl extension
          const timeA = parseInt(a.slice(this.prefix.length, -6), 10);
          const timeB = parseInt(b.slice(this.prefix.length, -6), 10);
          return timeA - timeB;
        });

      elizaLogger.info(`Retrieving ${logFiles.length} ordered chain-of-thought logs...`);

      let allLogs: any[] = [];

      // Download and parse each log file
      for (const logFile of logFiles) {
        try {
          const logData = await this.client.bucketManager().get(bucketAddress, logFile);
          if (!logData.result) continue;

          // Decode and split JSONL content
          const decodedLogs = new TextDecoder().decode(logData.result).trim().split('\n');
          const parsedLogs = decodedLogs.map((line) => JSON.parse(line));

          allLogs.push(...parsedLogs);
        } catch (error) {
          elizaLogger.error(`Error retrieving log file ${logFile}: ${error.message}`);
        }
      }

      // Sort logs by createdAt if available, otherwise maintain file order
      allLogs.sort((a, b) => {
        if (a.createdAt && b.createdAt) {
          return new Date(a.createdAt).getTime() - new Date(b.createdAt).getTime();
        }
        return 0;
      });

      elizaLogger.info(
        `Successfully retrieved and ordered ${allLogs.length} chain-of-thought logs.`,
      );
      return allLogs;
    } catch (error) {
      elizaLogger.error(`Error retrieving ordered chain-of-thought logs: ${error.message}`);
      throw error;
    }
  }

  /**
   * Starts the periodic log syncing.
   * @param intervalMs The interval in milliseconds for syncing logs.
   * @param batchSizeKB The maximum size of each batch in kilobytes.
   */
  public startPeriodicSync(intervalMs = 2 * 60 * 1000, batchSizeKB = 4): void {
    if (this.syncInterval) {
      elizaLogger.warn('Log sync is already running.');
      return;
    }

    elizaLogger.info('Starting periodic log sync...');
    this.syncInterval = setInterval(async () => {
      try {
        await this.syncLogsToRecall(this.alias, batchSizeKB);
      } catch (error) {
        elizaLogger.error(`Periodic log sync failed: ${error.message}`);
      }
    }, intervalMs);

    // Perform an immediate sync on startup
    this.syncLogsToRecall(this.alias, batchSizeKB).catch((error) =>
      elizaLogger.error(`Initial log sync failed: ${error.message}`),
    );
  }

  /**
   * Stops the periodic log syncing.
   */
  public stopPeriodicSync(): void {
    if (this.syncInterval) {
      clearInterval(this.syncInterval);
      this.syncInterval = undefined;
      elizaLogger.info('Stopped periodic log syncing.');
    }
  }
}
