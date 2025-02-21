import { elizaLogger, UUID, Service, ServiceType } from '@elizaos/core';
import duckdb from 'duckdb';
import { TextEncoder } from 'util';
import { createEmbedding } from './embedding.service.ts';
import { ParquetReader } from '@dsnp/parquetjs'; // If you are actually using `ParquetReader`
import { writeParquetToBuffer } from './stream.service.ts';
import { ChainName, getChain, testnet } from '@recallnet/chains';
import { AccountInfo } from '@recallnet/sdk/account';
import { ListResult } from '@recallnet/sdk/bucket';
import { RecallClient, walletClientFromPrivateKey } from '@recallnet/sdk/client';
import { CreditAccount } from '@recallnet/sdk/credit';
import { Address, Hex, parseEther, TransactionReceipt } from 'viem';
import { ICotAgentRuntime } from '../../types/index.ts';

type Result<T = unknown> = {
  result: T;
  meta?: {
    tx?: TransactionReceipt;
  };
};

// Type for the raw log entry before processing
export type RawLogEntry = {
  userId: string;
  agentId: string;
  userMessage: string;
  log: string;
};

// Type for the processed log record with embedding and metadata
export type LogRecord = {
  // Core identification fields
  userId: string;
  agentId: string;

  // Message content
  userMessage: string;
  log: string;

  // Vector embedding for similarity search
  embedding: number[];

  // Metadata
  timestamp: string;
  logFileKey?: string; // Optional as it's added during DuckDB insertion
};

// Type for search results including similarity score
export type LogSearchResult = Omit<LogRecord, 'embedding'> & {
  similarityScore: number;
};

// Type for the record as stored in DuckDB
export type DuckDBLogRecord = LogRecord & {
  logFileKey: string; // Required in DuckDB storage
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
  private runtime: ICotAgentRuntime;
  private syncInterval: NodeJS.Timeout | undefined;
  private alias: string;
  private prefix: string;
  private db: duckdb.Connection;
  private processedFiles: Set<string> = new Set();
  private intervalMs: number;
  private batchSizeKB: number;

  getInstance(): RecallService {
    return RecallService.getInstance();
  }

  async initialize(_runtime: ICotAgentRuntime): Promise<void> {
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
      // ✅ Initialize DuckDB
      const db = new duckdb.Database(':memory:'); // In-memory DB for performance
      this.db = db.connect();
      // ✅ Create logs table if it doesn't exist
      await new Promise<void>((resolve, reject) => {
        this.db.run(
          `CREATE TABLE logs (
              userId TEXT,
              agentId TEXT,
              userMessage TEXT,
              log TEXT,
              embedding FLOAT[], 
              timestamp TEXT,
              logFileKey TEXT,
              UNIQUE(userId, agentId, timestamp, logFileKey)
          );
          
          CREATE TABLE processed_files (
              fileKey TEXT PRIMARY KEY,
              processedAt TEXT
          );`,
          (err) => {
            if (err) reject(err);
            else resolve();
          },
        );
      });

      // Load processed files into memory
      await this.loadProcessedFiles();

      await this.startPeriodicSync();
      // Use user-defined sync interval and batch size, if provided
      this.intervalMs = intervalPeriod ? parseInt(intervalPeriod, 10) : 2 * 60 * 1000;
      this.batchSizeKB = batchSize ? parseInt(batchSize, 10) : 4;
      this.startPeriodicSync(this.intervalMs, this.batchSizeKB);
      elizaLogger.success('RecallService initialized successfully, starting periodic sync.');
    } catch (error) {
      elizaLogger.error(`Error initializing RecallService: ${error.message}`);
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
   * Loads processed files from DuckDB into memory.
   * @returns A promise that resolves when the files are loaded.
   */
  private async loadProcessedFiles(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.db.all('SELECT fileKey FROM processed_files;', (err, rows: { fileKey: string }[]) => {
        if (err) {
          reject(err);
          return;
        }
        this.processedFiles = new Set(rows.map((row) => row.fileKey));
        resolve();
      });
    });
  }

  /**
   * Marks a file as processed in both DuckDB and memory
   * @param fileKey The key of the file to mark as processed.
   * @returns A promise that resolves when the file is marked as processed.
   */
  private async markFileAsProcessed(fileKey: string): Promise<void> {
    if (this.processedFiles.has(fileKey)) {
      return; // Already processed
    }

    await new Promise<void>((resolve, reject) => {
      this.db.run(
        'INSERT INTO processed_files (fileKey, processedAt) VALUES (?, ?);',
        fileKey,
        new Date().toISOString(),
        (err) => {
          if (err) reject(err);
          else {
            this.processedFiles.add(fileKey);
            resolve();
          }
        },
      );
    });
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
      throw error;
    }
  }

  /**
   * Stores a batch of logs to Recall.
   * @param bucketAddress The address of the bucket to store logs.
   * @param batch The batch of logs to store.
   * @returns The key under which the logs were stored.
   */
  async storeBatchToRecall(bucketAddress: Address, batch: string[]): Promise<string | undefined> {
    try {
      const timestamp = Date.now();
      const nextLogKey = `${this.prefix}${timestamp}.parquet`;

      // Process logs for Parquet
      const logs: LogRecord[] = await Promise.all(
        batch.map(async (jsonlEntry, index) => {
          try {
            const parsed: RawLogEntry = JSON.parse(jsonlEntry);

            return {
              userId: parsed.userId || '',
              agentId: parsed.agentId || '',
              userMessage: String(parsed.userMessage || ''),
              log: JSON.stringify(parsed.log),
              embedding: Array.from(await createEmbedding(parsed.log)),
              timestamp: new Date().toISOString(),
            };
          } catch (error) {
            elizaLogger.error(`Error processing log entry ${index}: ${error.message}`);
            return null;
          }
        }),
      );

      // Filter out failed logs
      const validLogs = logs.filter((log) => log !== null);
      if (validLogs.length === 0) {
        elizaLogger.warn('No valid logs to store.');
        return undefined;
      }

      const parquetBuffer = await writeParquetToBuffer(validLogs);
      if (!parquetBuffer || parquetBuffer.length === 0) {
        elizaLogger.warn('Generated Parquet file is empty. Skipping upload.');
        return undefined;
      }

      // Upload to Recall
      const addObject = await this.withTimeout(
        this.client.bucketManager().add(bucketAddress, nextLogKey, parquetBuffer),
        30000, // 30-second timeout
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
      elizaLogger.error(`Error storing logs as Parquet in Recall: ${error.message}`);
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
      const bucketAddress = await this.withTimeout(
        this.getOrCreateBucket(bucketAlias),
        15000,
        'Get/Create bucket',
      );

      const unsyncedLogs = await this.runtime.databaseAdapter.getUnsyncedLogs();
      const filteredLogs = unsyncedLogs.filter((log) => log.type === 'chain-of-thought');

      if (filteredLogs.length === 0) {
        elizaLogger.info('No unsynced logs to process.');
        return;
      }

      elizaLogger.info(`Found ${filteredLogs.length} unsynced logs.`);

      let batch: string[] = [];
      let batchSize = 0;
      let syncedLogIds: UUID[] = [];
      let failedLogIds: UUID[] = [];

      for (const log of filteredLogs) {
        try {
          const parsedLog = JSON.parse(log.body);
          const jsonlEntry = JSON.stringify({
            userId: parsedLog.userId,
            agentId: parsedLog.agentId,
            userMessage: parsedLog.userMessage,
            log: parsedLog.log,
          });

          const logSize = new TextEncoder().encode(jsonlEntry).length;
          elizaLogger.info(`Processing log entry of size: ${logSize} bytes`);
          elizaLogger.info(`New batch size: ${batchSize + logSize} bytes`);
          if (batchSize + logSize > batchSizeKB * 1024) {
            elizaLogger.info(
              `Batch size ${
                batchSize + logSize
              } bytes exceeds ${batchSizeKB} KB limit. Attempting sync...`,
            );
            const logFileKey = await this.storeBatchToRecall(bucketAddress, batch);

            if (logFileKey) {
              await this.runtime.databaseAdapter.markLogsAsSynced(syncedLogIds);
              elizaLogger.info(`Successfully synced batch of ${syncedLogIds.length} logs`);
            } else {
              failedLogIds.push(...syncedLogIds);
              elizaLogger.warn(
                `Failed to sync batch of ${syncedLogIds.length} logs - will retry on next sync`,
              );
            }
            batch = [];
            batchSize = 0;
            syncedLogIds = [];
          }

          batch.push(jsonlEntry);
          batchSize += logSize;
          syncedLogIds.push(log.id);
        } catch (error) {
          elizaLogger.error(`Error processing log entry ${log.id}: ${error.message}`);
        }
      }
      if (batch.length > 0) {
        // notify the user that the batch size was not exceeded
        elizaLogger.info(
          `Batch size ${batchSize} bytes did not exceed ${batchSizeKB} KB limit. Will recheck on next sync cycle.`,
        );
      }

      if (failedLogIds.length > 0) {
        elizaLogger.warn(
          `Sync attempt finished. ${failedLogIds.length} logs failed to upload and remain unsynced. Will retry next cycle.`,
        );
      } else {
        const logSyncInterval =
          this.intervalMs < 60000
            ? `${this.intervalMs / 1000} seconds`
            : `${this.intervalMs / 1000 / 60} minutes`;
        elizaLogger.info(`Sync cycle complete. Next sync in ${logSyncInterval}.`);
      }
    } catch (error) {
      if (error.message.includes('timed out')) {
        elizaLogger.error(`Recall sync operation timed out: ${error.message}`);
      } else {
        elizaLogger.error(`Error in syncLogsToRecall: ${error.message}`);
      }
    }
  }

  /**
   * Inserts a log into DuckDB.
   * @param record The log record to insert.
   * @param logFileKey The key of the log file in Recall.
   * @param conn The DuckDB connection to use.
   * @returns A promise that resolves when the log is inserted.
   * @throws An error if the log cannot be inserted.
   **/
  private async insertLogIntoDuckDB(
    record: LogRecord,
    logFileKey: string,
    conn: duckdb.Connection,
  ): Promise<void> {
    if (record.embedding && Array.isArray(record.embedding)) {
      const embeddingArray = record.embedding.map((num: number) => parseFloat(num as any)); // Ensure it's a FLOAT[]

      try {
        await new Promise<void>((resolve, reject) => {
          conn.run(
            `INSERT INTO logs 
             VALUES (?, ?, ?, ?, CAST(? AS FLOAT[]), ?, ?)
             ON CONFLICT (userId, agentId, timestamp, logFileKey) DO NOTHING;`,
            record.userId,
            record.agentId,
            record.userMessage,
            record.log,
            JSON.stringify(embeddingArray),
            record.timestamp,
            logFileKey,
            (err) => (err ? reject(err) : resolve()),
          );
        });
      } catch (error) {
        if (!error.message.includes('UNIQUE constraint')) {
          elizaLogger.error(`Error inserting log: ${error.message}`);
          throw error;
        }
      }
    }
  }

  /**
   * Retrieve and order all chain-of-thought logs from Recall.
   * @param bucketAlias The alias of the bucket to query.
   * @returns An array of ordered chain-of-thought logs.
   */

  async retrieveOrderedChainOfThoughtLogs(
    bucketAlias: string,
    queryText: string,
  ): Promise<LogSearchResult[]> {
    try {
      const bucketAddress = await this.getOrCreateBucket(bucketAlias);
      elizaLogger.info(`Retrieving chain-of-thought logs from bucket: ${bucketAddress}`);

      // Query for Parquet files
      const queryResult = await this.client
        .bucketManager()
        .query(bucketAddress, { prefix: this.prefix });

      if (!queryResult.result?.objects.length) {
        elizaLogger.info(`No chain-of-thought logs found in bucket: ${bucketAlias}`);
        return [];
      }

      // Filter for unprocessed Parquet files
      const unprocessedFiles = queryResult.result.objects
        .map((obj) => obj.key)
        .filter((key) => key.endsWith('.parquet'))
        .filter((key) => !this.processedFiles.has(key));

      elizaLogger.info(
        `Found ${unprocessedFiles.length} unprocessed files out of ${
          queryResult.result.objects.length
        } total files`,
      );

      // Process only new files
      for (const logFile of unprocessedFiles) {
        try {
          elizaLogger.info(`Processing new file: ${logFile}`);
          const logData = await this.client.bucketManager().get(bucketAddress, logFile);

          if (!logData.result) {
            elizaLogger.warn(`Invalid or empty result for log file: ${logFile}`);
            continue;
          }

          let parquetBuffer: Buffer;
          if (Buffer.isBuffer(logData.result)) {
            parquetBuffer = logData.result;
          } else if (Array.isArray(logData.result) || logData.result instanceof Uint8Array) {
            parquetBuffer = Buffer.from(logData.result);
          } else if (typeof logData.result === 'object') {
            parquetBuffer = Buffer.from(Object.values(logData.result) as number[]);
          } else {
            throw new Error(`Invalid logData.result format for log file: ${logFile}`);
          }

          const reader = await ParquetReader.openBuffer(parquetBuffer);
          const cursor = reader.getCursor();

          let record;
          while ((record = await cursor.next())) {
            await this.insertLogIntoDuckDB(record, logFile, this.db);
          }

          await reader.close();
          await this.markFileAsProcessed(logFile);
        } catch (error) {
          elizaLogger.error(`Error processing file ${logFile}: ${error.message}`);
        }
      }

      // Generate embedding for query
      const queryEmbedding = new Float32Array(await createEmbedding(queryText));
      if (!queryEmbedding || queryEmbedding.length === 0) {
        elizaLogger.warn('Failed to generate embedding for query text.');
        return [];
      }

      const queryEmbeddingArray = `ARRAY[${[...queryEmbedding].join(',')}]`;

      // Perform similarity search on all stored logs
      const searchResults: any[] = await new Promise((resolve, reject) => {
        this.db.all(
          `SELECT DISTINCT userId, agentId, userMessage, log, timestamp,
              1 - (embedding <-> ${queryEmbeddingArray}) AS similarity 
           FROM logs 
           ORDER BY similarity DESC 
           LIMIT 5;`,
          (err, res) => {
            if (err) reject(err);
            else resolve(res);
          },
        );
      });

      if (!searchResults || searchResults.length === 0) {
        elizaLogger.warn('No matching logs found.');
        return [];
      }

      // Sort and return results
      const sortedLogs = searchResults
        .map((log) => ({
          ...log,
          similarityScore: parseFloat(log.similarity),
        }))
        .sort((a, b) => b.similarityScore - a.similarityScore);

      elizaLogger.info(`Returning ${sortedLogs.length} most relevant logs.`);

      return sortedLogs.map((log) => ({
        userId: log.userId,
        agentId: log.agentId,
        userMessage: log.userMessage,
        log: log.log,
        timestamp: log.timestamp,
        similarityScore: log.similarityScore,
      }));
    } catch (error) {
      elizaLogger.error(`Error retrieving ordered logs: ${error.message}`);
      return [];
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
