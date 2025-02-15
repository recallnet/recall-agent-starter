import {
  RecallClient,
  walletClientFromPrivateKey,
} from '../../../../js-recall/packages/sdk/dist/client.mjs';
import { testnet } from '../../../../js-recall/packages/sdk/dist/chains.mjs';
import {
  // @ts-expect-error this is temporary
  CreditAccount,
  // @ts-expect-error this is temporary
  BuyResult,
  // @ts-expect-error this is temporary
  ListResult,
  // @ts-expect-error this is temporary
  AddObjectResult,
} from '../../../../js-recall/packages/sdk/dist/entities.mjs';
import { elizaLogger, UUID, Service, ServiceType } from '@elizaos/core';
import { parseEther } from 'viem';
import { ICotAgentRuntime } from '../../types/index.ts';

type Address = `0x${string}`;
type AccountInfo = {
  address: Address;
  nonce: number;
  balance: bigint;
  parentBalance?: bigint;
};

const privateKey = process.env.RECALL_PRIVATE_KEY as `0x${string}`;
const envAlias = process.env.RECALL_BUCKET_ALIAS as string;
const envPrefix = process.env.COT_LOG_PREFIX as string;

export class RecallService extends Service {
  static serviceType: ServiceType = 'recall' as ServiceType;
  // @ts-expect-error this is temporary
  private client: RecallClient;
  private runtime: ICotAgentRuntime;
  private syncInterval: NodeJS.Timeout | undefined;
  private alias: string;
  private prefix: string;

  getInstance(): RecallService {
    return RecallService.getInstance();
  }

  async initialize(_runtime: ICotAgentRuntime): Promise<void> {
    try {
      if (!process.env.RECALL_PRIVATE_KEY) {
        throw new Error('RECALL_PRIVATE_KEY is required');
      }
      if (!process.env.RECALL_BUCKET_ALIAS) {
        throw new Error('RECALL_BUCKET_ALIAS is required');
      }
      if (!process.env.COT_LOG_PREFIX) {
        throw new Error('COT_LOG_PREFIX is required');
      }
      const wallet = walletClientFromPrivateKey(privateKey, testnet);
      this.client = new RecallClient({ walletClient: wallet });
      this.alias = envAlias;
      this.prefix = envPrefix;
      this.runtime = _runtime;
      await this.startPeriodicSync();
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

  public async buyCredit(amount: string): Promise<BuyResult> | undefined {
    try {
      const info = await this.client.creditManager().buy(parseEther(amount));
      return info.result;
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
   * @param data The data to store.
   * @param options Optional options for adding the object.
   * @returns An object containing the owner's address, bucket address, and key.
   */

  public async addObject(
    bucket: Address,
    key: string,
    data: string | File | Uint8Array,
    options?: { overwrite?: boolean },
  ): Promise<AddObjectResult | undefined> {
    try {
      const info = await this.client.bucketManager().add(bucket, key, data, {
        overwrite: options?.overwrite ?? false,
      });
      return info.result;
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

      // @ts-expect-error this is temporary
      if (!addObject || !addObject.result) {
        elizaLogger.error('Recall API returned invalid response for batch storage');
        return undefined;
      }

      // @ts-expect-error this is temporary
      elizaLogger.info(`Successfully stored batch at key: ${addObject.result.key}`);
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
          failedLogIds.push(log.id);
        }
      }

      if (failedLogIds.length > 0) {
        elizaLogger.warn(
          `Sync attempt finished. ${failedLogIds.length} logs failed to upload and remain unsynced. Will retry next cycle.`,
        );
      } else {
        elizaLogger.success('All processed logs were successfully uploaded and marked as synced');
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
   */

  public startPeriodicSync(intervalMs = 2 * 60 * 1000): void {
    if (this.syncInterval) {
      elizaLogger.warn('Log sync is already running.');
      return;
    }

    elizaLogger.info('Starting periodic log sync...');
    this.syncInterval = setInterval(async () => {
      try {
        await this.syncLogsToRecall(this.alias);
      } catch (error) {
        elizaLogger.error(`Periodic log sync failed: ${error.message}`);
      }
    }, intervalMs);

    // Perform an immediate sync on startup
    this.syncLogsToRecall(this.alias).catch((error) =>
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
