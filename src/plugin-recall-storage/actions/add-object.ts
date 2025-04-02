import {
  type Action,
  type IAgentRuntime,
  type Memory,
  type State,
  type HandlerCallback,
  type ActionExample,
  elizaLogger,
  ServiceType,
} from '@elizaos/core';
import { RecallService } from '../services/recall.service';
import * as fs from 'fs';
import * as path from 'path';

const addObjectKeywords = ['add object', 'store object', 'upload object', 'save object'];

export const addObjectAction: Action = {
  name: 'ADD_OBJECT',
  similes: ['ADD_OBJECT', 'STORE_OBJECT', 'UPLOAD_OBJECT', 'SAVE_OBJECT', 'ADD_TO_BUCKET'],

  validate: async (_runtime: IAgentRuntime, message: Memory) => {
    const text = message.content.text.toLowerCase();

    // Ensure message contains an add object request
    if (!addObjectKeywords.some((keyword) => text.includes(keyword))) {
      return false;
    }

    // Extract first and second quoted strings
    const matches = message.content.text.match(/"([^"]+)"\s+.*?"([^"]+)"/);
    if (!matches || matches.length < 3) {
      elizaLogger.error(
        'ADD_OBJECT validation failed: No valid object path and bucket alias detected.',
      );
      return false;
    }

    elizaLogger.info(
      `ADD_OBJECT Validation Passed! Object: ${matches[1]}, Bucket Alias: ${matches[2]}`,
    );
    return true;
  },

  description: 'Adds an object to a specified Recall bucket.',

  handler: async (
    runtime: IAgentRuntime,
    message: Memory,
    state: State,
    _options: { [key: string]: unknown },
    callback?: HandlerCallback,
  ): Promise<boolean> => {
    const recallService = runtime.services.get('recall' as ServiceType) as RecallService;
    let text = '';

    try {
      let currentState = state;
      if (!currentState) {
        currentState = (await runtime.composeState(message)) as State;
      } else {
        currentState = await runtime.updateRecentMessageState(currentState);
      }

      elizaLogger.info(`ADD_OBJECT Handler triggered: ${message.content.text}`);

      // Extract object file and bucket alias
      const matches = message.content.text.match(/"([^"]+)"\s+.*?"([^"]+)"/);
      if (!matches || matches.length < 3) {
        text =
          '❌ Invalid request. Please specify both the object file and the bucket alias in double quotes.';
        elizaLogger.error('ADD_OBJECT failed: Missing object or bucket alias.');
      } else {
        const objectPath = matches[1].trim();
        const bucketAlias = matches[2].trim();

        elizaLogger.info(`Looking up bucket for alias: ${bucketAlias}`);

        // Retrieve or create the bucket
        const bucketAddress = await recallService.getOrCreateBucket(bucketAlias);
        if (!bucketAddress) {
          text = `❌ Failed to find or create bucket with alias "${bucketAlias}".`;
          elizaLogger.error(`ADD_OBJECT failed: No bucket found for alias "${bucketAlias}".`);
        } else {
          elizaLogger.info(`Found bucket ${bucketAddress} for alias "${bucketAlias}".`);

          // Resolve absolute path and check if file exists
          const filePath = path.resolve(process.cwd(), objectPath);
          if (!fs.existsSync(filePath)) {
            text = `❌ Object file not found: ${objectPath}`;
            elizaLogger.error(`ADD_OBJECT failed: File "${filePath}" does not exist.`);
          } else {
            // Read file data
            const fileData = fs.readFileSync(filePath);
            const fileName = path.basename(filePath);

            elizaLogger.info(`Uploading object "${fileName}" to bucket "${bucketAlias}"...`);

            // Call RecallService to add object
            const result = await recallService.addObject(
              bucketAddress,
              fileName,
              fileData as unknown as File,
            );

            if (result?.meta?.tx) {
              // Check for transaction receipt instead of just result
              text = `✅ Successfully added object **"${fileName}"** to bucket **"${bucketAlias}"**. Transaction hash: ${result.meta.tx.transactionHash}`;
              elizaLogger.info(
                `ADD_OBJECT success: "${fileName}" added to bucket "${bucketAlias}". TX: ${result.meta.tx.transactionHash}`,
              );
            } else {
              text = '❌ Failed to add object to the bucket. Please try again later.';
              elizaLogger.error('ADD_OBJECT failed: Transaction unsuccessful');
            }
          }
        }
      }
    } catch (error) {
      text = '⚠️ An error occurred while adding the object. Please try again later.';
      elizaLogger.error(`ADD_OBJECT error: ${error.message}`);
      if (error.cause) {
        elizaLogger.error(`ADD_OBJECT error cause: ${error.cause.message}`);
      }
    }

    // Create a new memory entry for the response
    const newMemory: Memory = {
      ...message,
      userId: message.agentId,
      content: {
        text,
        action: 'ADD_OBJECT',
        source: message.content.source,
      },
    };

    // Save to memory
    await runtime.messageManager.createMemory(newMemory);

    // Call callback AFTER saving memory
    await callback?.({ text });

    return true;
  },

  examples: [
    [
      {
        user: '{{user1}}',
        content: { text: 'Add object "./object.txt" to "my-bucket"' },
      },
      {
        user: '{{agentName}}',
        content: {
          text: '✅ Successfully added object **"object.txt"** to bucket **"my-bucket"**. Transaction hash: 0x...',
          action: 'ADD_OBJECT',
        },
      },
    ],
    [
      {
        user: '{{user1}}',
        content: { text: 'Store object "./data.json" in "backup"' },
      },
      {
        user: '{{agentName}}',
        content: {
          text: '✅ Successfully added object **"data.json"** to bucket **"backup"**. Transaction hash: 0x...',
          action: 'ADD_OBJECT',
        },
      },
    ],
    [
      {
        user: '{{user1}}',
        content: { text: 'Upload "./logs.txt" into "logs"' },
      },
      {
        user: '{{agentName}}',
        content: {
          text: '✅ Successfully added object **"logs.txt"** to bucket **"logs"**. Transaction hash: 0x...',
          action: 'ADD_OBJECT',
        },
      },
    ],
  ] as ActionExample[][],
};
