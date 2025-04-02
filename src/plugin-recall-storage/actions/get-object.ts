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

const getObjectKeywords = ['get object', 'retrieve object', 'fetch object', 'download object'];

export const getObjectAction: Action = {
  name: 'GET_OBJECT',
  similes: ['GET_OBJECT', 'RETRIEVE_OBJECT', 'FETCH_OBJECT', 'DOWNLOAD_OBJECT', 'GET_FROM_BUCKET'],
  validate: async (_runtime: IAgentRuntime, message: Memory) => {
    const text = message.content.text.toLowerCase();

    // Ensure message contains a valid "get object" request
    if (!getObjectKeywords.some((keyword) => text.includes(keyword))) {
      return false;
    }

    // Extract object key and bucket alias (both wrapped in double quotes)
    const matches = message.content.text.match(/"([^"]+)"\s+from bucket\s+"([^"]+)"/);
    if (!matches || matches.length < 3) {
      elizaLogger.error(
        'GET_OBJECT validation failed: No valid object key and bucket alias detected.',
      );
      return false;
    }

    elizaLogger.info(
      `GET_OBJECT Validation Passed! Object: ${matches[1]}, Bucket Alias: ${matches[2]}`,
    );
    return true;
  },
  description: 'Retrieves an object from a specified Recall bucket.',
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

      elizaLogger.info(`GET_OBJECT Handler triggered: ${message.content.text}`);

      // Extract object key and bucket alias
      const matches = message.content.text.match(/"([^"]+)"\s+from bucket\s+"([^"]+)"/);
      if (!matches || matches.length < 3) {
        text =
          '❌ Invalid request. Please specify both the object key and the bucket alias in double quotes.';
        elizaLogger.error('GET_OBJECT failed: Missing object key or bucket alias.');
      } else {
        const objectKey = matches[1].trim();
        const bucketAlias = matches[2].trim();

        elizaLogger.info(`Looking up bucket for alias: ${bucketAlias}`);

        // Retrieve the bucket address
        const bucketAddress = await recallService.getOrCreateBucket(bucketAlias);
        if (!bucketAddress) {
          text = `❌ Failed to find or create bucket with alias "${bucketAlias}".`;
          elizaLogger.error(`GET_OBJECT failed: No bucket found for alias "${bucketAlias}".`);
        } else {
          elizaLogger.info(`Found bucket ${bucketAddress} for alias "${bucketAlias}".`);

          // Ensure the downloads directory exists
          const downloadsDir = path.resolve(process.cwd(), 'downloads');
          if (!fs.existsSync(downloadsDir)) {
            fs.mkdirSync(downloadsDir, { recursive: true });
          }

          // Retrieve the object from Recall
          const objectData = await recallService.getObject(bucketAddress, objectKey);

          if (objectData) {
            // Define the file path within the downloads directory
            const filePath = path.join(downloadsDir, objectKey);

            // Write the file to the downloads directory
            fs.writeFileSync(filePath, Buffer.from(objectData));

            text = `✅ Successfully retrieved object **"${objectKey}"** from bucket **"${bucketAlias}"**.\n📂 File saved at: \`${filePath}\``;
            elizaLogger.info(
              `GET_OBJECT success: "${objectKey}" retrieved and saved to "${filePath}".`,
            );
          } else {
            text = `❌ Object **"${objectKey}"** not found in bucket **"${bucketAlias}"**.`;
            elizaLogger.error(`GET_OBJECT failed: Object "${objectKey}" not found.`);
          }
        }
      }
    } catch (error) {
      text = '⚠️ An error occurred while retrieving the object. Please try again later.';
      elizaLogger.error(`GET_OBJECT error: ${error.message}`);
    }

    // Create a new memory entry for the response
    const newMemory: Memory = {
      ...message,
      userId: message.agentId,
      content: {
        text,
        action: 'GET_OBJECT',
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
        content: { text: 'Get object "object.txt" from bucket "my-bucket"' },
      },
      {
        user: '{{agentName}}',
        content: {
          text: '✅ Successfully retrieved object **"object.txt"** from bucket **"my-bucket"**.\n📂 File saved at: `/path/to/object.txt`',
          action: 'GET_OBJECT',
        },
      },
    ],
    [
      {
        user: '{{user1}}',
        content: { text: 'Retrieve object "data.json" from bucket "backup"' },
      },
      {
        user: '{{agentName}}',
        content: {
          text: '✅ Successfully retrieved object **"data.json"** from bucket **"backup"**.\n📂 File saved at: `/path/to/data.json`',
          action: 'GET_OBJECT',
        },
      },
    ],
    [
      {
        user: '{{user1}}',
        content: { text: 'Fetch object "logs.txt" from bucket "logs"' },
      },
      {
        user: '{{agentName}}',
        content: {
          text: '✅ Successfully retrieved object **"logs.txt"** from bucket **"logs"**.\n📂 File saved at: `/path/to/logs.txt`',
          action: 'GET_OBJECT',
        },
      },
    ],
  ] as ActionExample[][],
};
