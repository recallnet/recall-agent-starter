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

const createBucketKeywords = [
  'create a bucket',
  'make a bucket',
  'new bucket',
  'generate a bucket',
  'add a bucket',
];

export const createBucketAction: Action = {
  name: 'CREATE_BUCKET',
  similes: ['CREATE_BUCKET', 'MAKE_BUCKET', 'NEW_BUCKET', 'GENERATE_BUCKET', 'ADD_BUCKET'],
  validate: async (_runtime: IAgentRuntime, message: Memory) => {
    const text = message.content.text.toLowerCase();

    // Ensure the message contains a create bucket request
    if (!createBucketKeywords.some((keyword) => text.includes(keyword))) {
      return false;
    }

    // Extract alias from quoted text (single or double quotes)
    const aliasMatch = message.content.text.match(/["']([^"']+)["']/);
    if (!aliasMatch || !aliasMatch[1]) {
      elizaLogger.error('CREATE_BUCKET validation failed: No alias detected in quotes.');
      return false;
    }

    elizaLogger.info(`CREATE_BUCKET Validation Passed! Alias: ${aliasMatch[1]}`);
    return true;
  },
  description: 'Creates a new Recall bucket with a given alias.',
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

      elizaLogger.info(`CREATE_BUCKET Handler triggered: ${message.content.text}`);

      // Extract alias from quoted text
      const aliasMatch = message.content.text.match(/["']([^"']+)["']/);
      if (!aliasMatch || !aliasMatch[1]) {
        text = '❌ Invalid bucket request. Please specify an alias in quotes.';
        elizaLogger.error('CREATE_BUCKET failed: No alias found.');
      } else {
        const bucketAlias = aliasMatch[1].trim();
        elizaLogger.info(`Creating bucket with alias: ${bucketAlias}`);

        // Call RecallService to create or fetch bucket
        const bucketAddress = await recallService.getOrCreateBucket(bucketAlias);

        if (bucketAddress) {
          text = `✅ Successfully created or retrieved bucket **"${bucketAlias}"** at address: **${bucketAddress}**`;
          elizaLogger.info(
            `CREATE_BUCKET success: Bucket "${bucketAlias}" created at ${bucketAddress}`,
          );
        } else {
          text = '❌ Bucket creation failed. Please try again later.';
          elizaLogger.error('CREATE_BUCKET failed: No response from RecallService.');
        }
      }
    } catch (error) {
      text = '⚠️ An error occurred while creating your bucket. Please try again later.';
      elizaLogger.error(`CREATE_BUCKET error: ${error.message}`);
    }

    // Create a new memory entry for the response
    const newMemory: Memory = {
      ...message,
      userId: message.agentId,
      content: {
        text,
        action: 'CREATE_BUCKET',
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
        content: { text: 'Create a bucket for me named "new-bucket"' },
      },
      {
        user: '{{agentName}}',
        content: {
          text: '✅ Successfully created or retrieved bucket **"new-bucket"** at address: **0x123...456**',
          action: 'CREATE_BUCKET',
        },
      },
    ],
    [
      {
        user: '{{user1}}',
        content: { text: "Make a bucket for me with the alias 'backup-data'" },
      },
      {
        user: '{{agentName}}',
        content: {
          text: '✅ Successfully created or retrieved bucket **"backup-data"** at address: **0xDEF...789**',
          action: 'CREATE_BUCKET',
        },
      },
    ],
    [
      {
        user: '{{user1}}',
        content: { text: "Generate a new bucket called 'logs'" },
      },
      {
        user: '{{agentName}}',
        content: {
          text: '✅ Successfully created or retrieved bucket **"logs"** at address: **0xAAA...BBB**',
          action: 'CREATE_BUCKET',
        },
      },
    ],
  ] as ActionExample[][],
};
