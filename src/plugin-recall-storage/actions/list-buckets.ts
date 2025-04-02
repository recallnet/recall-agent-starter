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

const bucketKeywords = [
  'list buckets',
  'get my buckets',
  'retrieve my buckets',
  'show my buckets',
  'fetch my buckets',
  'available buckets',
];

export const listBucketsAction: Action = {
  name: 'LIST_BUCKETS',
  similes: ['LIST_BUCKETS', 'GET_BUCKETS', 'SHOW_BUCKETS', 'FETCH_BUCKETS', 'AVAILABLE_BUCKETS'],
  validate: async (_runtime: IAgentRuntime, message: Memory) => {
    const text = message.content.text.toLowerCase();

    // Ensure the user is requesting a list of buckets
    if (!bucketKeywords.some((keyword) => text.includes(keyword))) {
      return false;
    }

    elizaLogger.info('LIST_BUCKETS Validation Passed!');
    return true;
  },
  description: 'Retrieves and lists all available Recall buckets.',
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

      elizaLogger.info('Fetching bucket list...');
      const bucketList = await recallService.listBuckets();

      if (bucketList && bucketList.length > 0) {
        const bucketDetails = bucketList
          .map((bucket) => `🔹 **${bucket.kind}** (Address: ${bucket.addr})`)
          .join('\n');

        text = `📂 **Your Recall Buckets:**\n\n${bucketDetails}`;
        elizaLogger.info(`LIST_BUCKETS success: Retrieved ${bucketList.length} buckets.`);
      } else {
        text = '📂 You currently have no Recall buckets.';
        elizaLogger.info('LIST_BUCKETS success: No buckets found.');
      }
    } catch (error) {
      text = '⚠️ An error occurred while retrieving your buckets. Please try again later.';
      elizaLogger.error(`LIST_BUCKETS error: ${error.message}`);
    }

    // Create a new memory entry for the response
    const newMemory: Memory = {
      ...message,
      userId: message.agentId,
      content: {
        text,
        action: 'LIST_BUCKETS',
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
        content: { text: 'Get a list of my buckets' },
      },
      {
        user: '{{agentName}}',
        content: {
          text: '📂 **Your Recall Buckets:**\n🔹 **Bucket Type 1** (Address: 0x123...456)\n🔹 **Bucket Type 2** (Address: 0x789...ABC)',
          action: 'LIST_BUCKETS',
        },
      },
    ],
    [
      {
        user: '{{user1}}',
        content: { text: 'Show me my buckets' },
      },
      {
        user: '{{agentName}}',
        content: {
          text: '📂 **Your Recall Buckets:**\n🔹 **Data Storage** (Address: 0xDEF...789)\n🔹 **AI Memory** (Address: 0x123...ABC)',
          action: 'LIST_BUCKETS',
        },
      },
    ],
    [
      {
        user: '{{user1}}',
        content: { text: 'Do I have any buckets?' },
      },
      {
        user: '{{agentName}}',
        content: {
          text: '📂 You currently have no Recall buckets.',
          action: 'LIST_BUCKETS',
        },
      },
    ],
  ] as ActionExample[][],
};
