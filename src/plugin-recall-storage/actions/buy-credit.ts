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

const keywords = ['buy', 'credit', 'credits', 'purchase', 'add credit', 'add credits'];

export const buyCreditAction: Action = {
  name: 'BUY_CREDIT',
  similes: [
    'BUY_CREDIT',
    'buy credit',
    'buy credits',
    'purchase credit',
    'purchase credits',
    'add credit',
    'add credits',
    'ADD_CREDIT',
    'RELOAD_CREDIT',
    'PURCHASE_CREDIT',
    'BUY RECALL CREDITS',
    'GET MORE CREDITS',
    'RECHARGE ACCOUNT',
  ],
  validate: async (_runtime: IAgentRuntime, message: Memory) => {
    const text = message.content.text.toLowerCase();
    const amountMatch = text.match(/([\d.]+)/);

    if (!amountMatch) {
      return false;
    }

    const amount = parseFloat(amountMatch[1]);
    if (isNaN(amount) || amount <= 0) {
      return false;
    }

    // Now check for any matching keywords
    if (!keywords.some((keyword) => text.includes(keyword))) {
      elizaLogger.error('BUY_CREDIT failed: No valid keyword found in message.');
      return false;
    }

    elizaLogger.info(`BUY_CREDIT Validation Passed! Amount: ${amount}`);
    return true;
  },
  description: "Buys Recall credits for the agent's wallet",
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
      elizaLogger.info(`BUY_CREDIT Handler: ${message.content.text}`);
      const amountMatch = message.content.text.trim().match(/([\d.]+)/);

      if (!amountMatch) {
        text = '❌ Invalid credit request. Please specify an amount.';
        elizaLogger.error('BUY_CREDIT failed: No amount provided.');
      } else {
        const amount = parseFloat(amountMatch[1]);
        if (isNaN(amount) || amount <= 0) {
          text = '❌ Invalid credit amount. Please enter a number greater than 0.';
          elizaLogger.error('BUY_CREDIT failed: Invalid amount.');
        } else {
          elizaLogger.info(`Attempting to purchase ${amount} credits...`);

          // Call RecallService to buy credit
          const result = await recallService.buyCredit(amount.toString());

          if (result?.meta?.tx) {
            // Check for transaction receipt instead of direct result
            text = `✅ Successfully purchased ${amount} Recall credits! Transaction hash: ${result.meta.tx.transactionHash}`;
            elizaLogger.info(
              `BUY_CREDIT success: ${amount} credits added. TX: ${result.meta.tx.transactionHash}`,
            );
          } else {
            text = '❌ Credit purchase failed. Please try again later.';
            elizaLogger.error('BUY_CREDIT failed: Transaction unsuccessful');
          }
        }
      }
    } catch (error) {
      text = '⚠️ An error occurred while purchasing credits. Please try again later.';
      elizaLogger.error(`BUY_CREDIT error: ${error.message}`);
    }

    // Create a new memory entry for the response
    const newMemory: Memory = {
      ...message,
      userId: message.agentId,
      content: {
        text,
        action: 'BUY_CREDIT',
        source: message.content.source,
      },
    };

    // Save to memory
    await runtime.messageManager.createMemory(newMemory);

    // Call callback AFTER saving memory
    await callback?.({
      text,
    });

    return true;
  },
  examples: [
    [
      {
        user: '{{user1}}',
        content: { text: 'Please buy 0.1 credits for my account' },
      },
      {
        user: '{{agentName}}',
        content: {
          text: '✅ Successfully purchased 0.1 Recall credits! Transaction hash: 0x...',
          action: 'BUY_CREDIT',
        },
      },
    ],
    [
      {
        user: '{{user1}}',
        content: { text: 'Please buy 1.5 credits for my account' },
      },
      {
        user: '{{agentName}}',
        content: {
          text: '✅ Successfully bought 1.5 Recall credits! Transaction hash: 0x...',
          action: 'BUY_CREDIT',
        },
      },
    ],
    [
      {
        user: '{{user1}}',
        content: { text: 'Please add 3 credits to my account' },
      },
      {
        user: '{{agentName}}',
        content: {
          text: '✅ Successfully added 3 Recall credits to your account! Transaction hash: 0x...',
          action: 'BUY_CREDIT',
        },
      },
    ],
  ] as ActionExample[][],
};
