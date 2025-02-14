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

const balanceKeywords = [
  'balance',
  'credit balance',
  'account balance',
  'how many credits',
  'check my credits',
  'my available credits',
  'how much credit',
  'how much do I have',
  'do I have credits',
];

export const getCreditBalanceAction: Action = {
  name: 'GET_CREDIT_BALANCE',
  similes: [
    'GET_CREDIT_BALANCE',
    'CHECK_CREDIT',
    'ACCOUNT_CREDIT_BALANCE',
    'CREDIT_BALANCE',
    'BALANCE_CHECK',
    'AVAILABLE_CREDITS',
  ],
  validate: async (_runtime: IAgentRuntime, message: Memory) => {
    const text = message.content.text.toLowerCase();

    // Ensure the user is asking about balance, not trying to buy credits
    if (!balanceKeywords.some((keyword) => text.includes(keyword))) {
      return false;
    }

    if (text.includes('solana') || text.includes('sol') || text.includes('solana wallet')) {
      return false;
    }

    // Ensure there are NO numeric values (to prevent confusion with BUY_CREDIT)
    const amountMatch = text.match(/([\d.]+)/);
    if (amountMatch) {
      elizaLogger.error(
        'GET_CREDIT_BALANCE validation failed: Message contains numeric values (possible buy request).',
      );
      return false;
    }

    elizaLogger.info('GET_CREDIT_BALANCE Validation Passed!');
    return true;
  },
  description: "Checks the user's Recall credit balance",
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
      elizaLogger.info('Fetching credit balance...');
      const balanceInfo = await recallService.getCreditInfo();

      if (balanceInfo?.creditFree !== undefined) {
        const balance = balanceInfo.creditFree.toString(); // Ensure it's a string
        elizaLogger.info(`Credit Balance Retrieved: ${balance}`);

        text = `💰 Your current Recall credit balance is **${balance} credits**.`;
      } else {
        elizaLogger.error('GET_CREDIT_BALANCE failed: No balance info received.');
        text = '⚠️ Unable to retrieve your credit balance. Please try again later.';
      }
    } catch (error) {
      elizaLogger.error(`GET_CREDIT_BALANCE error: ${error.message}`);
      text = '⚠️ An error occurred while fetching your credit balance. Please try again later.';
    }

    // Create a new memory entry for the response
    const newMemory: Memory = {
      ...message,
      userId: message.agentId,
      content: {
        text,
        action: 'GET_CREDIT_BALANCE',
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
        content: { text: 'What is my account credit balance?' },
      },
      {
        user: '{{agentName}}',
        content: {
          text: '💰 Your current Recall credit balance is **X credits**.',
          action: 'GET_CREDIT_BALANCE',
        },
      },
    ],
    [
      {
        user: '{{user1}}',
        content: { text: 'How many credits are in my account?' },
      },
      {
        user: '{{agentName}}',
        content: {
          text: '💰 Your current Recall credit balance is **X credits**.',
          action: 'GET_CREDIT_BALANCE',
        },
      },
    ],
    [
      {
        user: '{{user1}}',
        content: { text: 'Can you check my available credits?' },
      },
      {
        user: '{{agentName}}',
        content: {
          text: '💰 Your current Recall credit balance is **X credits**.',
          action: 'GET_CREDIT_BALANCE',
        },
      },
    ],
  ] as ActionExample[][],
};
