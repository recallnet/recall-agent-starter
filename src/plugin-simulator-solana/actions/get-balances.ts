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
import { TradingService } from '../services/trading.service.ts';
import { ICotAgentRuntime } from '../../types/index.ts';

const balanceKeywords = [
  'check solana balance',
  'show solana balance',
  'get solana balance',
  'view solana balance',
];

export const getBalancesAction: Action = {
  name: 'GET_SOLANA_BALANCES',
  similes: [
    'CHECK_SOLANA_BALANCES',
    'SHOW_SOLANA_BALANCES',
    'VIEW_SOLANA_BALANCES',
    'LIST_SOLANA_BALANCES',
  ],
  description: 'Retrieves and displays all token balances in the trading account.',
  validate: async (_runtime: ICotAgentRuntime, message: Memory) => {
    const text = message.content.text.toLowerCase();
    // Check if message contains a balance check request
    const val = balanceKeywords.some((keyword) => text.includes(keyword));
    return balanceKeywords.some((keyword) => text.includes(keyword));
  },

  handler: async (
    runtime: ICotAgentRuntime,
    message: Memory,
    state: State,
    _options: { [key: string]: unknown },
    callback?: HandlerCallback,
  ): Promise<boolean> => {
    let text = '';
    elizaLogger.info(`GET_SOLANA_BALANCES Handler triggered: ${message.content.text}`);
    try {
      let currentState = state;
      if (!currentState) {
        currentState = (await runtime.composeState(message)) as State;
      } else {
        currentState = await runtime.updateRecentMessageState(currentState);
      }

      elizaLogger.info(`GET_SOLANA_BALANCES Handler triggered: ${message.content.text}`);

      // Initialize trading module
      const tradingService = runtime.services.get('trading' as ServiceType) as TradingService;

      // Get all balances
      const balances = tradingService.getAllBalances();
      elizaLogger.info(`GET_SOLANA_BALANCES: ${JSON.stringify(balances)}`);
      if (balances.length === 0) {
        text = '❌ No token balances found in the account.';
        elizaLogger.warn('GET_SOLANA_BALANCES: No balances found');
      } else {
        // Format balances into a readable message
        const balanceLines = balances.map((balance) => `**${balance.token}**: ${balance.amount}`);

        text = '💰 Current Account Balances:\n' + balanceLines.join('\n');
        elizaLogger.info('GET_SOLANA_BALANCES: Successfully retrieved balances');
      }
    } catch (error) {
      text = '⚠️ An error occurred while retrieving balances. Please try again later.';
      elizaLogger.error(`GET_SOLANA_BALANCES error: ${error.message}`);
    }

    // Create a new memory entry for the response
    const newMemory: Memory = {
      ...message,
      userId: message.agentId,
      content: {
        text,
        action: 'GET_SOLANA_BALANCES',
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
        content: { text: 'Check solana balance' },
      },
      {
        user: '{{agentName}}',
        content: {
          text: '💰 Current Account Balances:\n**SOL**: 10\n**USDC**: 1000\n**USDT**: 1000',
          action: 'GET_SOLANA_BALANCES',
        },
      },
    ],
    [
      {
        user: '{{user1}}',
        content: { text: 'Show solana balance in my wallet' },
      },
      {
        user: '{{agentName}}',
        content: {
          text: '💰 Current Account Balances:\n**SOL**: 10\n**USDC**: 1000\n**USDT**: 1000',
          action: 'GET_SOLANA_BALANCES',
        },
      },
    ],
    [
      {
        user: '{{user1}}',
        content: { text: 'View solana balance please' },
      },
      {
        user: '{{agentName}}',
        content: {
          text: '💰 Current Account Balances:\n**SOL**: 10\n**USDC**: 1000\n**USDT**: 1000',
          action: 'GET_SOLANA_BALANCES',
        },
      },
    ],
  ] as ActionExample[][],
};
