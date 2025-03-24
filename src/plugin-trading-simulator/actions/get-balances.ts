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
import { TradingSimulatorService } from '../services/trading-simulator.service.ts';
import { balanceKeywords } from '../types.ts';
import { containsKeywords } from '../utils.ts';

export const getBalancesAction: Action = {
  name: 'GET_BALANCES',
  similes: [
    'GET_BALANCES',
    'CHECK_BALANCES',
    'TOKEN_BALANCES',
    'VIEW_BALANCES',
    'SHOW_BALANCES',
    'LIST_TOKENS',
  ],
  validate: async (_runtime: IAgentRuntime, message: Memory) => {
    const text = message.content.text.toLowerCase();

    // Check if the message contains balance-related keywords
    if (!containsKeywords(text, balanceKeywords)) {
      return false;
    }

    elizaLogger.info('GET_BALANCES validation passed');
    return true;
  },
  description: "Retrieves user's token balances across all supported chains",
  handler: async (
    runtime: IAgentRuntime,
    message: Memory,
    state?: State,
    options?: { [key: string]: unknown },
    callback?: HandlerCallback,
  ): Promise<boolean> => {
    const tradingSimulatorService = runtime.services.get(
      'tradingsimulator' as ServiceType,
    ) as TradingSimulatorService;
    let text = '';

    try {
      let currentState = state;
      if (!currentState) {
        currentState = (await runtime.composeState(message)) as State;
      } else {
        currentState = await runtime.updateRecentMessageState(currentState);
      }

      elizaLogger.info('Fetching token balances...');
      const balanceInfo = await tradingSimulatorService.getBalances();

      if (balanceInfo?.success && balanceInfo.balances) {
        // Group balances by chain
        const balancesByChain: Record<string, any[]> = {};

        balanceInfo.balances.forEach((balance) => {
          const chain = balance.specificChain || balance.chain;
          if (!balancesByChain[chain]) {
            balancesByChain[chain] = [];
          }
          balancesByChain[chain].push(balance);
        });

        // Format the response
        const chainSections = Object.entries(balancesByChain)
          .map(([chain, balances]) => {
            const balanceLines = balances.map((b) => `- **${b.token}**: ${b.amount}`).join('\n');
            return `**Chain: ${chain.toUpperCase()}**\n${balanceLines}`;
          })
          .join('\n\n');

        text = `📊 **Your Token Balances**\n\n${chainSections}`;
      } else {
        elizaLogger.error('GET_BALANCES failed: No balance info received.');
        text = '⚠️ Unable to retrieve your token balances. Please try again later.';
      }
    } catch (error: any) {
      elizaLogger.error(`GET_BALANCES error: ${error.message}`);
      text = '⚠️ An error occurred while fetching your token balances. Please try again later.';
    }

    // Create a new memory entry for the response
    const newMemory: Memory = {
      ...message,
      userId: message.agentId,
      content: {
        text,
        action: 'GET_BALANCES',
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
        content: { text: 'What are my token balances?' },
      },
      {
        user: '{{agentName}}',
        content: {
          text: '📊 **Your Token Balances**\n\n**Chain: ETH**\n- **0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48**: 5000\n- **0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2**: 1\n\n**Chain: SVM**\n- **EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v**: 5000\n- **So11111111111111111111111111111111111111112**: 10',
          action: 'GET_BALANCES',
        },
      },
    ],
    [
      {
        user: '{{user1}}',
        content: { text: 'Show me my balances' },
      },
      {
        user: '{{agentName}}',
        content: {
          text: '📊 **Your Token Balances**\n\n**Chain: ETH**\n- **0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48**: 5000\n- **0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2**: 1\n\n**Chain: SVM**\n- **EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v**: 5000\n- **So11111111111111111111111111111111111111112**: 10',
          action: 'GET_BALANCES',
        },
      },
    ],
    [
      {
        user: '{{user1}}',
        content: { text: 'Check my tokens' },
      },
      {
        user: '{{agentName}}',
        content: {
          text: '📊 **Your Token Balances**\n\n**Chain: ETH**\n- **0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48**: 5000\n- **0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2**: 1\n\n**Chain: SVM**\n- **EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v**: 5000\n- **So11111111111111111111111111111111111111112**: 10',
          action: 'GET_BALANCES',
        },
      },
    ],
  ] as ActionExample[][],
};
