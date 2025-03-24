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
import { portfolioKeywords } from '../types.ts';
import { containsKeywords, formatCurrency } from '../utils.ts';

export const getPortfolioAction: Action = {
  name: 'GET_PORTFOLIO',
  similes: [
    'GET_PORTFOLIO',
    'PORTFOLIO_STATUS',
    'PORTFOLIO_VALUE',
    'CHECK_PORTFOLIO',
    'PORTFOLIO_BREAKDOWN',
    'ASSET_ALLOCATION',
  ],
  validate: async (_runtime: IAgentRuntime, message: Memory) => {
    const text = message.content.text.toLowerCase();

    // Check if the message contains portfolio-related keywords
    if (!containsKeywords(text, portfolioKeywords)) {
      return false;
    }

    elizaLogger.info('GET_PORTFOLIO validation passed');
    return true;
  },
  description: "Retrieves user's portfolio information across all chains",
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

      elizaLogger.info('Fetching portfolio information...');
      const portfolioInfo = await tradingSimulatorService.getPortfolio();

      if (portfolioInfo?.success && portfolioInfo.tokens) {
        // Group tokens by chain
        const tokensByChain: Record<string, any[]> = {};

        portfolioInfo.tokens.forEach((token) => {
          const chain = token.specificChain || token.chain;
          if (!tokensByChain[chain]) {
            tokensByChain[chain] = [];
          }
          tokensByChain[chain].push(token);
        });

        // Access different possible total value properties from the API response
        // The actual API response includes totalValue but our interface uses portfolioValue
        let totalValue = 0;
        if ('totalValue' in portfolioInfo && typeof portfolioInfo.totalValue === 'number') {
          totalValue = portfolioInfo.totalValue;
        } else if (typeof portfolioInfo.portfolioValue === 'number') {
          totalValue = portfolioInfo.portfolioValue;
        } else {
          // Calculate from tokens as fallback
          totalValue = portfolioInfo.tokens.reduce((sum, token) => {
            // Calculate token value based on available properties
            let tokenValue = 0;
            if ('value' in token && typeof token.value === 'number') {
              tokenValue = token.value;
            } else if ('valueUsd' in token && typeof token.valueUsd === 'number') {
              tokenValue = token.valueUsd;
            } else if (
              typeof token.amount === 'number' &&
              'price' in token &&
              typeof token.price === 'number'
            ) {
              tokenValue = token.amount * token.price;
            }
            return sum + tokenValue;
          }, 0);
        }

        // Format the response
        const chainSections = Object.entries(tokensByChain)
          .map(([chain, tokens]) => {
            const tokenLines = tokens
              .map((t) => {
                // Calculate token value based on available properties
                let tokenValue = 0;
                if ('value' in t && typeof t.value === 'number') {
                  tokenValue = t.value;
                } else if ('valueUsd' in t && typeof t.valueUsd === 'number') {
                  tokenValue = t.valueUsd;
                } else if (
                  typeof t.amount === 'number' &&
                  'price' in t &&
                  typeof t.price === 'number'
                ) {
                  tokenValue = t.amount * t.price;
                }

                // Calculate percentage safely
                let percentage = '0.00';
                if (totalValue > 0 && tokenValue > 0) {
                  percentage = ((tokenValue / totalValue) * 100).toFixed(2);
                }

                return `- **${t.token}**: ${t.amount} tokens (${formatCurrency(tokenValue)} USD, ${percentage}%)`;
              })
              .join('\n');

            return `**Chain: ${chain.toUpperCase()}**\n${tokenLines}`;
          })
          .join('\n\n');

        text = `💼 **Your Portfolio (Total Value: ${formatCurrency(totalValue)} USD)**\n\n${chainSections}`;
      } else {
        elizaLogger.error('GET_PORTFOLIO failed: No portfolio info received.');
        text = '⚠️ Unable to retrieve your portfolio information. Please try again later.';
      }
    } catch (error: any) {
      elizaLogger.error(`GET_PORTFOLIO error: ${error.message}`);
      text =
        '⚠️ An error occurred while fetching your portfolio information. Please try again later.';
    }

    // Create a new memory entry for the response
    const newMemory: Memory = {
      ...message,
      userId: message.agentId,
      content: {
        text,
        action: 'GET_PORTFOLIO',
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
        content: { text: 'What is in my portfolio?' },
      },
      {
        user: '{{agentName}}',
        content: {
          text: '💼 **Your Portfolio (Total Value: 15,000.00 USD)**\n\n**Chain: ETH**\n- **0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48**: 5000 tokens (5,000.00 USD, 33.33%)\n- **0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2**: 1 tokens (5,000.00 USD, 33.33%)\n\n**Chain: SVM**\n- **EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v**: 5000 tokens (5,000.00 USD, 33.33%)',
          action: 'GET_PORTFOLIO',
        },
      },
    ],
    [
      {
        user: '{{user1}}',
        content: { text: 'Show me my portfolio breakdown' },
      },
      {
        user: '{{agentName}}',
        content: {
          text: '💼 **Your Portfolio (Total Value: 15,000.00 USD)**\n\n**Chain: ETH**\n- **0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48**: 5000 tokens (5,000.00 USD, 33.33%)\n- **0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2**: 1 tokens (5,000.00 USD, 33.33%)\n\n**Chain: SVM**\n- **EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v**: 5000 tokens (5,000.00 USD, 33.33%)',
          action: 'GET_PORTFOLIO',
        },
      },
    ],
    [
      {
        user: '{{user1}}',
        content: { text: "What's my asset allocation?" },
      },
      {
        user: '{{agentName}}',
        content: {
          text: '💼 **Your Portfolio (Total Value: 15,000.00 USD)**\n\n**Chain: ETH**\n- **0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48**: 5000 tokens (5,000.00 USD, 33.33%)\n- **0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2**: 1 tokens (5,000.00 USD, 33.33%)\n\n**Chain: SVM**\n- **EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v**: 5000 tokens (5,000.00 USD, 33.33%)',
          action: 'GET_PORTFOLIO',
        },
      },
    ],
  ] as ActionExample[][],
};
