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
import { priceKeywords } from '../types.ts';
import { containsKeywords, extractTokenAddresses, formatCurrency } from '../utils.ts';

export const getPriceAction: Action = {
  name: 'GET_PRICE',
  similes: [
    'GET_PRICE',
    'TOKEN_PRICE',
    'CHECK_PRICE',
    'CURRENT_PRICE',
    'PRICE_CHECK',
    'HOW_MUCH_IS',
  ],
  validate: async (_runtime: IAgentRuntime, message: Memory) => {
    const text = message.content.text.toLowerCase();

    // Check if the message contains price-related keywords
    if (!containsKeywords(text, priceKeywords)) {
      return false;
    }

    // Check if there's a token address in the message
    const tokenAddresses = extractTokenAddresses(text);
    if (tokenAddresses.length === 0) {
      elizaLogger.info('GET_PRICE validation failed: No token address found in message.');
      return false;
    }

    elizaLogger.info('GET_PRICE validation passed');
    return true;
  },
  description: 'Retrieves the current price of a specified token',
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

      // Extract token address from the message
      const tokenAddresses = extractTokenAddresses(message.content.text);
      if (tokenAddresses.length === 0) {
        text = "⚠️ I couldn't find a valid token address in your message.";
      } else {
        const token = tokenAddresses[0];

        // Try to get detailed token info if possible
        try {
          elizaLogger.info(`Fetching token info for ${token}...`);
          const tokenInfo = await tradingSimulatorService.getTokenInfo(token);

          if (tokenInfo?.success) {
            const chain = tokenInfo.specificChain || tokenInfo.chain;
            const chainDisplay = chain.toUpperCase();
            const symbol = tokenInfo.symbol || 'Unknown';
            const name = tokenInfo.name || 'Unknown Token';

            text =
              `💰 **${name} (${symbol})**\n\n` +
              `- **Price**: ${formatCurrency(tokenInfo.price)} USD\n` +
              `- **Chain**: ${chainDisplay}\n` +
              `- **Address**: ${token}\n`;
          } else {
            // Fallback to basic price lookup
            throw new Error('Token info not available');
          }
        } catch (error) {
          // If token info fails, try just getting the price
          elizaLogger.info(`Falling back to basic price lookup for ${token}...`);
          const priceInfo = await tradingSimulatorService.getPrice(token);

          if (priceInfo?.success) {
            const chain = priceInfo.specificChain || priceInfo.chain;
            const chainDisplay = chain.toUpperCase();

            text =
              `💰 **Token Price**\n\n` +
              `- **Price**: ${formatCurrency(priceInfo.price)} USD\n` +
              `- **Chain**: ${chainDisplay}\n` +
              `- **Address**: ${token}\n` +
              `- **Last Updated**: ${new Date(priceInfo.timestamp).toLocaleString()}`;
          } else {
            elizaLogger.error(`GET_PRICE failed: No price info received for token ${token}.`);
            text = `⚠️ Unable to retrieve price information for token ${token}. Please ensure the token address is valid.`;
          }
        }
      }
    } catch (error: any) {
      elizaLogger.error(`GET_PRICE error: ${error.message}`);
      text =
        '⚠️ An error occurred while fetching the token price. Please ensure the token address is valid and try again.';
    }

    // Create a new memory entry for the response
    const newMemory: Memory = {
      ...message,
      userId: message.agentId,
      content: {
        text,
        action: 'GET_PRICE',
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
        content: { text: 'What is the price of 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2?' },
      },
      {
        user: '{{agentName}}',
        content: {
          text: '💰 **Wrapped Ether (WETH)**\n\n- **Price**: 5,000.00 USD\n- **Chain**: ETH\n- **Address**: 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2\n- **Last Updated**: 3/21/2023, 2:30:45 PM',
          action: 'GET_PRICE',
        },
      },
    ],
    [
      {
        user: '{{user1}}',
        content: { text: 'Check price of EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v' },
      },
      {
        user: '{{agentName}}',
        content: {
          text: '💰 **USD Coin (USDC)**\n\n- **Price**: 1.00 USD\n- **Chain**: SVM\n- **Address**: EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v\n- **Last Updated**: 3/21/2023, 2:32:15 PM',
          action: 'GET_PRICE',
        },
      },
    ],
    [
      {
        user: '{{user1}}',
        content: { text: 'How much is 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48 token worth?' },
      },
      {
        user: '{{agentName}}',
        content: {
          text: '💰 **USD Coin (USDC)**\n\n- **Price**: 1.00 USD\n- **Chain**: ETH\n- **Address**: 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48\n- **Last Updated**: 3/21/2023, 2:35:10 PM',
          action: 'GET_PRICE',
        },
      },
    ],
  ] as ActionExample[][],
};
