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

const priceKeywords = [
  'get price for token',
  'check price for token',
  'what is price for token',
  'get token price',
  'get token cost',
  'fetch token price',
  'query token price',
  'view token price',
];

const SOL_ADDRESS = 'So11111111111111111111111111111111111111112';
const USDC_ADDRESS = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';

export const getTokenPriceAction: Action = {
  name: 'GET_TOKEN_PRICE',
  similes: ['CHECK_TOKEN_PRICE', 'FETCH_TOKEN_PRICE', 'QUERY_TOKEN_PRICE', 'VIEW_TOKEN_PRICE'],

  description: 'Retrieves the current price for a specified token in SOL or USDC.',

  validate: async (_runtime: ICotAgentRuntime, message: Memory) => {
    const text = message.content.text.toLowerCase();
    elizaLogger.info(`GET_TOKEN_PRICE Validation triggered: ${message.content.text}`);

    // Check if message contains a price request
    const hasPriceKeyword = priceKeywords.some((keyword) => text.includes(keyword));

    // Validate format: token address in quotes and denomination in quotes
    const hasValidFormat = /"[a-zA-Z0-9]{32,44}".+in\s*"(sol|usdc)"/i.test(text);

    const val = hasPriceKeyword && hasValidFormat;
    elizaLogger.info(`GET_TOKEN_PRICE Validation result: ${val}`);
    return val;
  },

  handler: async (
    runtime: ICotAgentRuntime,
    message: Memory,
    state: State,
    _options: { [key: string]: unknown },
    callback?: HandlerCallback,
  ): Promise<boolean> => {
    let text = '';
    elizaLogger.info(`GET_TOKEN_PRICE Handler triggered: ${message.content.text}`);

    try {
      let currentState = state;
      if (!currentState) {
        currentState = (await runtime.composeState(message)) as State;
      } else {
        currentState = await runtime.updateRecentMessageState(currentState);
      }

      const tradingService = runtime.services.get('trading' as ServiceType) as TradingService;

      // Extract token address and denomination
      const addressMatch = message.content.text.match(/"([a-zA-Z0-9]{32,44})"/);
      const denomMatch = message.content.text.match(/in\s*"(sol|usdc)"/i);

      if (!addressMatch || !denomMatch) {
        text =
          '❌ Invalid format. Please specify token address and denomination (e.g., get price for token "TokenAddress" in "sol")';
        elizaLogger.error('GET_TOKEN_PRICE: Invalid format');
      } else {
        const tokenAddress = addressMatch[1];
        const denomination = denomMatch[1].toLowerCase();

        // All prices from getTokenPrice are in USDC
        const tokenPriceInUSDC = await tradingService.getTokenPrice(tokenAddress);

        if (tokenPriceInUSDC === null) {
          text = `❌ Unable to fetch price for token **${tokenAddress}**. The token may not be supported or there might be insufficient liquidity.`;
          elizaLogger.error(`GET_TOKEN_PRICE: No price available for ${tokenAddress}`);
          return true;
        }

        // If we need SOL denomination, get SOL price in USDC for conversion
        if (denomination === 'sol') {
          const solPriceInUSDC = await tradingService.getTokenPrice(SOL_ADDRESS);
          if (solPriceInUSDC === null) {
            text = `❌ Unable to convert price to SOL. Service may be temporarily unavailable.`;
            elizaLogger.error(`GET_TOKEN_PRICE: Unable to get SOL price for conversion`);
            return true;
          }

          const priceInSOL = tokenPriceInUSDC / solPriceInUSDC;
          text =
            `💰 Current price for token **${tokenAddress}**:\n` +
            `**${priceInSOL.toFixed(6)} SOL**`;
          elizaLogger.info(
            `GET_TOKEN_PRICE: Successfully converted price to SOL for ${tokenAddress}`,
          );
        } else {
          // For USDC denomination, use price directly
          text =
            `💰 Current price for token **${tokenAddress}**:\n` +
            `**${tokenPriceInUSDC.toFixed(6)} USDC**`;
          elizaLogger.info(
            `GET_TOKEN_PRICE: Successfully retrieved USDC price for ${tokenAddress}`,
          );
        }
      }
    } catch (error) {
      text = '⚠️ An error occurred while fetching the token price. Please try again later.';
      elizaLogger.error(`GET_TOKEN_PRICE error: ${error.message}`);
    }

    // Create a new memory entry for the response
    const newMemory: Memory = {
      ...message,
      userId: message.agentId,
      content: {
        text,
        action: 'GET_TOKEN_PRICE',
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
        content: {
          text: 'get price for token "So11111111111111111111111111111111111111112" in "usdc"',
        },
      },
      {
        user: '{{agentName}}',
        content: {
          text: '💰 Current price for token **So11111111111111111111111111111111111111112**:\n**22.45 USDC**',
          action: 'GET_TOKEN_PRICE',
        },
      },
    ],
    [
      {
        user: '{{user1}}',
        content: {
          text: 'check price for token "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v" in "sol"',
        },
      },
      {
        user: '{{agentName}}',
        content: {
          text: '💰 Current price for token **EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v**:\n**0.044 SOL**',
          action: 'GET_TOKEN_PRICE',
        },
      },
    ],
  ] as ActionExample[][],
};
