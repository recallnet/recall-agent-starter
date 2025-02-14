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

const tradeKeywords = ['trade token', 'swap token', 'exchange token', 'convert token'];

export const executeTradeAction: Action = {
  name: 'EXECUTE_TOKEN_TRADE',
  similes: ['TRADE_TOKENS', 'SWAP_TOKENS', 'EXCHANGE_TOKENS', 'CONVERT_TOKENS'],

  description: 'Executes a trade between two token addresses.',

  validate: async (_runtime: ICotAgentRuntime, message: Memory) => {
    const text = message.content.text.toLowerCase();
    elizaLogger.info(`EXECUTE_TOKEN_TRADE Validation triggered: ${message.content.text}`);

    // Check if message contains a trade request
    const hasTradeKeyword = tradeKeywords.some((keyword) => text.includes(keyword));

    // Basic validation that we have amount and both token addresses in quotes
    // Format expected: 'swap 1 "address1" for "address2"' or similar
    const hasTradeFormat =
      /\d+(\.\d+)?\s*"[a-zA-Z0-9]{32,44}"\s+(?:to|for)\s+"[a-zA-Z0-9]{32,44}"/i.test(text);

    const val = hasTradeKeyword && hasTradeFormat;
    elizaLogger.info(`EXECUTE_TOKEN_TRADE Validation result: ${val}`);
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
    elizaLogger.info(`EXECUTE_TOKEN_TRADE Handler triggered: ${message.content.text}`);

    try {
      let currentState = state;
      if (!currentState) {
        currentState = (await runtime.composeState(message)) as State;
      } else {
        currentState = await runtime.updateRecentMessageState(currentState);
      }

      const tradingService = runtime.services.get('trading' as ServiceType) as TradingService;

      // Extract amount and token addresses using regex
      const tradeMatch = message.content.text.match(
        /(\d+(?:\.\d+)?)\s*"([a-zA-Z0-9]{32,44})"\s+(?:to|for)\s+"([a-zA-Z0-9]{32,44})"/i,
      );

      if (!tradeMatch) {
        text =
          '❌ Invalid trade format. Please specify amount and token addresses (e.g., "swap 1 "TokenAddress1" for "TokenAddress2"")';
        elizaLogger.error('EXECUTE_TOKEN_TRADE: Invalid trade format');
      } else {
        const amount = parseFloat(tradeMatch[1]);
        const fromAddress = tradeMatch[2];
        const toAddress = tradeMatch[3];

        // Execute the trade with direct addresses
        const tradeResult = await tradingService.executeTrade(fromAddress, toAddress, amount);

        if (tradeResult.success && tradeResult.trade) {
          text =
            `✅ Trade executed successfully!\n\n` +
            `Sent: **${tradeResult.trade.fromAmount} ${fromAddress}**\n` +
            `Received: **${tradeResult.trade.toAmount} ${toAddress}**\n` +
            `Rate: 1 ${fromAddress} = ${(tradeResult.trade.toAmount / tradeResult.trade.fromAmount).toFixed(4)} ${toAddress}`;
          elizaLogger.info('EXECUTE_TOKEN_TRADE: Trade executed successfully');
        } else {
          text = `❌ Trade failed: ${tradeResult.error || 'Unknown error'}`;
          elizaLogger.error(`EXECUTE_TOKEN_TRADE: Trade failed - ${tradeResult.error}`);
        }
      }
    } catch (error) {
      text = '⚠️ An error occurred while executing the trade. Please try again later.';
      elizaLogger.error(`EXECUTE_TOKEN_TRADE error: ${error.message}`);
    }

    // Create a new memory entry for the response
    const newMemory: Memory = {
      ...message,
      userId: message.agentId,
      content: {
        text,
        action: 'EXECUTE_TOKEN_TRADE',
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
          text: 'Swap token 1 "So11111111111111111111111111111111111111112" for "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"',
        },
      },
      {
        user: '{{agentName}}',
        content: {
          text: '✅ Trade executed successfully!\n\nSent: **1 So11111111111111111111111111111111111111112**\nReceived: **20 EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v**\nRate: 1 So11111111111111111111111111111111111111112 = 20.0000 EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
          action: 'EXECUTE_TOKEN_TRADE',
        },
      },
    ],
    [
      {
        user: '{{user1}}',
        content: {
          text: 'Trade token 50 "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB" to "6p6xgHyF7AeE6TZkSmFsko444wqoP15icUSqi2jfGiPN"',
        },
      },
      {
        user: '{{agentName}}',
        content: {
          text: '✅ Trade executed successfully!\n\nSent: **50 Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB**\nReceived: **100 6p6xgHyF7AeE6TZkSmFsko444wqoP15icUSqi2jfGiPN**\nRate: 1 Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB = 2.0000 6p6xgHyF7AeE6TZkSmFsko444wqoP15icUSqi2jfGiPN',
          action: 'EXECUTE_TOKEN_TRADE',
        },
      },
    ],
  ] as ActionExample[][],
};
