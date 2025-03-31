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
import { executeTradeKeywords } from '../types.ts';
import { containsKeywords, extractAmount, extractTokenAddresses } from '../utils.ts';

export const executeTradeAction: Action = {
  name: 'EXECUTE_TRADE',
  similes: [
    'EXECUTE_TRADE',
    'TRADE_TOKENS',
    'SWAP_TOKENS',
    'EXCHANGE_TOKENS',
    'BUY_TOKEN',
    'SELL_TOKEN',
  ],
  validate: async (_runtime: IAgentRuntime, message: Memory) => {
    const text = message.content.text.toLowerCase();

    // Check if the message contains trade-related keywords
    if (!containsKeywords(text, executeTradeKeywords)) {
      return false;
    }

    // Check if there are at least two token addresses in the message
    const tokenAddresses = extractTokenAddresses(text);
    if (tokenAddresses.length < 2) {
      elizaLogger.info('EXECUTE_TRADE validation failed: Fewer than 2 token addresses found.');
      return false;
    }

    // Check if there's an amount in the message
    const amount = extractAmount(text);
    if (!amount) {
      elizaLogger.info('EXECUTE_TRADE validation failed: No amount found in message.');
      return false;
    }

    elizaLogger.info('EXECUTE_TRADE validation passed');
    return true;
  },
  description: 'Executes a trade between two tokens',
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

      // Extract token addresses and amount from the message
      const tokenAddresses = extractTokenAddresses(message.content.text);
      const amount = extractAmount(message.content.text);

      if (tokenAddresses.length < 2) {
        text = '⚠️ I need at least two different token addresses to execute a trade.';
      } else if (!amount) {
        text = "⚠️ I couldn't find a valid amount in your message.";
      } else {
        const fromToken = tokenAddresses[0];
        const toToken = tokenAddresses[1];

        // Get a quote first to show expected outcome
        elizaLogger.info(
          `Getting quote for trade from ${fromToken} to ${toToken} with amount ${amount}...`,
        );

        const quoteInfo = await tradingSimulatorService.getQuote(fromToken, toToken, amount);

        // Debug the quote response to help troubleshoot
        elizaLogger.info(`Quote response success flag: ${quoteInfo?.success}`);
        elizaLogger.info(`Quote response structure: ${JSON.stringify(quoteInfo)}`);

        // Check for a successful response - the API may not include a success flag
        // We consider the quote successful if we have exchange rate information
        if (
          quoteInfo?.success !== false &&
          (quoteInfo?.quote?.exchangeRate !== undefined || quoteInfo?.exchangeRate !== undefined)
        ) {
          // Extract exchange rate and estimated amount safely
          let exchangeRate = 0;
          let estimatedAmount = 0;

          if ('quote' in quoteInfo && quoteInfo.quote) {
            // If nested quote object is present
            exchangeRate = Number(quoteInfo.quote.exchangeRate) || 0;
            estimatedAmount =
              Number(quoteInfo.quote.estimatedToAmount || quoteInfo.quote.toAmount) || 0;
          } else {
            // If flat structure
            exchangeRate = Number(quoteInfo.exchangeRate) || 0;
            estimatedAmount = Number(quoteInfo.toAmount) || 0;
          }

          // Now execute the trade
          elizaLogger.info(
            `Executing trade from ${fromToken} to ${toToken} with amount ${amount}...`,
          );
          const tradeResult = await tradingSimulatorService.executeTrade({
            fromToken,
            toToken,
            amount,
            slippageTolerance: '0.5', // Default 0.5% slippage tolerance
          });

          // Debug the trade response structure
          elizaLogger.info(`Trade execution response: ${JSON.stringify(tradeResult)}`);

          // Check for a successful response
          // API might return trade info in either 'trade' or 'transaction' field
          if (tradeResult?.success && (tradeResult.trade || tradeResult.transaction)) {
            // Get the trade data from either the trade or transaction field
            const tradeData = tradeResult.trade || tradeResult.transaction;

            if (tradeData) {
              text =
                `✅ **Trade Executed Successfully**\n\n` +
                `- **From**: ${fromToken}\n` +
                `- **To**: ${toToken}\n` +
                `- **Amount Sent**: ${tradeData.fromAmount}\n` +
                `- **Amount Received**: ${tradeData.toAmount}\n` +
                `- **Exchange Rate**: 1 unit ≈ ${exchangeRate} units\n` +
                `- **Transaction ID**: ${tradeData.id}\n` +
                `- **Timestamp**: ${new Date(tradeData.timestamp).toLocaleString()}`;
            } else {
              elizaLogger.error(`EXECUTE_TRADE failed: No trade data in response.`);
              text = `⚠️ Trade execution failed. No trade data received.`;
            }
          } else {
            elizaLogger.error(`EXECUTE_TRADE failed: Trade execution failed.`);
            text = `⚠️ Trade execution failed. Please check your token balances and try again.`;
          }
        } else {
          elizaLogger.error(`EXECUTE_TRADE failed: Could not get quote.`);
          text = `⚠️ Unable to get a quote for this trade. Please check that both tokens are valid and try again.`;
        }
      }
    } catch (error: any) {
      elizaLogger.error(`EXECUTE_TRADE error: ${error.message}`);
      text = `⚠️ An error occurred while executing the trade: ${error.message}`;
    }

    // Create a new memory entry for the response
    const newMemory: Memory = {
      ...message,
      userId: message.agentId,
      content: {
        text,
        action: 'EXECUTE_TRADE',
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
        content: {
          text: 'Trade 100 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48 for 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2',
        },
      },
      {
        user: '{{agentName}}',
        content: {
          text: '✅ **Trade Executed Successfully**\n\n- **From**: 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48\n- **To**: 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2\n- **Amount Sent**: 100\n- **Amount Received**: 0.02\n- **Exchange Rate**: 1 unit ≈ 0.0002 units\n- **Transaction ID**: tx123456\n- **Timestamp**: 3/21/2023, 3:15:45 PM',
          action: 'EXECUTE_TRADE',
        },
      },
    ],
    [
      {
        user: '{{user1}}',
        content: {
          text: 'Swap 5 So11111111111111111111111111111111111111112 to EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
        },
      },
      {
        user: '{{agentName}}',
        content: {
          text: '✅ **Trade Executed Successfully**\n\n- **From**: So11111111111111111111111111111111111111112\n- **To**: EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v\n- **Amount Sent**: 5\n- **Amount Received**: 500\n- **Exchange Rate**: 1 unit ≈ 100 units\n- **Transaction ID**: tx789012\n- **Timestamp**: 3/21/2023, 3:18:22 PM',
          action: 'EXECUTE_TRADE',
        },
      },
    ],
    [
      {
        user: '{{user1}}',
        content: {
          text: 'Buy 0.5 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2 using 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48',
        },
      },
      {
        user: '{{agentName}}',
        content: {
          text: '✅ **Trade Executed Successfully**\n\n- **From**: 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48\n- **To**: 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2\n- **Amount Sent**: 2500\n- **Amount Received**: 0.5\n- **Exchange Rate**: 1 unit ≈ 0.0002 units\n- **Transaction ID**: tx345678\n- **Timestamp**: 3/21/2023, 3:22:10 PM',
          action: 'EXECUTE_TRADE',
        },
      },
    ],
  ] as ActionExample[][],
};
