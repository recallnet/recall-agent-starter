import { Trade, TradeResult } from '../types/index.ts';
import { BalanceManager } from './BalanceManager.ts';
import { PriceTracker } from './PriceTracker.ts';

export class TradeSimulator {
  private balanceManager: BalanceManager;
  private priceTracker: PriceTracker;
  private trades: Trade[];

  constructor(balanceManager: BalanceManager, priceTracker: PriceTracker) {
    this.balanceManager = balanceManager;
    this.priceTracker = priceTracker;
    this.trades = [];
  }

  async executeTrade(fromToken: string, toToken: string, fromAmount: number): Promise<TradeResult> {
    try {
      console.log(`\n[TradeSimulator] Starting trade execution:
                From Token: ${fromToken}
                To Token: ${toToken}
                Amount: ${fromAmount}
            `);

      // Validate balances
      const currentBalance = this.balanceManager.getBalance(fromToken);
      console.log(`[TradeSimulator] Current balance of ${fromToken}: ${currentBalance}`);

      if (currentBalance < fromAmount) {
        console.log(`[TradeSimulator] Insufficient balance: ${currentBalance} < ${fromAmount}`);
        return {
          success: false,
          error: 'Insufficient balance',
        };
      }

      // Get prices
      const fromPrice = await this.priceTracker.getPrice(fromToken);
      const toPrice = await this.priceTracker.getPrice(toToken);

      console.log(`[TradeSimulator] Got prices:
                From Token (${fromToken}): $${fromPrice}
                To Token (${toToken}): $${toPrice}
            `);

      if (!fromPrice || !toPrice) {
        console.log(`[TradeSimulator] Missing price data:
                    From Token Price: ${fromPrice}
                    To Token Price: ${toPrice}
                `);
        return {
          success: false,
          error: 'Unable to determine price for tokens',
        };
      }

      // Calculate the trade using USD values
      const fromValueUSD = fromAmount * fromPrice;
      const toAmount = fromValueUSD / toPrice;

      // Debug logging for price calculations
      console.log(`[TradeSimulator] Trade calculation details:
                From Token (${fromToken}):
                - Amount: ${fromAmount}
                - Price: $${fromPrice}
                - USD Value: $${fromValueUSD.toFixed(6)}

                To Token (${toToken}):
                - Price: $${toPrice}
                - Calculated Amount: ${toAmount.toFixed(6)}

                Exchange Rate: 1 ${fromToken} = ${(toAmount / fromAmount).toFixed(6)} ${toToken}
            `);

      // Execute the trade
      this.balanceManager.subtractAmount(fromToken, fromAmount);
      this.balanceManager.addAmount(toToken, toAmount);

      const trade: Trade = {
        timestamp: new Date(),
        fromToken,
        toToken,
        fromAmount,
        toAmount,
        price: toAmount / fromAmount, // Exchange rate in terms of how many toTokens per fromToken
        success: true,
      };

      this.trades.push(trade);

      console.log(`[TradeSimulator] Trade executed successfully:
                Initial ${fromToken} Balance: ${currentBalance}
                New ${fromToken} Balance: ${this.balanceManager.getBalance(fromToken)}
                New ${toToken} Balance: ${this.balanceManager.getBalance(toToken)}
            `);

      return {
        success: true,
        trade,
      };
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : 'Unknown error during trade';
      console.error(`[TradeSimulator] Trade execution failed:`, errorMessage);
      return {
        success: false,
        error: errorMessage,
      };
    }
  }

  getTrades(): Trade[] {
    return [...this.trades];
  }
}
