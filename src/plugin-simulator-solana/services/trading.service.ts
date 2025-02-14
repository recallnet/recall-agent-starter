import { BalanceManager } from './BalanceManager.ts';
import { PriceTracker } from './PriceTracker.ts';
import { TradeSimulator } from './TradeSimulator.ts';
import { Trade, TradeResult, AccountState } from '../types/index.ts';
import { elizaLogger, Service, ServiceType } from '@elizaos/core';
import { ICotAgentRuntime } from '../../types/index.ts';

export class TradingService extends Service {
  static serviceType: ServiceType = 'trading' as ServiceType;
  private balanceManager: BalanceManager;
  private priceTracker: PriceTracker;
  public tradeSimulator: TradeSimulator;
  private runtime: ICotAgentRuntime;

  getInstance(): TradingService {
    return TradingService.getInstance();
  }

  async initialize(_runtime: ICotAgentRuntime) {
    // Default initial balances if none provided
    const defaultBalances = new Map<string, number>([
      ['So11111111111111111111111111111111111111112', 10], // 10 SOL
      ['EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', 1000], // 1000 USDC
      ['Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB', 1000], // 1000 USDT
    ]);
    this.runtime = _runtime;
    this.balanceManager = new BalanceManager(defaultBalances);
    this.priceTracker = new PriceTracker();
    this.tradeSimulator = new TradeSimulator(this.balanceManager, this.priceTracker);
    elizaLogger.info('TradingService initialized');
  }

  async executeTrade(fromToken: string, toToken: string, amount: number): Promise<TradeResult> {
    return this.tradeSimulator.executeTrade(fromToken, toToken, amount);
  }

  async getTokenPrice(tokenAddress: string): Promise<number | null> {
    return this.priceTracker.getPrice(tokenAddress);
  }

  getBalance(tokenAddress: string): number {
    return this.balanceManager.getBalance(tokenAddress);
  }

  getAllBalances() {
    return this.balanceManager.getAllBalances();
  }

  getTrades(): Trade[] {
    return this.tradeSimulator.getTrades();
  }

  async isTokenSupported(tokenAddress: string): Promise<boolean> {
    return this.priceTracker.isTokenSupported(tokenAddress);
  }

  getCurrentState(): AccountState {
    return {
      balances: new Map(this.balanceManager.getAllBalances().map((b) => [b.token, b.amount])),
      trades: this.getTrades(),
    };
  }

  updateBalance(tokenAddress: string, amount: number): void {
    this.balanceManager.updateBalance(tokenAddress, amount);
  }
}
