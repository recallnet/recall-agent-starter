import { Balance, TokenInfo } from '../types/index.ts';

export class BalanceManager {
  private balances: Map<string, number>;

  constructor(initialBalance?: Map<string, number>) {
    this.balances = initialBalance || new Map();
  }

  getBalance(tokenAddress: string): number {
    return this.balances.get(tokenAddress) || 0;
  }

  getAllBalances(): Balance[] {
    return Array.from(this.balances.entries()).map(([token, amount]) => ({
      token,
      amount,
    }));
  }

  updateBalance(tokenAddress: string, amount: number): void {
    if (amount < 0) {
      throw new Error('Balance cannot be negative');
    }
    this.balances.set(tokenAddress, amount);
  }

  addAmount(tokenAddress: string, amount: number): void {
    const currentBalance = this.getBalance(tokenAddress);
    this.updateBalance(tokenAddress, currentBalance + amount);
  }

  subtractAmount(tokenAddress: string, amount: number): void {
    const currentBalance = this.getBalance(tokenAddress);
    if (currentBalance < amount) {
      throw new Error('Insufficient balance');
    }
    this.updateBalance(tokenAddress, currentBalance - amount);
  }
}
