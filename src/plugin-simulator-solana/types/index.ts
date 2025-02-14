import { PublicKey } from '@solana/web3.js';

export interface TokenInfo {
  address: string;
  symbol: string;
  decimals: number;
  price?: number;
  lastUpdated?: Date;
}

export interface Balance {
  token: string; // Token address
  amount: number;
}

export interface Trade {
  timestamp: Date;
  fromToken: string;
  toToken: string;
  fromAmount: number;
  toAmount: number;
  price: number;
  success: boolean;
  error?: string;
}

export interface PriceSource {
  getName(): string;
  getPrice(tokenAddress: string): Promise<number | null>;
  supports(tokenAddress: string): Promise<boolean>;
}

export interface TradeResult {
  success: boolean;
  error?: string;
  trade?: Trade;
}

export interface AccountState {
  balances: Map<string, number>;
  trades: Trade[];
}
