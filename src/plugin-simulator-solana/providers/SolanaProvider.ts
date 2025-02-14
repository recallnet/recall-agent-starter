import { Connection, PublicKey } from '@solana/web3.js';
import { PriceSource, TokenInfo } from '../types/index.ts';

export class SolanaProvider implements PriceSource {
  private connection: Connection;

  constructor() {
    this.connection = new Connection('https://api.mainnet-beta.solana.com', 'confirmed');
  }

  getName(): string {
    return 'Solana RPC';
  }

  async getPrice(tokenAddress: string): Promise<number | null> {
    // Direct RPC calls can't get price data
    // This provider is mainly for token validation and metadata
    return null;
  }

  async getTokenInfo(tokenAddress: string): Promise<TokenInfo | null> {
    try {
      const tokenPublicKey = new PublicKey(tokenAddress);
      const accountInfo = await this.connection.getParsedAccountInfo(tokenPublicKey);

      if (!accountInfo.value) return null;

      return {
        address: tokenAddress,
        symbol: '', // Would need to fetch from token metadata
        decimals: 9, // Default SOL decimals, would need to fetch actual
      };
    } catch (error) {
      console.error(`Error fetching token info for ${tokenAddress}:`, error);
      return null;
    }
  }

  async supports(tokenAddress: string): Promise<boolean> {
    try {
      const info = await this.getTokenInfo(tokenAddress);
      return info !== null;
    } catch {
      return false;
    }
  }
}
