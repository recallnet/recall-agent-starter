import { Connection, PublicKey } from '@solana/web3.js';
import { Market } from '@project-serum/serum';
import { PriceSource } from '../types/index.ts';

export class SerumProvider implements PriceSource {
  private connection: Connection;

  constructor() {
    this.connection = new Connection('https://api.mainnet-beta.solana.com', 'confirmed');
  }

  getName(): string {
    return 'Serum';
  }

  async getPrice(tokenAddress: string): Promise<number | null> {
    try {
      const marketAddress = await this.findMarketAddress(tokenAddress);
      if (!marketAddress) return null;

      const market = await Market.load(
        this.connection,
        new PublicKey(marketAddress),
        {},
        new PublicKey('9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin'), // Serum program ID
      );

      const bids = await market.loadBids(this.connection);
      const asks = await market.loadAsks(this.connection);

      if (bids.getL2(1).length === 0 || asks.getL2(1).length === 0) {
        return null;
      }

      const midPrice = (bids.getL2(1)[0][0] + asks.getL2(1)[0][0]) / 2;
      return midPrice;
    } catch (error) {
      console.error(`Serum price fetch error for ${tokenAddress}:`, error);
      return null;
    }
  }

  private async findMarketAddress(tokenAddress: string): Promise<string | null> {
    // Implementation would need to maintain a mapping of token addresses to market addresses
    // or query them from Serum's API
    return null;
  }

  async supports(tokenAddress: string): Promise<boolean> {
    try {
      const marketAddress = await this.findMarketAddress(tokenAddress);
      return marketAddress !== null;
    } catch {
      return false;
    }
  }
}
