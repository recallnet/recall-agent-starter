import { PriceSource, TokenInfo } from '../types/index.ts';
import axios from 'axios';

interface RaydiumPool {
  tvl: number;
  day?: {
    volume: number;
  };
  price: number;
  openTime: number;
  mintA: {
    address: string;
    symbol: string;
  };
  mintB: {
    address: string;
    symbol: string;
  };
}

export class RaydiumProvider implements PriceSource {
  private readonly API_BASE = 'https://api-v3.raydium.io/pools/info/mint';
  private cache: Map<string, { price: number; timestamp: number }>;
  private readonly CACHE_DURATION = 30000; // 30 seconds
  private readonly MAX_RETRIES = 3;
  private readonly RETRY_DELAY = 1000; // 1 second
  private readonly W_SOL = 'So11111111111111111111111111111111111111112';
  private readonly USDC = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';

  constructor() {
    this.cache = new Map();
  }

  getName(): string {
    return 'Raydium';
  }

  private async delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  private getCachedPrice(tokenAddress: string): number | null {
    const cached = this.cache.get(tokenAddress);
    if (cached && Date.now() - cached.timestamp < this.CACHE_DURATION) {
      return cached.price;
    }
    return null;
  }

  private setCachedPrice(tokenAddress: string, price: number): void {
    this.cache.set(tokenAddress, {
      price,
      timestamp: Date.now(),
    });
  }

  async getPrice(tokenAddress: string): Promise<number | null> {
    try {
      // Check cache first
      const cachedPrice = this.getCachedPrice(tokenAddress);
      if (cachedPrice !== null) {
        console.log(`[Raydium] Using cached price for ${tokenAddress}: $${cachedPrice}`);
        return cachedPrice;
      }

      console.log(`[Raydium] Fetching price for token: ${tokenAddress}`);
      for (let attempt = 1; attempt <= this.MAX_RETRIES; attempt++) {
        try {
          const url = new URL(this.API_BASE);
          url.searchParams.append('mint1', tokenAddress);
          url.searchParams.append('poolType', 'all');
          url.searchParams.append('sortField', 'tvl');
          url.searchParams.append('sortType', 'desc');
          url.searchParams.append('pageSize', '10');
          url.searchParams.append('page', '1');

          console.log(`[Raydium] Making API request to: ${url.toString()}`);

          const response = await axios.get(url.toString(), {
            timeout: 5000,
            headers: {
              Accept: 'application/json',
              'User-Agent': 'TradingModule/1.0',
            },
          });

          console.log(`[Raydium] Received response status: ${response.status}`);

          if (!response.data?.data) {
            console.log(`[Raydium] No pools found for token: ${tokenAddress}`);
            if (attempt === this.MAX_RETRIES) return null;
            await this.delay(this.RETRY_DELAY * attempt);
            continue;
          }

          const pools = response.data.data as RaydiumPool[];
          let bestPool: RaydiumPool | null = null;

          // First try to find a USDC pool
          bestPool =
            pools.find(
              (pool) =>
                (pool.mintA.address === this.USDC && pool.mintB.address === tokenAddress) ||
                (pool.mintB.address === this.USDC && pool.mintA.address === tokenAddress),
            ) || null;

          // If no USDC pool, try SOL pool
          if (!bestPool) {
            bestPool =
              pools.find(
                (pool) =>
                  (pool.mintA.address === this.W_SOL && pool.mintB.address === tokenAddress) ||
                  (pool.mintB.address === this.W_SOL && pool.mintA.address === tokenAddress),
              ) || null;
          }

          if (!bestPool) {
            console.log(`[Raydium] No suitable pools found for token: ${tokenAddress}`);
            return null;
          }

          const price = bestPool.price;
          if (isNaN(price) || price <= 0) {
            console.log(`[Raydium] Invalid price format for token: ${tokenAddress}`);
            return null;
          }

          console.log(`[Raydium] Found best pool for ${tokenAddress}:
                        TVL: ${bestPool.tvl}
                        Price: ${price}
                        Volume (24h): ${bestPool.day?.volume || 'N/A'}
                        Base Token: ${bestPool.mintA.symbol} (${bestPool.mintA.address})
                        Quote Token: ${bestPool.mintB.symbol} (${bestPool.mintB.address})
                    `);

          this.setCachedPrice(tokenAddress, price);
          return price;
        } catch (error) {
          if (attempt === this.MAX_RETRIES) throw error;
          console.log(`[Raydium] Attempt ${attempt} failed, retrying...`);
          await this.delay(this.RETRY_DELAY * attempt);
        }
      }
      return null;
    } catch (error) {
      console.error(`[Raydium] Error fetching price for ${tokenAddress}:`, error);
      return null;
    }
  }

  async supports(tokenAddress: string): Promise<boolean> {
    try {
      console.log(`[Raydium] Checking support for token: ${tokenAddress}`);
      if (this.getCachedPrice(tokenAddress) !== null) {
        return true;
      }

      const price = await this.getPrice(tokenAddress);
      return price !== null;
    } catch (error) {
      console.error(`[Raydium] Error checking token support:`, error);
      return false;
    }
  }
}
