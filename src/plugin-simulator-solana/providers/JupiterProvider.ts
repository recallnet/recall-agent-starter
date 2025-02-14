import { PriceSource, TokenInfo } from '../types/index.ts';
import axios from 'axios';

export class JupiterProvider implements PriceSource {
  private readonly API_BASE = 'https://api.jup.ag/price/v2';
  private cache: Map<string, { price: number; timestamp: number; confidence: string }>;
  private readonly CACHE_DURATION = 30000; // 30 seconds
  private lastRequestTime: number = 0;
  private readonly MIN_REQUEST_INTERVAL = 100;
  private readonly MAX_RETRIES = 3;
  private readonly RETRY_DELAY = 1000; // 1 second

  constructor() {
    this.cache = new Map();
  }

  getName(): string {
    return 'Jupiter';
  }

  private async delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  private async enforceRateLimit(): Promise<void> {
    const now = Date.now();
    const timeSinceLastRequest = now - this.lastRequestTime;
    if (timeSinceLastRequest < this.MIN_REQUEST_INTERVAL) {
      await this.delay(this.MIN_REQUEST_INTERVAL - timeSinceLastRequest);
    }
    this.lastRequestTime = Date.now();
  }

  private getCachedPrice(tokenAddress: string): { price: number; confidence: string } | null {
    const cached = this.cache.get(tokenAddress);
    if (cached && Date.now() - cached.timestamp < this.CACHE_DURATION) {
      return { price: cached.price, confidence: cached.confidence };
    }
    return null;
  }

  private setCachedPrice(tokenAddress: string, price: number, confidence: string): void {
    this.cache.set(tokenAddress, {
      price,
      confidence,
      timestamp: Date.now(),
    });
  }

  async getPrice(tokenAddress: string): Promise<number | null> {
    try {
      // Check cache first
      const cachedPrice = this.getCachedPrice(tokenAddress);
      if (cachedPrice !== null && cachedPrice.confidence === 'high') {
        console.log(`[Jupiter] Using cached price for ${tokenAddress}: $${cachedPrice.price}`);
        return cachedPrice.price;
      }

      for (let attempt = 1; attempt <= this.MAX_RETRIES; attempt++) {
        try {
          await this.enforceRateLimit();

          const url = `${this.API_BASE}?ids=${tokenAddress}&showExtraInfo=true`;
          console.log(`[Jupiter] Debug: Full URL being used: ${url}`);

          console.log(
            `[Jupiter] Attempt ${attempt}/${this.MAX_RETRIES} to fetch price for ${tokenAddress}`,
          );

          const response = await axios.get(url, {
            timeout: 5000,
            headers: {
              Accept: 'application/json',
              'User-Agent': 'TradingModule/1.0',
            },
          });

          console.log(`[Jupiter] Received response status: ${response.status}`);

          if (!response.data?.data?.[tokenAddress]) {
            console.log(`[Jupiter] No price data found for token: ${tokenAddress}`);
            return null;
          }

          const tokenData = response.data.data[tokenAddress];
          const price = parseFloat(tokenData.price);
          if (isNaN(price)) {
            console.log(`[Jupiter] Invalid price format for token: ${tokenAddress}`);
            return null;
          }

          let confidence = 'low';
          if (tokenData.extraInfo?.confidenceLevel) {
            confidence = tokenData.extraInfo.confidenceLevel.toLowerCase();
          }

          this.setCachedPrice(tokenAddress, price, confidence);
          console.log(`[Jupiter] Successfully fetched price for ${tokenAddress}: $${price}`);
          return price;
        } catch (error) {
          if (attempt === this.MAX_RETRIES) {
            throw error;
          }
          console.log(`[Jupiter] Attempt ${attempt} failed, retrying after delay...`);
          if (axios.isAxiosError(error)) {
            console.error(`[Jupiter] Axios error details:`, {
              message: error.message,
              code: error.code,
              status: error.response?.status,
              data: error.response?.data,
            });
          }
          await this.delay(this.RETRY_DELAY * attempt);
        }
      }
      return null;
    } catch (error) {
      if (axios.isAxiosError(error)) {
        console.error(`[Jupiter] API error for ${tokenAddress}:`, {
          status: error.response?.status,
          message: error.message,
          data: error.response?.data,
          config: {
            url: error.config?.url,
            method: error.config?.method,
            headers: error.config?.headers,
          },
        });
      } else {
        console.error(
          `[Jupiter] Unexpected error for ${tokenAddress}:`,
          error instanceof Error
            ? {
                name: error.name,
                message: error.message,
                stack: error.stack,
              }
            : 'Unknown error',
        );
      }
      return null;
    }
  }

  async supports(tokenAddress: string): Promise<boolean> {
    try {
      console.log(`[Jupiter] Checking support for token: ${tokenAddress}`);
      // First check cache
      if (this.getCachedPrice(tokenAddress) !== null) {
        return true;
      }

      const price = await this.getPrice(tokenAddress);
      return price !== null;
    } catch (error) {
      console.error(`[Jupiter] Error checking token support:`, error);
      return false;
    }
  }
}
