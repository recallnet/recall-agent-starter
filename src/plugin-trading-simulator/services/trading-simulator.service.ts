import { elizaLogger, Service, ServiceType, IAgentRuntime } from '@elizaos/core';
import * as crypto from 'crypto';
import {
  ApiResponse,
  BalancesResponse,
  BlockchainType,
  CompetitionStatusResponse,
  LeaderboardResponse,
  PriceHistoryParams,
  PriceHistoryResponse,
  PriceResponse,
  PortfolioResponse,
  SpecificChain,
  TokenInfoResponse,
  TradeExecutionResponse,
  TradeHistoryParams,
  TradeParams,
  TradesResponse,
  QuoteResponse,
  COMMON_TOKENS,
} from '../types.ts';
import { detectChain } from '../utils.ts';

const apiKey = process.env.TRADING_SIM_API_KEY;
const apiSecret = process.env.TRADING_SIM_API_SECRET as string;
const baseUrl = process.env.TRADING_SIM_API_URL as string;

/**
 * Service for interacting with the Trading Simulator API
 */
export class TradingSimulatorService extends Service {
  static serviceType: ServiceType = 'tradingsimulator' as ServiceType;
  private apiKey: string;
  private apiSecret: string;
  private baseUrl: string;
  private runtime: IAgentRuntime;

  getInstance(): TradingSimulatorService {
    return TradingSimulatorService.getInstance();
  }

  /**
   * Initialize the service with runtime and environment variables
   */
  async initialize(_runtime: IAgentRuntime): Promise<void> {
    try {
      if (!apiKey) {
        throw new Error('TRADING_SIM_API_KEY is required');
      }
      if (!apiSecret) {
        throw new Error('TRADING_SIM_API_SECRET is required');
      }
      if (!baseUrl) {
        throw new Error('TRADING_SIM_API_URL is required');
      }
      this.runtime = _runtime;
      this.apiKey = apiKey;
      this.apiSecret = apiSecret;
      this.baseUrl = baseUrl.endsWith('/') ? baseUrl.slice(0, -1) : baseUrl;

      elizaLogger.info('TradingSimulatorService initialized successfully');
    } catch (error) {
      elizaLogger.error(`Error initializing TradingSimulatorService: ${error.message}`);
      throw error;
    }
  }

  /**
   * Generate the required headers for API authentication
   */
  private generateHeaders(
    method: string,
    path: string,
    body: string = '{}',
  ): Record<string, string> {
    // Normalize method to uppercase
    const normalizedMethod = method.toUpperCase();

    // Don't modify the path - use it exactly as provided
    const normalizedPath = path;

    // Use current timestamp
    const timestamp = new Date().toISOString();

    // Important: Use '{}' for empty bodies to match the server's implementation
    const bodyString = body || '{}';

    // Remove query parameters for signature generation to match server behavior
    const pathForSignature = normalizedPath.split('?')[0];
    const data = normalizedMethod + pathForSignature + timestamp + bodyString;

    const signature = crypto.createHmac('sha256', this.apiSecret).update(data).digest('hex');

    return {
      'X-API-Key': this.apiKey,
      'X-Timestamp': timestamp,
      'X-Signature': signature,
      'Content-Type': 'application/json',
      'User-Agent': 'TradingSimulatorPlugin/1.0',
    };
  }

  /**
   * Make a request to the API
   */
  private async request<T extends ApiResponse>(
    method: string,
    path: string,
    body: any = null,
  ): Promise<T> {
    // Don't modify the path - use it exactly as provided
    const url = `${this.baseUrl}${path}`;

    // Handle body consistently - stringify once and only once
    let bodyString = '{}';
    if (body !== null) {
      bodyString = typeof body === 'string' ? body : JSON.stringify(body);
    }

    // Generate headers with the properly formatted body string
    const headers = this.generateHeaders(method, path, bodyString);

    const options: RequestInit = {
      method: method.toUpperCase(),
      headers,
      body: body !== null ? bodyString : undefined,
    };

    try {
      const response = await fetch(url, options);
      let data: any;

      try {
        const text = await response.text();
        try {
          data = JSON.parse(text);
          elizaLogger.info(JSON.stringify(data));
        } catch (parseError) {
          throw new Error(`Failed to parse response: ${parseError}`);
        }
      } catch (parseError) {
        throw new Error(`Failed to read response: ${parseError}`);
      }

      if (!response.ok) {
        throw new Error(
          data.error || data.message || `API request failed with status ${response.status}`,
        );
      }

      return data as T;
    } catch (error) {
      elizaLogger.error(`API Request Error: ${error.message}`);
      throw error;
    }
  }

  /**
   * Find token in COMMON_TOKENS and determine its chain information
   */
  private findTokenChainInfo(
    token: string,
  ): { chain: BlockchainType; specificChain: SpecificChain } | null {
    if (!token) return null;

    // Normalize EVM token addresses to lowercase for comparison
    const normalizedToken = token.startsWith('0x') ? token.toLowerCase() : token;

    // Check SVM tokens
    if (COMMON_TOKENS.SVM) {
      for (const [specificChain, tokens] of Object.entries(COMMON_TOKENS.SVM)) {
        for (const [_, address] of Object.entries(tokens)) {
          if (address === normalizedToken) {
            return {
              chain: BlockchainType.SVM,
              specificChain: SpecificChain.SVM,
            };
          }
        }
      }
    }

    // Check EVM tokens
    if (COMMON_TOKENS.EVM) {
      for (const [specificChain, tokens] of Object.entries(COMMON_TOKENS.EVM)) {
        for (const [_, address] of Object.entries(tokens)) {
          const normalizedAddress = (address as string).toLowerCase();
          if (normalizedAddress === normalizedToken) {
            return {
              chain: BlockchainType.EVM,
              specificChain: specificChain as SpecificChain,
            };
          }
        }
      }
    }

    return null;
  }

  /**
   * Get token balances across all supported chains
   */
  public async getBalances(): Promise<BalancesResponse> {
    elizaLogger.info('Getting balances from trading simulator');
    return this.request<BalancesResponse>('GET', '/api/account/balances');
  }

  /**
   * Get portfolio value and composition across chains
   */
  public async getPortfolio(): Promise<PortfolioResponse> {
    elizaLogger.info('Getting portfolio from trading simulator');
    return this.request<PortfolioResponse>('GET', '/api/account/portfolio');
  }

  /**
   * Get trade history with optional filters
   */
  public async getTrades(params: TradeHistoryParams = {}): Promise<TradesResponse> {
    const queryParams = new URLSearchParams();

    if (params.limit) queryParams.append('limit', params.limit.toString());
    if (params.offset) queryParams.append('offset', params.offset.toString());
    if (params.token) queryParams.append('token', params.token);
    if (params.chain) queryParams.append('chain', params.chain);

    const queryString = queryParams.toString();
    const path = `/api/account/trades${queryString ? `?${queryString}` : ''}`;

    elizaLogger.info(`Getting trade history from trading simulator: ${path}`);
    return this.request<TradesResponse>('GET', path);
  }

  /**
   * Get current price for a token
   */
  public async getPrice(
    token: string,
    chain?: BlockchainType,
    specificChain?: SpecificChain,
  ): Promise<PriceResponse> {
    const queryParams = new URLSearchParams();
    queryParams.append('token', token);

    // If chain not specified, try to detect it
    const detectedChain = chain || detectChain(token);
    queryParams.append('chain', detectedChain);

    // If specificChain not specified, try to find it from common tokens
    if (!specificChain) {
      const tokenInfo = this.findTokenChainInfo(token);
      if (tokenInfo) {
        specificChain = tokenInfo.specificChain;
      }
    }

    if (specificChain) queryParams.append('specificChain', specificChain);

    const path = `/api/price?${queryParams.toString()}`;

    elizaLogger.info(`Getting price for token ${token} from trading simulator`);
    return this.request<PriceResponse>('GET', path);
  }

  /**
   * Get detailed information about a token
   */
  public async getTokenInfo(
    token: string,
    chain?: BlockchainType,
    specificChain?: SpecificChain,
  ): Promise<TokenInfoResponse> {
    const queryParams = new URLSearchParams();
    queryParams.append('token', token);

    // If chain not specified, try to detect it
    const detectedChain = chain || detectChain(token);
    queryParams.append('chain', detectedChain);

    // If specificChain not specified, try to find it from common tokens
    if (!specificChain) {
      const tokenInfo = this.findTokenChainInfo(token);
      if (tokenInfo) {
        specificChain = tokenInfo.specificChain;
      }
    }

    if (specificChain) queryParams.append('specificChain', specificChain);

    const path = `/api/price/token-info?${queryParams.toString()}`;

    elizaLogger.info(`Getting token info for ${token} from trading simulator`);
    return this.request<TokenInfoResponse>('GET', path);
  }

  /**
   * Get historical price data for a token
   */
  public async getPriceHistory(params: PriceHistoryParams): Promise<PriceHistoryResponse> {
    const queryParams = new URLSearchParams();
    queryParams.append('token', params.token);

    if (params.startTime) queryParams.append('startTime', params.startTime);
    if (params.endTime) queryParams.append('endTime', params.endTime);
    if (params.interval) queryParams.append('interval', params.interval);

    // If chain not specified, try to detect it
    const chain = params.chain || detectChain(params.token);
    queryParams.append('chain', chain);

    // If specificChain not specified, try to find it from common tokens
    if (!params.specificChain) {
      const tokenInfo = this.findTokenChainInfo(params.token);
      if (tokenInfo) {
        params.specificChain = tokenInfo.specificChain;
      }
    }

    if (params.specificChain) queryParams.append('specificChain', params.specificChain);

    const path = `/api/price/history?${queryParams.toString()}`;

    elizaLogger.info(`Getting price history for token ${params.token} from trading simulator`);
    return this.request<PriceHistoryResponse>('GET', path);
  }

  /**
   * Execute a trade between two tokens
   */
  public async executeTrade(params: TradeParams): Promise<TradeExecutionResponse> {
    const payload: any = {
      fromToken: params.fromToken,
      toToken: params.toToken,
      amount: params.amount.toString(),
    };

    // Add optional parameters if they exist
    if (params.slippageTolerance) payload.slippageTolerance = params.slippageTolerance;
    if (params.price) payload.price = params.price.toString();

    // Check if the tokens are in COMMON_TOKENS and get their chain info
    const fromTokenInfo = this.findTokenChainInfo(params.fromToken);
    const toTokenInfo = this.findTokenChainInfo(params.toToken);

    // Add explicitly provided chain parameters if they exist
    let hasExplicitFromChain = false;
    let hasExplicitToChain = false;

    if (params.fromChain) {
      payload.fromChain = params.fromChain;
      hasExplicitFromChain = true;
    }

    if (params.toChain) {
      payload.toChain = params.toChain;
      hasExplicitToChain = true;
    }

    if (params.fromSpecificChain) {
      payload.fromSpecificChain = params.fromSpecificChain;
      hasExplicitFromChain = true;
    }

    if (params.toSpecificChain) {
      payload.toSpecificChain = params.toSpecificChain;
      hasExplicitToChain = true;
    }

    // If no explicit chain parameters were provided, auto-detect or use COMMON_TOKENS info

    // First, try same-chain trade if both tokens are found and on the same chain
    if (
      fromTokenInfo &&
      toTokenInfo &&
      fromTokenInfo.chain === toTokenInfo.chain &&
      fromTokenInfo.specificChain === toTokenInfo.specificChain
    ) {
      if (!hasExplicitFromChain) {
        payload.fromChain = fromTokenInfo.chain;
        payload.fromSpecificChain = fromTokenInfo.specificChain;
      }

      if (!hasExplicitToChain) {
        payload.toChain = toTokenInfo.chain;
        payload.toSpecificChain = toTokenInfo.specificChain;
      }
    }
    // For tokens where only one is known from COMMON_TOKENS
    else {
      // Auto-assign fromChain info if known and not explicitly provided
      if (fromTokenInfo && !hasExplicitFromChain) {
        payload.fromChain = fromTokenInfo.chain;
        payload.fromSpecificChain = fromTokenInfo.specificChain;
      }

      // Auto-assign toChain info if known and not explicitly provided
      if (toTokenInfo && !hasExplicitToChain) {
        payload.toChain = toTokenInfo.chain;
        payload.toSpecificChain = toTokenInfo.specificChain;
      }

      // For remaining unknown chains, use the autodetect
      if (!payload.fromChain) {
        payload.fromChain = detectChain(params.fromToken);
      }

      if (!payload.toChain) {
        payload.toChain = detectChain(params.toToken);
      }
    }

    try {
      elizaLogger.info(`Executing trade from ${params.fromToken} to ${params.toToken}`);
      return this.request<TradeExecutionResponse>('POST', '/api/trade/execute', payload);
    } catch (error) {
      // If the first attempt fails and we auto-assigned parameters for cross-chain trade,
      // try again with auto-assigned parameters removed
      if (
        error instanceof Error &&
        error.message.includes('cross-chain') &&
        (fromTokenInfo || toTokenInfo)
      ) {
        // Create a new payload without auto-assigned chain parameters
        const fallbackPayload: Record<string, any> = {
          fromToken: params.fromToken,
          toToken: params.toToken,
          amount: params.amount.toString(),
        };

        // Only keep explicitly provided parameters
        if (params.slippageTolerance) fallbackPayload.slippageTolerance = params.slippageTolerance;
        if (params.price) fallbackPayload.price = params.price.toString();
        if (params.fromChain) fallbackPayload.fromChain = params.fromChain;
        if (params.toChain) fallbackPayload.toChain = params.toChain;
        if (params.fromSpecificChain) fallbackPayload.fromSpecificChain = params.fromSpecificChain;
        if (params.toSpecificChain) fallbackPayload.toSpecificChain = params.toSpecificChain;

        // Try again with only explicit parameters
        elizaLogger.info(`Retrying trade with explicit parameters only`);
        return this.request<TradeExecutionResponse>('POST', '/api/trade/execute', fallbackPayload);
      }

      // Re-throw the error if it's not a cross-chain issue or we can't handle it
      throw error;
    }
  }

  /**
   * Get a quote for a potential trade
   */
  public async getQuote(
    fromToken: string,
    toToken: string,
    amount: string,
    fromChain?: BlockchainType,
    toChain?: BlockchainType,
    fromSpecificChain?: SpecificChain,
    toSpecificChain?: SpecificChain,
  ): Promise<QuoteResponse> {
    const queryParams = new URLSearchParams();
    queryParams.append('fromToken', fromToken);
    queryParams.append('toToken', toToken);
    queryParams.append('amount', amount);

    // If chains not specified, try to detect them
    const detectedFromChain = fromChain || detectChain(fromToken);
    const detectedToChain = toChain || detectChain(toToken);

    if (detectedFromChain) queryParams.append('fromChain', detectedFromChain);
    if (detectedToChain) queryParams.append('toChain', detectedToChain);

    // If specificChain not specified, try to find it from common tokens
    if (!fromSpecificChain) {
      const tokenInfo = this.findTokenChainInfo(fromToken);
      if (tokenInfo) {
        fromSpecificChain = tokenInfo.specificChain;
      }
    }

    if (!toSpecificChain) {
      const tokenInfo = this.findTokenChainInfo(toToken);
      if (tokenInfo) {
        toSpecificChain = tokenInfo.specificChain;
      }
    }

    // Add specific chain information if available
    if (fromSpecificChain) queryParams.append('fromSpecificChain', fromSpecificChain);
    if (toSpecificChain) queryParams.append('toSpecificChain', toSpecificChain);

    const path = `/api/trade/quote?${queryParams.toString()}`;

    elizaLogger.info(`Getting quote for trade from ${fromToken} to ${toToken}`);

    try {
      return this.request<QuoteResponse>('GET', path);
    } catch (error) {
      // If the first attempt fails and it might be a cross-chain issue, try again with minimal params
      if (error instanceof Error && error.message.includes('cross-chain')) {
        // Create a new query with just the essential parameters
        const fallbackParams = new URLSearchParams();
        fallbackParams.append('fromToken', fromToken);
        fallbackParams.append('toToken', toToken);
        fallbackParams.append('amount', amount);

        // Only add explicitly provided chain params
        if (fromChain) fallbackParams.append('fromChain', fromChain);
        if (toChain) fallbackParams.append('toChain', toChain);
        if (fromSpecificChain) fallbackParams.append('fromSpecificChain', fromSpecificChain);
        if (toSpecificChain) fallbackParams.append('toSpecificChain', toSpecificChain);

        const fallbackPath = `/api/trade/quote?${fallbackParams.toString()}`;
        elizaLogger.info(`Retrying quote with explicit parameters only`);
        return this.request<QuoteResponse>('GET', fallbackPath);
      }

      // Re-throw the error if it's not a cross-chain issue or we can't handle it
      throw error;
    }
  }

  /**
   * Get the status of the current competition
   */
  public async getCompetitionStatus(): Promise<CompetitionStatusResponse> {
    elizaLogger.info('Getting competition status from trading simulator');
    return this.request<CompetitionStatusResponse>('GET', '/api/competition/status');
  }

  /**
   * Get the competition leaderboard
   */
  public async getLeaderboard(): Promise<LeaderboardResponse> {
    elizaLogger.info('Getting leaderboard from trading simulator');
    return this.request<LeaderboardResponse>('GET', '/api/competition/leaderboard');
  }

  /**
   * Helper method to detect chain from token address
   */
  detectChain(token: string): BlockchainType {
    return detectChain(token);
  }
}
