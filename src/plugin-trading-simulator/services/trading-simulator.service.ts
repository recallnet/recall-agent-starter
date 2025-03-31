import { elizaLogger, Service, ServiceType, IAgentRuntime } from '@elizaos/core';
import {
  ApiResponse,
  BalancesResponse,
  BlockchainType,
  CompetitionStatusResponse,
  CompetitionRulesResponse,
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
const baseUrl = process.env.TRADING_SIM_API_URL || 'http://localhost:3000';
const debug = process.env.TRADING_SIM_DEBUG === 'true';

/**
 * Service for interacting with the Trading Simulator API
 */
export class TradingSimulatorService extends Service {
  static serviceType: ServiceType = 'tradingsimulator' as ServiceType;
  private apiKey: string;
  private baseUrl: string;
  private debug: boolean;
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
      this.runtime = _runtime;
      this.apiKey = apiKey.trim();
      this.baseUrl = baseUrl.endsWith('/') ? baseUrl.slice(0, -1) : baseUrl;
      this.debug = debug;

      elizaLogger.info('TradingSimulatorService initialized successfully');
    } catch (error) {
      elizaLogger.error(`Error initializing TradingSimulatorService: ${error.message}`);
      throw error;
    }
  }

  /**
   * Generate the required headers for API authentication
   */
  private generateHeaders(): Record<string, string> {
    const headers: Record<string, string> = {
      Authorization: `Bearer ${this.apiKey}`,
      'Content-Type': 'application/json',
      'User-Agent': 'TradingSimulatorPlugin/1.0',
    };

    if (this.debug) {
      elizaLogger.info('[TradingSimulator] Request headers:');
      elizaLogger.info('[TradingSimulator] Authorization: Bearer xxxxx... (masked)');
      elizaLogger.info('[TradingSimulator] Content-Type: application/json');
    }

    return headers;
  }

  /**
   * Make a request to the API
   */
  private async request<T extends ApiResponse>(
    method: string,
    path: string,
    body: any = null,
  ): Promise<T> {
    const url = `${this.baseUrl}${path}`;
    const bodyString = body ? JSON.stringify(body) : undefined;
    const headers = this.generateHeaders();

    const options: RequestInit = {
      method: method.toUpperCase(),
      headers,
      body: bodyString,
    };

    if (this.debug) {
      elizaLogger.info('[TradingSimulator] Request details:');
      elizaLogger.info(`[TradingSimulator] Method: ${method}`);
      elizaLogger.info(`[TradingSimulator] URL: ${url}`);
      elizaLogger.info(`[TradingSimulator] Body: ${body ? JSON.stringify(body, null, 2) : 'none'}`);
    }

    let response: Response;
    try {
      response = await fetch(url, options);
    } catch (networkError) {
      elizaLogger.error(`[TradingSimulator] Network error during fetch: ${networkError}`);
      throw new Error('Network error occurred while making API request.');
    }

    const responseText = await response.text();

    if (!response.ok) {
      let errorMessage = `API request failed with status ${response.status}`;
      if (responseText.trim()) {
        try {
          const errorData = JSON.parse(responseText);
          errorMessage = errorData.error?.message || errorData.message || errorMessage;
        } catch {
          errorMessage = responseText;
        }
      }
      throw new Error(errorMessage);
    }

    try {
      const data = JSON.parse(responseText);
      return data as T;
    } catch (parseError) {
      throw new Error(`Failed to parse successful response: ${parseError}`);
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
    let query = '';

    if (Object.keys(params).length > 0) {
      const queryParams = new URLSearchParams();
      if (params.limit) queryParams.append('limit', params.limit.toString());
      if (params.offset) queryParams.append('offset', params.offset.toString());
      if (params.token) queryParams.append('token', params.token);
      if (params.chain) queryParams.append('chain', params.chain);
      query = `?${queryParams.toString()}`;
    }

    elizaLogger.info(`Getting trade history from trading simulator: ${query}`);
    return this.request<TradesResponse>('GET', `/api/account/trades${query}`);
  }

  /**
   * Get current price for a token
   */
  public async getPrice(
    token: string,
    chain?: BlockchainType,
    specificChain?: SpecificChain,
  ): Promise<PriceResponse> {
    let query = `?token=${encodeURIComponent(token)}`;

    // Add chain parameter if explicitly provided
    if (chain) {
      query += `&chain=${chain}`;
    }

    // Add specificChain parameter if provided (for EVM tokens)
    if (specificChain) {
      query += `&specificChain=${specificChain}`;
    }

    elizaLogger.info(`Getting price for token ${token} from trading simulator`);
    return this.request<PriceResponse>('GET', `/api/price${query}`);
  }

  /**
   * Get detailed information about a token
   */
  public async getTokenInfo(
    token: string,
    chain?: BlockchainType,
    specificChain?: SpecificChain,
  ): Promise<TokenInfoResponse> {
    let query = `?token=${encodeURIComponent(token)}`;

    // Add chain parameter if explicitly provided
    if (chain) {
      query += `&chain=${chain}`;
    }

    // Add specificChain parameter if provided
    if (specificChain) {
      query += `&specificChain=${specificChain}`;
    }

    elizaLogger.info(`Getting token info for ${token} from trading simulator`);
    return this.request<TokenInfoResponse>('GET', `/api/price/token-info${query}`);
  }

  /**
   * Get historical price data for a token
   */
  public async getPriceHistory(params: PriceHistoryParams): Promise<PriceHistoryResponse> {
    const urlParams = new URLSearchParams();
    urlParams.append('token', params.token);

    if (params.startTime) urlParams.append('startTime', params.startTime);
    if (params.endTime) urlParams.append('endTime', params.endTime);
    if (params.interval) urlParams.append('interval', params.interval);
    if (params.chain) urlParams.append('chain', params.chain);
    if (params.specificChain) urlParams.append('specificChain', params.specificChain);

    const query = `?${urlParams.toString()}`;
    elizaLogger.info(`Getting price history for token ${params.token} from trading simulator`);
    return this.request<PriceHistoryResponse>('GET', `/api/price/history${query}`);
  }

  /**
   * Execute a trade between two tokens
   */
  public async executeTrade(params: TradeParams): Promise<TradeExecutionResponse> {
    // Create the request payload
    const payload: any = {
      fromToken: params.fromToken,
      toToken: params.toToken,
      amount: params.amount.toString(),
    };

    // Add optional parameters if they exist
    if (params.slippageTolerance) payload.slippageTolerance = params.slippageTolerance;
    if (params.fromChain) payload.fromChain = params.fromChain;
    if (params.toChain) payload.toChain = params.toChain;
    if (params.fromSpecificChain) payload.fromSpecificChain = params.fromSpecificChain;
    if (params.toSpecificChain) payload.toSpecificChain = params.toSpecificChain;

    // If chain parameters are not provided, try to detect them
    if (!params.fromChain) {
      payload.fromChain = this.detectChain(params.fromToken);
    }

    if (!params.toChain) {
      payload.toChain = this.detectChain(params.toToken);
    }

    elizaLogger.info(`Executing trade from ${params.fromToken} to ${params.toToken}`);
    return this.request<TradeExecutionResponse>('POST', '/api/trade/execute', payload);
  }

  /**
   * Get a quote for a potential trade
   */
  public async getQuote(
    fromToken: string,
    toToken: string,
    amount: string,
  ): Promise<QuoteResponse> {
    let query = `?fromToken=${encodeURIComponent(fromToken)}&toToken=${encodeURIComponent(toToken)}&amount=${encodeURIComponent(amount)}`;

    elizaLogger.info(`Getting quote for trade from ${fromToken} to ${toToken}`);
    return this.request<QuoteResponse>('GET', `/api/trade/quote${query}`);
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
  public async getLeaderboard(competitionId?: string): Promise<LeaderboardResponse> {
    const path = competitionId
      ? `/api/competition/leaderboard?competitionId=${competitionId}`
      : '/api/competition/leaderboard';
    elizaLogger.info('Getting leaderboard from trading simulator');
    return this.request<LeaderboardResponse>('GET', path);
  }

  /**
   * Get the rules for the current competition
   */
  public async getCompetitionRules(): Promise<CompetitionRulesResponse> {
    elizaLogger.info('Getting competition rules from trading simulator');
    return this.request<CompetitionRulesResponse>('GET', '/api/competition/rules');
  }

  /**
   * Helper method to detect chain from token address
   */
  public detectChain(token: string): BlockchainType {
    return detectChain(token);
  }
}
