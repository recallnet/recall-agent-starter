// Blockchain types
export enum BlockchainType {
  SVM = 'svm', // Solana Virtual Machine
  EVM = 'evm', // Ethereum Virtual Machine
}

// Specific EVM chains
export enum SpecificChain {
  ETH = 'eth',
  POLYGON = 'polygon',
  BSC = 'bsc',
  ARBITRUM = 'arbitrum',
  BASE = 'base',
  OPTIMISM = 'optimism',
  AVALANCHE = 'avalanche',
  LINEA = 'linea',
  SVM = 'svm',
}

// Common token addresses
export const COMMON_TOKENS = {
  // Solana tokens
  SVM: {
    SVM: {
      USDC: 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
      SOL: 'So11111111111111111111111111111111111111112',
    },
  },
  // Ethereum tokens
  EVM: {
    ETH: {
      USDC: '0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48',
      WETH: '0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2',
    },
    BASE: {
      USDC: '0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913',
      ETH: '0x4200000000000000000000000000000000000006',
    },
  },
};

// Trade Parameters
export interface TradeParams {
  fromToken: string;
  toToken: string;
  amount: string;
  price?: string;
  slippageTolerance?: string;
  fromChain?: BlockchainType;
  toChain?: BlockchainType;
  fromSpecificChain?: SpecificChain;
  toSpecificChain?: SpecificChain;
}

// Trade History Query Parameters
export interface TradeHistoryParams {
  limit?: number;
  offset?: number;
  token?: string;
  chain?: BlockchainType;
}

// Price History Parameters
export interface PriceHistoryParams {
  token: string;
  startTime?: string;
  endTime?: string;
  interval?: '1m' | '5m' | '15m' | '1h' | '4h' | '1d';
  chain?: BlockchainType;
  specificChain?: SpecificChain;
}

// API Response Types
export interface ApiResponse {
  success: boolean;
  error?: {
    code: string;
    message: string;
  };
}

export interface BalancesResponse extends ApiResponse {
  balances: Array<{
    token: string;
    amount: string;
    chain: BlockchainType;
    specificChain?: SpecificChain;
  }>;
  teamId?: string;
}

export interface PortfolioResponse extends ApiResponse {
  teamId?: string;
  portfolioValue: number;
  tokens: Array<{
    token: string;
    amount: number;
    valueUsd: number;
    chain: BlockchainType;
    specificChain?: SpecificChain;
  }>;
}

export interface TradesResponse extends ApiResponse {
  teamId?: string;
  trades: Array<{
    id: string;
    timestamp: string;
    fromToken: string;
    toToken: string;
    fromAmount: string;
    toAmount: string;
    fromChain: BlockchainType;
    toChain: BlockchainType;
    fromSpecificChain?: SpecificChain;
    toSpecificChain?: SpecificChain;
    status: string;
  }>;
  count: number;
  total: number;
}

export interface PriceResponse extends ApiResponse {
  price: number;
  token: string;
  chain: BlockchainType;
  specificChain?: SpecificChain;
  timestamp: string;
}

export interface TokenInfoResponse extends ApiResponse {
  token: string;
  name: string;
  symbol: string;
  decimals: number;
  chain: BlockchainType;
  specificChain?: SpecificChain;
  price: number;
  timestamp: string;
}

export interface PriceHistoryResponse extends ApiResponse {
  token: string;
  chain: BlockchainType;
  specificChain?: SpecificChain;
  prices: Array<{
    timestamp: string;
    price: number;
  }>;
}

export interface TradeExecutionResponse extends ApiResponse {
  trade?: {
    id: string;
    fromToken: string;
    toToken: string;
    fromAmount: string;
    toAmount: string;
    fromChain: BlockchainType;
    toChain: BlockchainType;
    fromSpecificChain?: SpecificChain;
    toSpecificChain?: SpecificChain;
    timestamp: string;
    status: string;
    price?: string;
    teamId?: string;
    competitionId?: string;
  };
  // API might return transaction field instead of trade
  transaction?: {
    id: string;
    fromToken: string;
    toToken: string;
    fromAmount: string; // Normalized to string for consistency
    toAmount: string; // Normalized to string for consistency
    price?: string; // Normalized to string for consistency
    success?: boolean;
    teamId?: string;
    competitionId?: string;
    fromChain?: BlockchainType;
    toChain?: BlockchainType;
    fromSpecificChain?: SpecificChain;
    toSpecificChain?: SpecificChain;
    timestamp: string;
    status?: string;
  };
}

export interface QuoteResponse extends ApiResponse {
  // The API might return a quote object or a flat structure
  quote?: {
    fromToken: string;
    toToken: string;
    fromAmount: string;
    estimatedToAmount?: string;
    toAmount?: string;
    exchangeRate: string;
    timestamp?: string;
    fromChain?: BlockchainType;
    toChain?: BlockchainType;
    fromSpecificChain?: SpecificChain;
    toSpecificChain?: SpecificChain;
  };
  // Direct fields for flat structure
  fromToken?: string;
  toToken?: string;
  fromAmount?: string; // Normalized to string for consistency
  toAmount?: string; // Normalized to string for consistency
  exchangeRate?: string; // Normalized to string for consistency
  slippage?: number;
  prices?: {
    fromToken: number;
    toToken: number;
  };
  chains?: {
    fromChain: BlockchainType;
    toChain: BlockchainType;
    fromSpecificChain?: SpecificChain;
    toSpecificChain?: SpecificChain;
  };
}

export interface CompetitionStatusResponse extends ApiResponse {
  competition: {
    id: string;
    name: string;
    description: string;
    status: string;
    startTime: string;
    endTime: string;
  };
  active: boolean;
  timeRemaining: number;
}

export interface LeaderboardResponse extends ApiResponse {
  competition: {
    id: string;
    name: string;
  };
  leaderboard: Array<{
    teamId: string;
    teamName: string;
    portfolioValue: number;
    rank: number;
    change24h: number;
  }>;
}

export interface CompetitionRulesResponse extends ApiResponse {
  rules: {
    tradingRules: string[];
    supportedChains: string[];
    rateLimits:
      | string[]
      | {
          tradeRequestsPerMinute: number;
          priceRequestsPerMinute: number;
          accountRequestsPerMinute: number;
          totalRequestsPerMinute: number;
          totalRequestsPerHour: number;
        };
    slippageFormula: string;
  };
}

// Keywords for action validation
export const balanceKeywords = [
  'balance',
  'balances',
  'token balance',
  'check my tokens',
  'my tokens',
  'what tokens do I have',
  'how many tokens',
  'check all my balances',
];

export const portfolioKeywords = [
  'portfolio',
  'my portfolio',
  'portfolio value',
  'portfolio status',
  'portfolio breakdown',
  'asset breakdown',
  'asset allocation',
  'current holdings',
];

export const tradeHistoryKeywords = [
  'trade history',
  'past trades',
  'my trades',
  'trading history',
  'previous trades',
  'transactions',
  'trade transactions',
];

export const priceKeywords = [
  'price',
  'token price',
  'current price',
  'price of',
  'how much is',
  'value of token',
];

export const tokenInfoKeywords = [
  'token info',
  'token information',
  'details about token',
  'token details',
  'tell me about token',
];

export const priceHistoryKeywords = [
  'price history',
  'historical price',
  'price over time',
  'price chart',
  'price graph',
  'historical data',
];

export const executeTradeKeywords = [
  'execute trade',
  'trade',
  'swap',
  'exchange',
  'buy',
  'sell',
  'convert',
];

export const quoteKeywords = [
  'quote',
  'trade quote',
  'exchange rate',
  'conversion rate',
  'how much will I get',
  'estimate',
];

export const competitionStatusKeywords = [
  'competition status',
  'competition',
  'contest status',
  'tournament status',
  'current competition',
];

export const leaderboardKeywords = [
  'leaderboard',
  'rankings',
  'standings',
  'top teams',
  'competition ranking',
  'who is winning',
];
