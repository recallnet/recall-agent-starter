import { BlockchainType } from './types.ts';

/**
 * Detects the blockchain type based on the token address format
 * @param token The token address to check
 * @returns The detected blockchain type (EVM or SVM)
 */
export function detectChain(token: string): BlockchainType {
  if (token.startsWith('0x') && token.length === 42) {
    return BlockchainType.EVM;
  } else if (token.length >= 32 && token.length <= 44 && !token.startsWith('0x')) {
    return BlockchainType.SVM;
  }

  throw new Error(`Unable to detect chain for token address: ${token}`);
}

/**
 * Formats a currency value for display
 * @param value The value to format
 * @param decimals The number of decimal places to show
 * @returns Formatted currency string
 */
export function formatCurrency(value: number | string, decimals = 2): string {
  const numValue = typeof value === 'string' ? parseFloat(value) : value;

  if (isNaN(numValue)) {
    return '0.00';
  }

  // Use compact notation for large numbers
  if (numValue >= 1000000) {
    return `${(numValue / 1000000).toFixed(decimals)}M`;
  } else if (numValue >= 1000) {
    return `${(numValue / 1000).toFixed(decimals)}K`;
  }

  // Use 4 decimal places for values close to 1.00 (like stablecoins)
  if (numValue > 0.9 && numValue < 1.1) {
    return numValue.toFixed(4);
  }

  return numValue.toFixed(decimals);
}

/**
 * Formats a percentage value for display
 * @param value The value to format
 * @returns Formatted percentage string with + or - prefix
 */
export function formatPercentage(value: number | string): string {
  const numValue = typeof value === 'string' ? parseFloat(value) : value;

  if (isNaN(numValue)) {
    return '0.00%';
  }

  const prefix = numValue > 0 ? '+' : '';
  return `${prefix}${numValue.toFixed(2)}%`;
}

/**
 * Extracts token addresses from a message
 * @param text The message text to analyze
 * @returns Array of extracted token addresses
 */
export function extractTokenAddresses(text: string): string[] {
  // Match Ethereum addresses (0x followed by 40 hex characters)
  const evmRegex = /0x[a-fA-F0-9]{40}/g;
  // Match Solana addresses (32-44 characters, not starting with 0x)
  const svmRegex = /\b(?!0x)[1-9A-HJ-NP-Za-km-z]{32,44}\b/g;

  const evmMatches = text.match(evmRegex) || [];
  const svmMatches = text.match(svmRegex) || [];

  return [...evmMatches, ...svmMatches];
}

/**
 * Extracts a numeric amount from a message
 * @param text The message text to analyze
 * @returns The extracted amount or undefined if not found
 */
export function extractAmount(text: string): string | undefined {
  // Match numbers with optional decimal points
  const amountRegex = /\b\d+(\.\d+)?\b/g;
  const matches = text.match(amountRegex);

  if (matches && matches.length > 0) {
    return matches[0];
  }

  return undefined;
}

/**
 * Determines if text contains at least one of the specified keywords
 * @param text The text to check
 * @param keywords Array of keywords to look for
 * @returns True if at least one keyword is found
 */
export function containsKeywords(text: string, keywords: string[]): boolean {
  const lowerText = text.toLowerCase();
  return keywords.some((keyword) => lowerText.includes(keyword.toLowerCase()));
}
