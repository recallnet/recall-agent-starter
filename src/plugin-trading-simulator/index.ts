import { Plugin } from '@elizaos/core';
import { TradingSimulatorService } from './services/trading-simulator.service.ts';
import { executeTradeAction } from './actions/execute-trade.ts';
import { getBalancesAction } from './actions/get-balances.ts';
import { getCompetitionStatusAction } from './actions/get-competition-status.ts';
import { getLeaderboardAction } from './actions/get-leaderboard.ts';
import { getPortfolioAction } from './actions/get-portfolio.ts';
import { getPriceAction } from './actions/get-price.ts';

export const tradingSimulatorPlugin: Plugin = {
  name: 'Trading Simulator Plugin',
  description: 'Provides basic trading simulator utilities the action can perform',
  actions: [
    executeTradeAction,
    getBalancesAction,
    getCompetitionStatusAction,
    getLeaderboardAction,
    getPortfolioAction,
    getPriceAction,
  ],
  providers: [],
  services: [],
};

export default tradingSimulatorPlugin;
