import {
  type Action,
  type IAgentRuntime,
  type Memory,
  type State,
  type HandlerCallback,
  type ActionExample,
  elizaLogger,
  ServiceType,
} from '@elizaos/core';
import { TradingSimulatorService } from '../services/trading-simulator.service.ts';
import { leaderboardKeywords } from '../types.ts';
import { containsKeywords, formatPercentage } from '../utils.ts';

export const getLeaderboardAction: Action = {
  name: 'GET_LEADERBOARD',
  similes: [
    'GET_LEADERBOARD',
    'COMPETITION_RANKINGS',
    'SHOW_RANKINGS',
    'TOP_TRADERS',
    'CONTEST_LEADERS',
  ],
  validate: async (_runtime: IAgentRuntime, message: Memory) => {
    const text = message.content.text.toLowerCase();

    // Check if the message contains leaderboard-related keywords
    if (!containsKeywords(text, leaderboardKeywords)) {
      return false;
    }

    elizaLogger.info('GET_LEADERBOARD validation passed');
    return true;
  },
  description: 'Retrieves the competition leaderboard showing top-performing traders',
  handler: async (
    runtime: IAgentRuntime,
    message: Memory,
    state?: State,
    options?: { [key: string]: unknown },
    callback?: HandlerCallback,
  ): Promise<boolean> => {
    const tradingSimulatorService = runtime.services.get(
      'tradingsimulator' as ServiceType,
    ) as TradingSimulatorService;
    let text = '';

    try {
      let currentState = state;
      if (!currentState) {
        currentState = (await runtime.composeState(message)) as State;
      } else {
        currentState = await runtime.updateRecentMessageState(currentState);
      }

      elizaLogger.info('Fetching leaderboard...');

      // First check if there's an active competition
      const competitionInfo = await tradingSimulatorService.getCompetitionStatus();
      if (!competitionInfo.active) {
        text =
          '⚠️ There is no active competition at the moment. Leaderboard is only available during active competitions.';
      } else {
        // Fetch the leaderboard
        const leaderboardInfo = await tradingSimulatorService.getLeaderboard();

        if (
          leaderboardInfo?.success &&
          leaderboardInfo.leaderboard &&
          leaderboardInfo.leaderboard.length > 0
        ) {
          const competitionName = competitionInfo.competition?.name || 'Current Competition';

          // Format the leaderboard
          text = `🏆 **${competitionName} Leaderboard**\n\n`;

          // Create a table header
          text += `| **Rank** | **Team** | **Portfolio Value** | **24h Change** |\n`;
          text += `|---------|----------|-------------------|-------------|\n`;

          // Add each team to the table
          leaderboardInfo.leaderboard.forEach((team) => {
            const formattedValue = team.portfolioValue.toLocaleString('en-US', {
              style: 'currency',
              currency: 'USD',
              maximumFractionDigits: 0,
            });

            const changeFormatted = formatPercentage(team.change24h);

            text += `| ${team.rank} | ${team.teamName} | ${formattedValue} | ${changeFormatted} |\n`;
          });

          // Add footer with your team's ranking - display user's position if we have that info
          // Note: Using index to find team with matching ID would require knowing the user's teamId
          const userTeam = leaderboardInfo.leaderboard.find((team) => team.rank <= 10);
          if (userTeam) {
            text += `\n\n**Current Top Teams Shown** - Total Teams: ${leaderboardInfo.leaderboard.length}`;
          }

          // Add time remaining in competition
          if (competitionInfo.timeRemaining) {
            const days = Math.floor(competitionInfo.timeRemaining / (1000 * 60 * 60 * 24));
            const hours = Math.floor(
              (competitionInfo.timeRemaining % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60),
            );

            text += `\n\n*Competition ends in ${days} day${days !== 1 ? 's' : ''} and ${hours} hour${hours !== 1 ? 's' : ''}*`;
          }
        } else {
          text = '⚠️ Unable to retrieve leaderboard information. Please try again later.';
        }
      }
    } catch (error: unknown) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      elizaLogger.error(`GET_LEADERBOARD error: ${errorMessage}`);
      text = '⚠️ An error occurred while fetching the leaderboard. Please try again later.';
    }

    // Create a new memory entry for the response
    const newMemory: Memory = {
      ...message,
      userId: message.agentId,
      content: {
        text,
        action: 'GET_LEADERBOARD',
        source: message.content.source,
      },
    };

    // Save to memory
    await runtime.messageManager.createMemory(newMemory);

    // Call callback AFTER saving memory
    await callback?.({
      text,
    });

    return true;
  },
  examples: [
    [
      {
        user: '{{user1}}',
        content: { text: 'Show me the leaderboard' },
      },
      {
        user: '{{agentName}}',
        content: {
          text: "🏆 **Spring Trading Competition Leaderboard**\n\n| **Rank** | **Team** | **Portfolio Value** | **24h Change** |\n|---------|----------|-------------------|-------------|\n| 1 | AlphaTrade | $25,432,100 | +12.45% |\n| 2 | ChainMasters | $22,156,780 | +8.92% |\n| 3 | TokenWarriors | $19,876,540 | +6.74% |\n| 4 | CryptoWolves | $18,321,450 | +4.56% |\n| 5 | BlockchainBulls | $17,654,890 | +3.21% |\n| 6 | TokenTitans | $16,987,230 | +1.89% |\n| 7 | DEXDragons | $15,678,900 | -0.34% |\n| 8 | YieldYodas | $14,532,100 | -1.25% |\n| 9 | SolanaSquad | $13,245,670 | -2.67% |\n| 10 | EthereumElite | $12,876,540 | -3.45% |\n\n**Your Team's Rank**: 4 of 25\n\n*Competition ends in 3 days and 6 hours*",
          action: 'GET_LEADERBOARD',
        },
      },
    ],
    [
      {
        user: '{{user1}}',
        content: { text: "Who's winning the trading competition?" },
      },
      {
        user: '{{agentName}}',
        content: {
          text: "🏆 **Spring Trading Competition Leaderboard**\n\n| **Rank** | **Team** | **Portfolio Value** | **24h Change** |\n|---------|----------|-------------------|-------------|\n| 1 | AlphaTrade | $25,432,100 | +12.45% |\n| 2 | ChainMasters | $22,156,780 | +8.92% |\n| 3 | TokenWarriors | $19,876,540 | +6.74% |\n| 4 | CryptoWolves | $18,321,450 | +4.56% |\n| 5 | BlockchainBulls | $17,654,890 | +3.21% |\n\n**Your Team's Rank**: 4 of 25\n\n*Competition ends in 3 days and 6 hours*",
          action: 'GET_LEADERBOARD',
        },
      },
    ],
    [
      {
        user: '{{user1}}',
        content: { text: 'Display the current rankings' },
      },
      {
        user: '{{agentName}}',
        content: {
          text: "🏆 **Spring Trading Competition Leaderboard**\n\n| **Rank** | **Team** | **Portfolio Value** | **24h Change** |\n|---------|----------|-------------------|-------------|\n| 1 | AlphaTrade | $25,432,100 | +12.45% |\n| 2 | ChainMasters | $22,156,780 | +8.92% |\n| 3 | TokenWarriors | $19,876,540 | +6.74% |\n| 4 | CryptoWolves | $18,321,450 | +4.56% |\n| 5 | BlockchainBulls | $17,654,890 | +3.21% |\n| 6 | TokenTitans | $16,987,230 | +1.89% |\n| 7 | DEXDragons | $15,678,900 | -0.34% |\n| 8 | YieldYodas | $14,532,100 | -1.25% |\n| 9 | SolanaSquad | $13,245,670 | -2.67% |\n| 10 | EthereumElite | $12,876,540 | -3.45% |\n\n**Your Team's Rank**: 4 of 25\n\n*Competition ends in 3 days and 6 hours*",
          action: 'GET_LEADERBOARD',
        },
      },
    ],
  ] as ActionExample[][],
};
