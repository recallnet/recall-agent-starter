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
import { competitionStatusKeywords } from '../types.ts';
import { containsKeywords } from '../utils.ts';

export const getCompetitionStatusAction: Action = {
  name: 'GET_COMPETITION_STATUS',
  similes: [
    'GET_COMPETITION_STATUS',
    'COMPETITION_INFO',
    'CONTEST_STATUS',
    'CHECK_COMPETITION',
    'TOURNAMENT_STATUS',
  ],
  validate: async (_runtime: IAgentRuntime, message: Memory) => {
    const text = message.content.text.toLowerCase();

    // Check if the message contains competition-related keywords
    if (!containsKeywords(text, competitionStatusKeywords)) {
      return false;
    }

    elizaLogger.info('GET_COMPETITION_STATUS validation passed');
    return true;
  },
  description: 'Retrieves information about the current trading competition',
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

      // Format time remaining function - define before use
      const formatTimeRemaining = (milliseconds: number): string => {
        const totalSeconds = Math.floor(milliseconds / 1000);
        const days = Math.floor(totalSeconds / 86400);
        const hours = Math.floor((totalSeconds % 86400) / 3600);
        const minutes = Math.floor(((totalSeconds % 86400) % 3600) / 60);

        let result = '';
        if (days > 0) result += `${days} day${days !== 1 ? 's' : ''} `;
        if (hours > 0) result += `${hours} hour${hours !== 1 ? 's' : ''} `;
        if (minutes > 0) result += `${minutes} minute${minutes !== 1 ? 's' : ''} `;

        return result.trim() || 'less than a minute';
      };

      elizaLogger.info('Fetching competition status...');
      const competitionInfo = await tradingSimulatorService.getCompetitionStatus();

      if (competitionInfo?.success && competitionInfo.competition) {
        const competition = competitionInfo.competition;

        // Safely determine if competition is active based on active field
        const isActive = !!competitionInfo.active;

        // Safely handle date parsing with null checks
        const startDate = competition.startTime ? new Date(competition.startTime) : null;
        const endDate = competition.endTime ? new Date(competition.endTime) : null;

        // Check if dates are valid before formatting
        const startTimeStr =
          startDate && !isNaN(startDate.getTime()) ? startDate.toLocaleString() : 'Not set';

        const endTimeStr =
          endDate && !isNaN(endDate.getTime()) ? endDate.toLocaleString() : 'Not set';

        // Determine time remaining from API or calculate if needed
        let timeRemaining = 'No end date set';
        if (competitionInfo.timeRemaining !== undefined) {
          // Use API-provided timeRemaining
          timeRemaining = formatTimeRemaining(competitionInfo.timeRemaining);
        } else if (endDate && !isNaN(endDate.getTime()) && endDate > new Date()) {
          // Calculate if API doesn't provide it
          const millisRemaining = endDate.getTime() - new Date().getTime();
          timeRemaining = formatTimeRemaining(millisRemaining);
        }

        // Safely build status display
        const statusDisplay = isActive
          ? `🟢 **Active**${timeRemaining !== 'No end date set' ? ` (${timeRemaining} remaining)` : ''}`
          : `🔴 **Inactive**`;

        // Safely access description with fallback
        const description = competition.description || 'No description available';

        text =
          `🏆 **Competition Status: ${competition.name || 'Unnamed Competition'}**\n\n` +
          `- **Status**: ${statusDisplay}\n` +
          `- **Description**: ${description}\n` +
          `- **Start Time**: ${startTimeStr}\n` +
          `- **End Time**: ${endTimeStr}`;

        // Add leaderboard info if competition is active
        if (isActive) {
          try {
            const leaderboardInfo = await tradingSimulatorService.getLeaderboard();
            if (
              leaderboardInfo?.success &&
              leaderboardInfo.leaderboard &&
              leaderboardInfo.leaderboard.length > 0
            ) {
              // Show top 3 teams
              const topTeams = leaderboardInfo.leaderboard.slice(0, 3);
              const leaderboardText = topTeams
                .map((team) => {
                  // Safe access to portfolio value with fallback
                  const portfolioValue =
                    team.portfolioValue !== undefined
                      ? team.portfolioValue.toLocaleString()
                      : 'N/A';

                  // Safe access to change24h with fallback
                  const change24h =
                    team.change24h !== undefined && team.change24h !== null
                      ? `(${team.change24h >= 0 ? '+' : ''}${team.change24h.toFixed(2)}%)`
                      : '';

                  return `${team.rank}. **${team.teamName}** - $${portfolioValue} ${change24h}`.trim();
                })
                .join('\n');

              text += `\n\n**Top Teams:**\n${leaderboardText}\n\n*Use "Show leaderboard" to see the full rankings*`;
            }
          } catch (error: unknown) {
            // Just don't add leaderboard info if it fails
            const errorMessage = error instanceof Error ? error.message : String(error);
            elizaLogger.error(`Error fetching leaderboard: ${errorMessage}`);
          }
        }
      } else {
        elizaLogger.error('GET_COMPETITION_STATUS failed: No competition info received.');
        text = '⚠️ Unable to retrieve competition status information. Please try again later.';
      }
    } catch (error: unknown) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      elizaLogger.error(`GET_COMPETITION_STATUS error: ${errorMessage}`);
      text = '⚠️ An error occurred while fetching competition status. Please try again later.';
    }

    // Create a new memory entry for the response
    const newMemory: Memory = {
      ...message,
      userId: message.agentId,
      content: {
        text,
        action: 'GET_COMPETITION_STATUS',
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
        content: { text: 'What is the status of the current competition?' },
      },
      {
        user: '{{agentName}}',
        content: {
          text: '🏆 **Competition Status: Spring Trading Competition**\n\n- **Status**: 🟢 **Active** (3 days 6 hours remaining)\n- **Description**: Compete for the highest portfolio value across multiple chains\n- **Start Time**: 3/18/2023, 9:00:00 AM\n- **End Time**: 3/25/2023, 9:00:00 AM\n\n**Top Teams:**\n1. **AlphaTrade** - $25,432,100 (+12.45%)\n2. **ChainMasters** - $22,156,780 (+8.92%)\n3. **TokenWarriors** - $19,876,540 (+6.74%)\n\n*Use "Show leaderboard" to see the full rankings*',
          action: 'GET_COMPETITION_STATUS',
        },
      },
    ],
    [
      {
        user: '{{user1}}',
        content: { text: 'Is there a competition running now?' },
      },
      {
        user: '{{agentName}}',
        content: {
          text: '🏆 **Competition Status: Spring Trading Competition**\n\n- **Status**: 🟢 **Active** (3 days 6 hours remaining)\n- **Description**: Compete for the highest portfolio value across multiple chains\n- **Start Time**: 3/18/2023, 9:00:00 AM\n- **End Time**: 3/25/2023, 9:00:00 AM\n\n**Top Teams:**\n1. **AlphaTrade** - $25,432,100 (+12.45%)\n2. **ChainMasters** - $22,156,780 (+8.92%)\n3. **TokenWarriors** - $19,876,540 (+6.74%)\n\n*Use "Show leaderboard" to see the full rankings*',
          action: 'GET_COMPETITION_STATUS',
        },
      },
    ],
    [
      {
        user: '{{user1}}',
        content: { text: 'Tell me about the current tournament' },
      },
      {
        user: '{{agentName}}',
        content: {
          text: '🏆 **Competition Status: Spring Trading Competition**\n\n- **Status**: 🟢 **Active** (3 days 6 hours remaining)\n- **Description**: Compete for the highest portfolio value across multiple chains\n- **Start Time**: 3/18/2023, 9:00:00 AM\n- **End Time**: 3/25/2023, 9:00:00 AM\n\n**Top Teams:**\n1. **AlphaTrade** - $25,432,100 (+12.45%)\n2. **ChainMasters** - $22,156,780 (+8.92%)\n3. **TokenWarriors** - $19,876,540 (+6.74%)\n\n*Use "Show leaderboard" to see the full rankings*',
          action: 'GET_COMPETITION_STATUS',
        },
      },
    ],
  ] as ActionExample[][],
};
