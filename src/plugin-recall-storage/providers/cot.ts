import {
  IAgentRuntime,
  Memory,
  Provider,
  State,
  elizaLogger,
  messageCompletionFooter,
  ModelClass,
  composeContext,
  generateText,
} from '@elizaos/core';
import { logMemoryPostgres, logMemorySqlite } from '../utils.ts';

export const systemPrompt = `You are an AI assistant helping with a conversation.
Before answering, please explicitly write out your step-by-step reasoning process starting with "REASONING:" and ending with "ANSWER:".
Always include both sections, even for simple questions.`;

export const messageHandlerTemplate = `# Action Examples
{{actionExamples}}
(Action examples are for reference only. Do not use the information from them in your response.)

# Knowledge
{{knowledge}}

# Task: Generate a detailed response and actions for the character {{agentName}}.
About {{agentName}}:
{{bio}}
{{lore}}

{{providers}}

{{attachments}}

# Capabilities
Note that {{agentName}} is capable of reading/seeing/hearing various forms of media, including images, videos, audio, plaintext and PDFs. Recent attachments have been included above under the "Attachments" section.

{{messageDirections}}

{{recentMessages}}

{{actions}}

# Instructions: Think step-by-step before responding.

Please follow these steps in your reasoning:
1. Identify the key elements in the conversation, including any knowledge and recent messages.
2. Break down the problem into smaller logical steps.
3. Analyze the relevant details, context, and past interactions.
4. Formulate a preliminary response that addresses the requirements.
5. If the user's message aligns with any actions you can take, consider which is most appropriate.

## Formatting Requirements

Your response MUST have two parts:

REASONING:
(Write your step-by-step analysis here)

ANSWER:
(Provide your final answer in the JSON format below)

# Response format should be formatted in a valid JSON block like this:
\`\`\`json
{ "user": "{{agentName}}", "text": "<string>", "action": "<string>" }
\`\`\`

The "action" field should be one of the options in [Available Actions] and the "text" field should be the response you want to send.
`;

/**
 * Extracts the chain of thought text from the model response.
 * Focuses on REASONING/ANSWER format and implements fallbacks.
 * @param text The model response text
 * @returns The extracted chain of thought text
 */
function extractChainOfThought(text: string): string {
  // Primary approach: Look for "REASONING:" section
  const reasoningMatch = text.match(/REASONING:\s*([\s\S]*?)(?=ANSWER:|$)/i);
  if (reasoningMatch && reasoningMatch[1]) {
    const reasoning = reasoningMatch[1].trim();
    if (reasoning.length > 0) {
      elizaLogger.info(`[extractChainOfThought] Successfully extracted REASONING section`);
      return reasoning;
    }
  }

  // Fallback 1: Look for text before the ANSWER section
  const answerMatch = text.match(/ANSWER:/i);
  if (answerMatch) {
    const answerIndex = text.indexOf(answerMatch[0]);
    if (answerIndex > 20) {
      const beforeAnswer = text.substring(0, answerIndex).trim();
      elizaLogger.info(`[extractChainOfThought] Extracted everything before ANSWER marker`);
      return beforeAnswer;
    }
  }

  // Fallback 2: Look for text before JSON
  const jsonMatch = text.match(/```json/i);
  if (jsonMatch) {
    const jsonIndex = text.indexOf(jsonMatch[0]);
    if (jsonIndex > 20) {
      // Ensure there's enough text before the JSON
      const beforeJson = text.substring(0, jsonIndex).trim();
      elizaLogger.info(`[extractChainOfThought] Extracted text before JSON formatting`);
      return beforeJson;
    }
  }

  // Last resort: If we couldn't extract anything, log a warning and take the first part of the text
  elizaLogger.warn(`[extractChainOfThought] Could not extract chain of thought with any pattern`);

  // If the response is longer than 500 characters, take the first 40% as a best guess
  if (text.length > 500) {
    const firstPortion = text.substring(0, Math.floor(text.length * 0.4));
    elizaLogger.info(`[extractChainOfThought] Using first 40% of response as fallback`);
    return `[Auto-extracted] ${firstPortion}`;
  }

  // If all else fails, indicate that we couldn't extract anything meaningful
  return '[Could not extract chain of thought]';
}

export const cotProvider: Provider = {
  get: async (runtime: IAgentRuntime, message: Memory, _state?: State): Promise<string> => {
    const logPrefix = `[CoT Provider]`;
    elizaLogger.info(`${logPrefix} Starting chain-of-thought generation`);

    try {
      let state = _state;
      if (!state) {
        state = (await runtime.composeState(message)) as State;
      } else {
        state = await runtime.updateRecentMessageState(state);
      }

      runtime.character.system = systemPrompt;

      state.actions = `# Actions \n${JSON.stringify(runtime.actions)}`;

      const context = composeContext({
        state,
        template: messageHandlerTemplate,
      });

      // Generate text using the chain-of-thought–enabled system prompt
      elizaLogger.info(`${logPrefix} Generating text with LLM model`);
      const gen = await generateText({
        runtime,
        context,
        modelClass: ModelClass.LARGE,
      });
      elizaLogger.info(`${logPrefix} Text generation complete`);

      // Log a preview of the response for debugging
      const previewLength = Math.min(gen.length, 500);
      elizaLogger.info(`${logPrefix} Response preview: ${gen.substring(0, previewLength)}...`);

      // Extract the chain of thought using REASONING/ANSWER markers
      let chainOfThoughtText = extractChainOfThought(gen);

      // Get user message text safely
      const userMessageText =
        message.content && typeof message.content === 'object' && message.content.text
          ? message.content.text
          : typeof message.content === 'string'
            ? message.content
            : '';

      // Format log data for storage
      const logData = {
        userId: message.userId,
        agentId: message.agentId,
        userMessage: userMessageText,
        log: chainOfThoughtText,
        timestamp: new Date().toISOString(),
      };

      // Store the log using the appropriate database-specific function
      try {
        const dbAdapter = runtime.databaseAdapter as any;

        if ('pool' in dbAdapter) {
          // PostgreSQL
          elizaLogger.info(`${logPrefix} Using PostgreSQL to log chain-of-thought`);
          await logMemoryPostgres(dbAdapter.pool, {
            userId: message.userId,
            agentId: message.agentId,
            roomId: message.roomId,
            type: 'chain-of-thought',
            body: JSON.stringify(logData),
          });
          elizaLogger.info(`${logPrefix} Successfully logged chain-of-thought to PostgreSQL`);
        } else if ('db' in dbAdapter) {
          // SQLite
          elizaLogger.info(`${logPrefix} Using SQLite to log chain-of-thought`);
          await logMemorySqlite(dbAdapter.db, {
            userId: message.userId,
            agentId: message.agentId,
            roomId: message.roomId,
            type: 'chain-of-thought',
            body: JSON.stringify(logData),
          });
          elizaLogger.info(`${logPrefix} Successfully logged chain-of-thought to SQLite`);
        } else {
          elizaLogger.error(`${logPrefix} Unsupported database adapter type`);
        }
      } catch (dbError) {
        elizaLogger.error(`${logPrefix} Database error while saving CoT log: ${dbError.message}`);
        elizaLogger.error(`${logPrefix} Error details:`, dbError);
      }

      elizaLogger.info(`${logPrefix} Chain-of-thought processing complete`);
      return chainOfThoughtText || '';
    } catch (error) {
      elizaLogger.error(`${logPrefix} Error in chain-of-thought provider:`);
      elizaLogger.error(`${logPrefix} ${error instanceof Error ? error.stack : 'Unknown error'}`);
      return '';
    }
  },
};
