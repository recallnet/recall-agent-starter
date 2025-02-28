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

export const systemPrompt = `# Instructions: Think step-by-step before responding.\n\nPlease follow these steps in your chain-of-thought:\n1. Identify the key technical elements in the conversation, including references from the injected knowledge base and recent messages.\n2. Break down the technical problem into smaller logical steps.\n3. Analyze the relevant technical details, context, and past interactions.\n4. Formulate a preliminary conclusion or solution that addresses the technical requirements.\n5. Use the above reasoning to generate your final, well-structured technical response.\n\n**Formatting Requirements:**\n\nPlease format your response using the following structure:\n\n<chain-of-thought>\n(full chain of thought logs go here, incorporating the injected technical knowledge and recent messages)\n</chain-of-thought>\n\nFinal Answer:\n(Your final technical answer goes here, written in a clear and conversational manner)\n\n# Examples\nExample Response:\n<chain-of-thought>\n1. I analyzed the conversation and noted that the user is facing a technical issue related to network configuration.\n2. I reviewed the injected knowledge base, which includes detailed documentation on network protocols, firewall settings, and troubleshooting techniques.\n3. I examined the recent messages, which mention that port 8080 is experiencing connectivity issues.\n4. I broke the problem down into checking the router configuration, verifying firewall rules, and ensuring that the port is open.\n</chain-of-thought>\nFinal Answer:\nBased on the analysis, please verify that your router's firewall settings allow external connections on port 8080, and review the network configuration for any misconfigurations.`;

export const messageHandlerTemplate =
  // {{goals}}
  `# Action Examples
{{actionExamples}}
(Action examples are for reference only. Do not use the information from them in your response.)

# Knowledge
{{knowledge}}

# Task: Generate a detailed technical response and actions for the character {{agentName}}.
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

Please follow these steps in your chain-of-thought:
1. Identify the key technical elements in the conversation, including references from the injected knowledge base and recent messages.
2. Break down the technical problem into smaller logical steps.
3. Analyze the relevant technical details, context, and past interactions.
4. Formulate a preliminary conclusion or solution that addresses the technical requirements.
5. If the users message aligns with any actions you can take, please note these in your response without executing, but be sure to include the reasoning behind them. Name the action and describe what it would do.
6. Use the above reasoning to generate your final, well-structured technical response.

**Formatting Requirements:**

Please format your response using the following structure:

<chain-of-thought>
(full chain of thought logs go here, incorporating the injected technical knowledge and recent messages)
</chain-of-thought>

Final Answer:
{{finalAnswer}}

# Examples
Keep in mind that the following examples are for reference only. Do not use the information from them in your response.
Example Response:
<chain-of-thought>
1. I analyzed the conversation and noted that the user is facing a technical issue related to network configuration.
2. I reviewed the injected knowledge base, which includes detailed documentation on network protocols, firewall settings, and troubleshooting techniques.
3. I examined the recent messages, which mention that port 8080 is experiencing connectivity issues.
4. I broke the problem down into checking the router configuration, verifying firewall rules, and ensuring that the port is open.
</chain-of-thought>
Final Answer:
Based on the analysis, please verify that your router's firewall settings allow external connections on port 8080, and review the network configuration for any misconfigurations.
 
# Generate the next message for {{agentName}}.
` + messageCompletionFooter;

export const cotProvider: Provider = {
  get: async (runtime: IAgentRuntime, message: Memory, _state?: State): Promise<string> => {
    const logPrefix = `[CoT Provider]`;
    elizaLogger.info(`${logPrefix} Starting chain-of-thought generation`);

    if (!process.env.RECALL_BUCKET_ALIAS) {
      elizaLogger.error(`${logPrefix} RECALL_BUCKET_ALIAS is not set - CoT logging may fail!`);
    }

    try {
      let state = _state;
      if (!state) {
        state = (await runtime.composeState(message)) as State;
      } else {
        state = await runtime.updateRecentMessageState(state);
      }

      runtime.character.system = systemPrompt;

      const context = composeContext({
        state,
        template: messageHandlerTemplate,
      });

      // Generate text using the chain-of-thought–enabled system prompt.
      elizaLogger.info(`${logPrefix} Generating text with LLM model`);
      const gen = await generateText({
        runtime,
        context,
        modelClass: ModelClass.LARGE,
      });
      elizaLogger.info(`${logPrefix} Text generation complete`);

      const finalAnswerMarker = 'Final Answer:';
      let chainOfThoughtText = '';

      if (gen.includes(finalAnswerMarker)) {
        elizaLogger.info(`${logPrefix} Final Answer marker found in response`);
        const parts = gen.split(finalAnswerMarker);

        // Extract the chain-of-thought portion and the final answer separately.
        chainOfThoughtText = parts[0].trim();

        // Remove <chain-of-thought> and </chain-of-thought>
        chainOfThoughtText = chainOfThoughtText.replace(/<\/?chain-of-thought>/g, '').trim();

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
      } else {
        elizaLogger.warn(`${logPrefix} No 'Final Answer:' marker found in LLM response!`);
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
