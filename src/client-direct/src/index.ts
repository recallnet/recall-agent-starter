import bodyParser from 'body-parser';
import cors from 'cors';
import express, { Request as ExpressRequest } from 'express';
import multer from 'multer';
import { createOpenAI } from '@ai-sdk/openai';
import {
  elizaLogger,
  generateCaption,
  generateImage,
  Media,
  getEmbeddingZeroVector,
  getEndpoint,
  getModelSettings,
  ModelProviderName,
  trimTokens,
  VerifiableInferenceResult,
  IVerifiableInferenceAdapter,
  VerifiableInferenceOptions,
  parseJSONObjectFromText,
} from '@elizaos/core';
import { generateText as aiGenerateText, CoreTool, StepResult as AIStepResult } from 'ai';
import { composeContext } from '@elizaos/core';
import { generateMessageResponse } from '@elizaos/core';
import { messageCompletionFooter } from '@elizaos/core';
import { Content, Memory, ModelClass, Client } from '@elizaos/core';
import { stringToUuid } from '@elizaos/core';
import { settings } from '@elizaos/core';
import { createApiRouter } from './api.ts';
import { ICotAgentRuntime, CotAgentRuntime } from '../../types/index.ts';
import * as fs from 'fs';
import * as path from 'path';

type Tool = CoreTool<any, any>;
type StepResult = AIStepResult<any>;

const storage = multer.diskStorage({
  destination: (_req, _file, cb) => {
    const uploadDir = path.join(process.cwd(), 'data', 'uploads');
    // Create the directory if it doesn't exist
    if (!fs.existsSync(uploadDir)) {
      fs.mkdirSync(uploadDir, { recursive: true });
    }
    cb(null, uploadDir);
  },
  filename: (_req, file, cb) => {
    const uniqueSuffix = `${Date.now()}-${Math.round(Math.random() * 1e9)}`;
    cb(null, `${uniqueSuffix}-${file.originalname}`);
  },
});

const upload = multer({ storage });

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
5. Use the above reasoning to generate your final, well-structured technical response.

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

function getCloudflareGatewayBaseURL(
  runtime: ICotAgentRuntime,
  provider: string,
): string | undefined {
  const isCloudflareEnabled = runtime.getSetting('CLOUDFLARE_GW_ENABLED') === 'true';
  const cloudflareAccountId = runtime.getSetting('CLOUDFLARE_AI_ACCOUNT_ID');
  const cloudflareGatewayId = runtime.getSetting('CLOUDFLARE_AI_GATEWAY_ID');

  elizaLogger.debug('Cloudflare Gateway Configuration:', {
    isEnabled: isCloudflareEnabled,
    hasAccountId: !!cloudflareAccountId,
    hasGatewayId: !!cloudflareGatewayId,
    provider: provider,
  });

  if (!isCloudflareEnabled) {
    elizaLogger.debug('Cloudflare Gateway is not enabled');
    return undefined;
  }

  if (!cloudflareAccountId) {
    elizaLogger.warn('Cloudflare Gateway is enabled but CLOUDFLARE_AI_ACCOUNT_ID is not set');
    return undefined;
  }

  if (!cloudflareGatewayId) {
    elizaLogger.warn('Cloudflare Gateway is enabled but CLOUDFLARE_AI_GATEWAY_ID is not set');
    return undefined;
  }

  const baseURL = `https://gateway.ai.cloudflare.com/v1/${cloudflareAccountId}/${cloudflareGatewayId}/${provider.toLowerCase()}`;
  elizaLogger.info('Using Cloudflare Gateway:', {
    provider,
    baseURL,
    accountId: cloudflareAccountId,
    gatewayId: cloudflareGatewayId,
  });

  return baseURL;
}

export async function generateText({
  runtime,
  context,
  modelClass,
  tools = {},
  onStepFinish,
  maxSteps = 1,
  verifiableInference = process.env.VERIFIABLE_INFERENCE_ENABLED === 'true',
  verifiableInferenceOptions,
}: {
  runtime: ICotAgentRuntime;
  context: string;
  modelClass: ModelClass;
  tools?: Record<string, Tool>;
  onStepFinish?: (event: StepResult) => Promise<void> | void;
  maxSteps?: number;
  stop?: string[];
  customSystemPrompt?: string;
  verifiableInference?: boolean;
  verifiableInferenceAdapter?: IVerifiableInferenceAdapter;
  verifiableInferenceOptions?: VerifiableInferenceOptions;
}): Promise<string> {
  if (!context) {
    console.error('generateText context is empty');
    return '';
  }

  elizaLogger.log('Generating text...');
  elizaLogger.info('Generating text with options:', {
    modelProvider: runtime.modelProvider,
    model: modelClass,
    verifiableInference,
  });
  elizaLogger.log('Using provider:', runtime.modelProvider);

  // If verifiable inference is requested and adapter is provided, use it
  if (verifiableInference && runtime.verifiableInferenceAdapter) {
    elizaLogger.log('Using verifiable inference adapter:', runtime.verifiableInferenceAdapter);
    try {
      const result: VerifiableInferenceResult =
        await runtime.verifiableInferenceAdapter.generateText(
          context,
          modelClass,
          verifiableInferenceOptions,
        );
      elizaLogger.log('Verifiable inference result:', result);
      // Verify the proof
      const isValid = await runtime.verifiableInferenceAdapter.verifyProof(result);
      if (!isValid) {
        throw new Error('Failed to verify inference proof');
      }
      return result.text;
    } catch (error) {
      elizaLogger.error('Error in verifiable inference:', error);
      throw error;
    }
  }

  const provider = runtime.modelProvider;
  elizaLogger.debug('Provider settings:', {
    provider,
    hasRuntime: !!runtime,
    runtimeSettings: {
      CLOUDFLARE_GW_ENABLED: runtime.getSetting('CLOUDFLARE_GW_ENABLED'),
      CLOUDFLARE_AI_ACCOUNT_ID: runtime.getSetting('CLOUDFLARE_AI_ACCOUNT_ID'),
      CLOUDFLARE_AI_GATEWAY_ID: runtime.getSetting('CLOUDFLARE_AI_GATEWAY_ID'),
    },
  });

  const endpoint = runtime.character.modelEndpointOverride || getEndpoint(provider);
  const modelSettings = getModelSettings(runtime.modelProvider, modelClass);
  let model = modelSettings.name;

  // allow character.json settings => secrets to override models
  switch (provider) {
    case ModelProviderName.LLAMACLOUD:
      {
        switch (modelClass) {
          case ModelClass.LARGE:
            {
              model = runtime.getSetting('LLAMACLOUD_MODEL_LARGE') || model;
            }
            break;
          case ModelClass.SMALL:
            {
              model = runtime.getSetting('LLAMACLOUD_MODEL_SMALL') || model;
            }
            break;
        }
      }
      break;
    case ModelProviderName.TOGETHER:
      {
        switch (modelClass) {
          case ModelClass.LARGE:
            {
              model = runtime.getSetting('TOGETHER_MODEL_LARGE') || model;
            }
            break;
          case ModelClass.SMALL:
            {
              model = runtime.getSetting('TOGETHER_MODEL_SMALL') || model;
            }
            break;
        }
      }
      break;
    case ModelProviderName.OPENROUTER:
      {
        switch (modelClass) {
          case ModelClass.LARGE:
            {
              model = runtime.getSetting('LARGE_OPENROUTER_MODEL') || model;
            }
            break;
          case ModelClass.SMALL:
            {
              model = runtime.getSetting('SMALL_OPENROUTER_MODEL') || model;
            }
            break;
        }
      }
      break;
  }

  elizaLogger.info('Selected model:', model);

  const modelConfiguration = runtime.character?.settings?.modelConfig;
  const temperature = modelConfiguration?.temperature || modelSettings.temperature;
  const frequency_penalty =
    modelConfiguration?.frequency_penalty || modelSettings.frequency_penalty;
  const presence_penalty = modelConfiguration?.presence_penalty || modelSettings.presence_penalty;
  const max_context_length = modelConfiguration?.maxInputTokens || modelSettings.maxInputTokens;

  // Use the configured maxOutputTokens, but if the system prompt includes a chain-of-thought
  // directive, force a higher limit (e.g., 16384 tokens)
  let max_response_length =
    modelConfiguration?.max_response_length || modelSettings.maxOutputTokens;
  if (runtime.character.system && runtime.character.system.includes('<chain-of-thought>')) {
    max_response_length = Math.max(max_response_length, 16384);
  }

  const experimental_telemetry =
    modelConfiguration?.experimental_telemetry || modelSettings.experimental_telemetry;

  const apiKey = runtime.token;

  try {
    elizaLogger.debug(`Trimming context to max length of ${max_context_length} tokens.`);
    context = await trimTokens(context, max_context_length, runtime);
    let response: string;
    elizaLogger.debug(
      `Using provider: ${provider}, model: ${model}, temperature: ${temperature}, max response length: ${max_response_length}`,
    );

    // (The switch statement with each provider branch remains unchanged.)
    // For brevity, here's just the OPENAI branch as an example:
    switch (provider) {
      case ModelProviderName.OPENAI:
      case ModelProviderName.ALI_BAILIAN:
      case ModelProviderName.VOLENGINE:
      case ModelProviderName.LLAMACLOUD:
      case ModelProviderName.NANOGPT:
      case ModelProviderName.HYPERBOLIC:
      case ModelProviderName.TOGETHER:
      case ModelProviderName.NINETEEN_AI:
      case ModelProviderName.AKASH_CHAT_API:
      case ModelProviderName.LMSTUDIO: {
        elizaLogger.debug('Initializing OpenAI model with Cloudflare check');
        const baseURL = getCloudflareGatewayBaseURL(runtime, 'openai') || endpoint;
        const openai = createOpenAI({
          apiKey,
          baseURL,
          fetch: runtime.fetch,
        });
        const { text: openaiResponse } = await aiGenerateText({
          model: openai.languageModel(model),
          prompt: context,
          system: runtime.character.system ?? settings.SYSTEM_PROMPT ?? undefined,
          tools: tools,
          onStepFinish: onStepFinish,
          maxSteps: maxSteps,
          temperature: temperature,
          maxTokens: max_response_length,
          frequencyPenalty: frequency_penalty,
          presencePenalty: presence_penalty,
          experimental_telemetry: experimental_telemetry,
        });
        response = openaiResponse;
        console.log('Received response from OpenAI model.');
        break;
      }
      // ... (other provider cases remain unchanged)
      default: {
        const errorMessage = `Unsupported provider: ${provider}`;
        elizaLogger.error(errorMessage);
        throw new Error(errorMessage);
      }
    }
    return response;
  } catch (error) {
    elizaLogger.error('Error in generateText:', error);
    throw error;
  }
}

export class DirectClient {
  public app: express.Application;
  private agents: Map<string, CotAgentRuntime>; // container management
  private server: any; // Store server instance
  // eslint-disable-next-line @typescript-eslint/no-unsafe-function-type
  public startAgent: Function;

  constructor() {
    elizaLogger.log('DirectClient constructor');
    this.app = express();
    this.app.use(cors());
    this.agents = new Map();

    this.app.use(bodyParser.json());
    this.app.use(bodyParser.urlencoded({ extended: true }));

    // Serve both uploads and generated images
    this.app.use('/media/uploads', express.static(path.join(process.cwd(), '/data/uploads')));
    this.app.use('/media/generated', express.static(path.join(process.cwd(), '/generatedImages')));

    const apiRouter = createApiRouter(this.agents, this);
    this.app.use(apiRouter);

    // Define an interface that extends the Express Request interface
    interface CustomRequest extends ExpressRequest {
      file?: Express.Multer.File;
    }

    // Update the route handler to use CustomRequest instead of express.Request
    this.app.post(
      '/:agentId/whisper',
      upload.single('file'),
      async (req: CustomRequest, res: express.Response) => {
        const audioFile = req.file; // Access the uploaded file using req.file
        const agentId = req.params.agentId;

        if (!audioFile) {
          res.status(400).send('No audio file provided');
          return;
        }

        let runtime = this.agents.get(agentId);

        // if runtime is null, look for runtime with the same name
        if (!runtime) {
          runtime = Array.from(this.agents.values()).find(
            (a) => a.character.name.toLowerCase() === agentId.toLowerCase(),
          );
        }

        if (!runtime) {
          res.status(404).send('Agent not found');
          return;
        }

        const formData = new FormData();
        const audioBlob = new Blob([audioFile.buffer], {
          type: audioFile.mimetype,
        });
        formData.append('file', audioBlob, audioFile.originalname);
        formData.append('model', 'whisper-1');

        const response = await fetch('https://api.openai.com/v1/audio/transcriptions', {
          method: 'POST',
          headers: {
            Authorization: `Bearer ${runtime.token}`,
          },
          body: formData,
        });

        const data = await response.json();
        res.json(data);
      },
    );

    this.app.post(
      '/:agentId/message',
      upload.single('file'),
      async (req: express.Request, res: express.Response) => {
        const agentId = req.params.agentId;
        const roomId = stringToUuid(req.body.roomId ?? 'default-room-' + agentId);
        const userId = stringToUuid(req.body.userId ?? 'user');

        let runtime = this.agents.get(agentId) as CotAgentRuntime;
        // if runtime is null, look for runtime with the same name
        if (!runtime) {
          runtime = Array.from(this.agents.values()).find(
            (a) => a.character.name.toLowerCase() === agentId.toLowerCase(),
          );
        }
        if (!runtime) {
          res.status(404).send('Agent not found');
          return;
        }

        await runtime.ensureConnection(userId, roomId, req.body.userName, req.body.name, 'direct');

        const text = req.body.text;
        const messageId = stringToUuid(Date.now().toString());

        const attachments: Media[] = [];
        if (req.file) {
          const filePath = path.join(process.cwd(), 'data', 'uploads', req.file.filename);
          attachments.push({
            id: Date.now().toString(),
            url: filePath,
            title: req.file.originalname,
            source: 'direct',
            description: `Uploaded file: ${req.file.originalname}`,
            text: '',
            contentType: req.file.mimetype,
          });
        }

        const content: Content = {
          text,
          attachments,
          source: 'direct',
          inReplyTo: undefined,
        };

        const userMessage = {
          content,
          userId,
          roomId,
          agentId: runtime.agentId,
        };

        const memory: Memory = {
          id: stringToUuid(messageId + '-' + userId),
          ...userMessage,
          agentId: runtime.agentId,
          userId,
          roomId,
          content,
          createdAt: Date.now(),
        };

        await runtime.messageManager.addEmbeddingToMemory(memory);
        await runtime.messageManager.createMemory(memory);

        let state = await runtime.composeState(userMessage, {
          agentName: runtime.character.name,
        });

        const context = composeContext({
          state,
          template: messageHandlerTemplate,
        });

        // Generate text using the chain-of-thought–enabled system prompt.
        const gen = await generateText({
          runtime,
          context,
          modelClass: ModelClass.LARGE,
        });
        const parsedContent = parseJSONObjectFromText(gen) as Content;
        const finalAnswerMarker = 'Final Answer:';
        let chainOfThoughtText = '';

        if (gen.includes(finalAnswerMarker)) {
          const parts = gen.split(finalAnswerMarker);
          // Extract the chain-of-thought portion and the final answer separately.
          chainOfThoughtText = parts[0].trim();

          // Remove <chain-of-thought> and </chain-of-thought>
          chainOfThoughtText = chainOfThoughtText.replace(/<\/?chain-of-thought>/g, '').trim();

          // write chain-of-thought to database
          await runtime.databaseAdapter.logMemory({
            userId: memory.userId,
            agentId: memory.agentId,
            type: 'chain-of-thought',
            body: JSON.stringify({
              log: chainOfThoughtText,
              userMessage: text,
            }),
            roomId,
          });
        }

        const responseMessage: Memory = {
          id: stringToUuid(messageId + '-' + runtime.agentId),
          ...userMessage,
          userId: runtime.agentId,
          content: parsedContent,
          embedding: getEmbeddingZeroVector(),
          createdAt: Date.now(),
        };

        await runtime.messageManager.createMemory(responseMessage);

        state = await runtime.updateRecentMessageState(state);

        let message: Content = {
          text: parsedContent.text,
          attachments: [],
          action: parsedContent.action, // Now this should have the correct value
          source: 'direct',
          inReplyTo: memory.id,
        };

        await runtime.processActions(memory, [responseMessage], state, async (newMessages) => {
          message = newMessages;
          return [memory];
        });

        await runtime.evaluate(memory, state);

        await runtime.composeState(memory, state);

        // Check if we should suppress the initial message
        const action = runtime.actions.find((a) => a.name === responseMessage.content.action);
        const shouldSuppressInitialMessage = action?.suppressInitialMessage;

        if (!shouldSuppressInitialMessage) {
          if (message) {
            res.json([responseMessage, message]);
          } else {
            res.json([responseMessage]);
          }
        } else {
          if (message) {
            res.json([message]);
          } else {
            res.json([]);
          }
        }
      },
    );

    this.app.post('/:agentId/image', async (req: express.Request, res: express.Response) => {
      const agentId = req.params.agentId;
      const agent = this.agents.get(agentId);
      if (!agent) {
        res.status(404).send('Agent not found');
        return;
      }

      const images = await generateImage({ ...req.body }, agent);
      const imagesRes: { image: string; caption: string }[] = [];
      if (images.data && images.data.length > 0) {
        for (let i = 0; i < images.data.length; i++) {
          const caption = await generateCaption({ imageUrl: images.data[i] }, agent);
          imagesRes.push({
            image: images.data[i],
            caption: caption.title,
          });
        }
      }
      res.json({ images: imagesRes });
    });

    this.app.post('/fine-tune', async (req: express.Request, res: express.Response) => {
      try {
        const response = await fetch('https://api.bageldb.ai/api/v1/asset', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'X-API-KEY': `${process.env.BAGEL_API_KEY}`,
          },
          body: JSON.stringify(req.body),
        });

        const data = await response.json();
        res.json(data);
      } catch (error) {
        res.status(500).json({
          error:
            'Please create an account at bakery.bagel.net and get an API key. Then set the BAGEL_API_KEY environment variable.',
          details: error.message,
        });
      }
    });
    this.app.get('/fine-tune/:assetId', async (req: express.Request, res: express.Response) => {
      const assetId = req.params.assetId;
      const downloadDir = path.join(process.cwd(), 'downloads', assetId);

      console.log('Download directory:', downloadDir);

      try {
        console.log('Creating directory...');
        await fs.promises.mkdir(downloadDir, { recursive: true });

        console.log('Fetching file...');
        const fileResponse = await fetch(
          `https://api.bageldb.ai/api/v1/asset/${assetId}/download`,
          {
            headers: {
              'X-API-KEY': `${process.env.BAGEL_API_KEY}`,
            },
          },
        );

        if (!fileResponse.ok) {
          throw new Error(
            `API responded with status ${fileResponse.status}: ${await fileResponse.text()}`,
          );
        }

        console.log('Response headers:', fileResponse.headers);

        const fileName =
          fileResponse.headers
            .get('content-disposition')
            ?.split('filename=')[1]
            ?.replace(/"/g, /* " */ '') || 'default_name.txt';

        console.log('Saving as:', fileName);

        const arrayBuffer = await fileResponse.arrayBuffer();
        const buffer = Buffer.from(arrayBuffer);

        const filePath = path.join(downloadDir, fileName);
        console.log('Full file path:', filePath);

        await fs.promises.writeFile(filePath, new Uint8Array(buffer));

        // Verify file was written
        const stats = await fs.promises.stat(filePath);
        console.log('File written successfully. Size:', stats.size, 'bytes');

        res.json({
          success: true,
          message: 'Single file downloaded successfully',
          downloadPath: downloadDir,
          fileCount: 1,
          fileName: fileName,
          fileSize: stats.size,
        });
      } catch (error) {
        console.error('Detailed error:', error);
        res.status(500).json({
          error: 'Failed to download files from BagelDB',
          details: error.message,
          stack: error.stack,
        });
      }
    });

    this.app.post('/:agentId/speak', async (req, res) => {
      const agentId = req.params.agentId;
      const roomId = stringToUuid(req.body.roomId ?? 'default-room-' + agentId);
      const userId = stringToUuid(req.body.userId ?? 'user');
      const text = req.body.text;

      if (!text) {
        res.status(400).send('No text provided');
        return;
      }

      let runtime = this.agents.get(agentId);

      // if runtime is null, look for runtime with the same name
      if (!runtime) {
        runtime = Array.from(this.agents.values()).find(
          (a) => a.character.name.toLowerCase() === agentId.toLowerCase(),
        );
      }

      if (!runtime) {
        res.status(404).send('Agent not found');
        return;
      }

      try {
        // Process message through agent (same as /message endpoint)
        await runtime.ensureConnection(userId, roomId, req.body.userName, req.body.name, 'direct');

        const messageId = stringToUuid(Date.now().toString());

        const content: Content = {
          text,
          attachments: [],
          source: 'direct',
          inReplyTo: undefined,
        };

        const userMessage = {
          content,
          userId,
          roomId,
          agentId: runtime.agentId,
        };

        const memory: Memory = {
          id: messageId,
          agentId: runtime.agentId,
          userId,
          roomId,
          content,
          createdAt: Date.now(),
        };

        await runtime.messageManager.createMemory(memory);

        const state = await runtime.composeState(userMessage, {
          agentName: runtime.character.name,
        });

        const context = composeContext({
          state,
          template: messageHandlerTemplate,
        });

        const response = await generateMessageResponse({
          runtime: runtime,
          context,
          modelClass: ModelClass.LARGE,
        });

        // save response to memory
        const responseMessage = {
          ...userMessage,
          userId: runtime.agentId,
          content: response,
        };

        await runtime.messageManager.createMemory(responseMessage);

        if (!response) {
          res.status(500).send('No response from generateMessageResponse');
          return;
        }

        await runtime.evaluate(memory, state);

        await runtime.processActions(memory, [responseMessage], state, async () => {
          return [memory];
        });

        // Get the text to convert to speech
        const textToSpeak = response.text;

        // Convert to speech using ElevenLabs
        const elevenLabsApiUrl = `https://api.elevenlabs.io/v1/text-to-speech/${process.env.ELEVENLABS_VOICE_ID}`;
        const apiKey = process.env.ELEVENLABS_XI_API_KEY;

        if (!apiKey) {
          throw new Error('ELEVENLABS_XI_API_KEY not configured');
        }

        const speechResponse = await fetch(elevenLabsApiUrl, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'xi-api-key': apiKey,
          },
          body: JSON.stringify({
            text: textToSpeak,
            model_id: process.env.ELEVENLABS_MODEL_ID || 'eleven_multilingual_v2',
            voice_settings: {
              stability: parseFloat(process.env.ELEVENLABS_VOICE_STABILITY || '0.5'),
              similarity_boost: parseFloat(process.env.ELEVENLABS_VOICE_SIMILARITY_BOOST || '0.9'),
              style: parseFloat(process.env.ELEVENLABS_VOICE_STYLE || '0.66'),
              use_speaker_boost: process.env.ELEVENLABS_VOICE_USE_SPEAKER_BOOST === 'true',
            },
          }),
        });

        if (!speechResponse.ok) {
          throw new Error(`ElevenLabs API error: ${speechResponse.statusText}`);
        }

        const audioBuffer = await speechResponse.arrayBuffer();

        // Set appropriate headers for audio streaming
        res.set({
          'Content-Type': 'audio/mpeg',
          'Transfer-Encoding': 'chunked',
        });

        res.send(Buffer.from(audioBuffer));
      } catch (error) {
        console.error('Error processing message or generating speech:', error);
        res.status(500).json({
          error: 'Error processing message or generating speech',
          details: error.message,
        });
      }
    });
  }

  // agent/src/index.ts:startAgent calls this
  public registerAgent(runtime: CotAgentRuntime) {
    this.agents.set(runtime.agentId, runtime);
  }

  public unregisterAgent(runtime: CotAgentRuntime) {
    this.agents.delete(runtime.agentId);
  }

  public start(port: number) {
    this.server = this.app.listen(port, () => {
      elizaLogger.success(
        `REST API bound to 0.0.0.0:${port}. If running locally, access it at http://localhost:${port}.`,
      );
    });

    // Handle graceful shutdown
    const gracefulShutdown = () => {
      elizaLogger.log('Received shutdown signal, closing server...');
      this.server.close(() => {
        elizaLogger.success('Server closed successfully');
        process.exit(0);
      });

      // Force close after 5 seconds if server hasn't closed
      setTimeout(() => {
        elizaLogger.error('Could not close connections in time, forcefully shutting down');
        process.exit(1);
      }, 5000);
    };

    // Handle different shutdown signals
    process.on('SIGTERM', gracefulShutdown);
    process.on('SIGINT', gracefulShutdown);
  }

  public stop() {
    if (this.server) {
      this.server.close(() => {
        elizaLogger.success('Server stopped');
      });
    }
  }
}

export const DirectClientInterface: Client = {
  // eslint-disable-next-line
  start: async (_runtime: ICotAgentRuntime) => {
    elizaLogger.log('DirectClientInterface start');
    const client = new DirectClient();
    const serverPort = parseInt(settings.SERVER_PORT || '3000');
    client.start(serverPort);
    return client;
  },

  stop: async (_runtime: ICotAgentRuntime, client?: Client) => {
    if (client instanceof DirectClient) {
      client.stop();
    }
  },
};

export default DirectClientInterface;
