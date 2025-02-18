import OpenAI from 'openai';
import { elizaLogger } from '@elizaos/core';

const openai = new OpenAI({
  apiKey: process.env.OPENAI_API_KEY,
});

/**
 * Creates a vector embedding for the given text using OpenAI's API.
 * @param text The text to encode.
 * @returns A float32 array of embeddings.
 */
export async function createEmbedding(text: string | any): Promise<Float32Array> {
  try {
    // Ensure input is a string
    let processedText: string;
    if (text === null || text === undefined) {
      processedText = '';
    } else if (typeof text === 'string') {
      processedText = text;
    } else if (typeof text === 'object') {
      processedText = JSON.stringify(text);
    } else {
      processedText = String(text);
    }

    elizaLogger.info(`Creating embedding for text of length: ${processedText.length}`);

    const response = await openai.embeddings.create({
      model: 'text-embedding-3-small', // or "text-embedding-3-large" for higher quality
      input: processedText,
      encoding_format: 'float',
    });

    const embedding = new Float32Array(response.data[0].embedding);
    elizaLogger.info(`Successfully created embedding of length: ${embedding.length}`);

    return embedding;
  } catch (error) {
    elizaLogger.error(`Error in createEmbedding: ${error.message}`);
    if (error.stack) {
      elizaLogger.error(`Stack trace: ${error.stack}`);
    }
    // Return a zero vector of appropriate size as fallback
    return new Float32Array(1536); // Size for text-embedding-3-small
  }
}
