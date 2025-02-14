import { BucketManager } from '../../../../js-recall/packages/sdk/dist/index.js';
import { Address } from 'viem';

export class RealTimeStreamReader {
  private bucketManager: BucketManager;

  constructor(bucketManager: BucketManager) {
    this.bucketManager = bucketManager;
  }

  async streamObjectUpdates(
    bucket: Address,
    key: string,
    onData: (data: Uint8Array) => void,
    onError?: (error: Error) => void,
  ): Promise<() => void> {
    let isReading = true;
    let currentPosition = 0;

    const stopReading = () => {
      isReading = false;
    };

    const readStream = async () => {
      while (isReading) {
        try {
          // Get the current object state to check size
          const objectState = await this.bucketManager.getObjectValue(bucket, key);
          const totalSize = Number(objectState.result.size);

          // If we haven't read everything yet
          if (currentPosition < totalSize) {
            // Get a stream for the new portion
            const { result: stream } = await this.bucketManager.getStream(bucket, key, {
              start: currentPosition,
            });

            const reader = stream.getReader();

            while (true) {
              const { done, value } = await reader.read();
              if (done) break;
              if (!isReading) break;

              currentPosition += value.length;
              onData(value);
            }

            reader.releaseLock();
          }

          // Wait a bit before checking for more data
          await new Promise((resolve) => setTimeout(resolve, 1000));
        } catch (error) {
          if (onError) {
            onError(error instanceof Error ? error : new Error(String(error)));
          }
          // Wait before retrying after an error
          await new Promise((resolve) => setTimeout(resolve, 5000));
        }
      }
    };

    // Start reading
    readStream().catch((error) => {
      if (onError) {
        onError(error);
      }
    });

    // Return function to stop streaming
    return stopReading;
  }
}

// Usage example:
/*
const bucketManager = new BucketManager(client);
const streamReader = new RealTimeStreamReader(bucketManager);

const stopStreaming = await streamReader.streamObjectUpdates(
  bucketAddress,
  'myKey',
  (data) => {
    console.log('Received new data:', data);
  },
  (error) => {
    console.error('Stream error:', error);
  }
);

// Later when you want to stop streaming:
stopStreaming();
*/
