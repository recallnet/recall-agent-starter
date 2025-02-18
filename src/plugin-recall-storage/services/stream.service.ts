import { ParquetSchema, ParquetWriter } from '@dsnp/parquetjs';
import { Writable } from 'stream';

/**
 * An in-memory writable stream for Parquet storage.
 */
class MemoryStream extends Writable {
  private chunks: Buffer[] = [];
  bytesWritten = 0;
  path: string | Buffer = ''; // ✅ Required for WriteStream compatibility
  pending = false; // ✅ Required for WriteStream compatibility

  constructor() {
    super();
  }

  _write(chunk: Buffer, _encoding: BufferEncoding, callback: (error?: Error | null) => void) {
    this.chunks.push(chunk);
    this.bytesWritten += chunk.length;
    callback();
  }

  write(
    chunk: any,
    encodingOrCallback?: BufferEncoding | ((error?: Error | null) => void),
    callback?: (error?: Error | null) => void,
  ): boolean {
    const encoding: BufferEncoding =
      typeof encodingOrCallback === 'string' ? encodingOrCallback : 'utf8';

    const cb: (error?: Error | null) => void =
      typeof encodingOrCallback === 'function' ? encodingOrCallback : callback || (() => {});

    this._write(chunk, encoding, cb);
    return true;
  }

  end(
    chunk?: any,
    encodingOrCallback?: BufferEncoding | (() => void),
    callback?: () => void,
  ): this {
    if (typeof chunk === 'function') {
      callback = chunk;
      chunk = undefined;
    } else if (typeof encodingOrCallback === 'function') {
      callback = encodingOrCallback;
      encodingOrCallback = undefined;
    }

    if (chunk !== undefined) {
      this.write(chunk, encodingOrCallback as BufferEncoding, callback);
    }

    this.emit('finish');
    if (callback) callback();

    return this;
  }

  // ✅ Improved `close()` for better callback handling
  close(callback?: (err?: Error | null) => void): void {
    this.emit('close');
    if (callback) callback(null);
  }

  getBuffer(): Buffer {
    return Buffer.concat(this.chunks);
  }
}

/**
 * Writes logs to an in-memory Parquet file and returns a Buffer.
 * @param logs An array of logs to store in Parquet format.
 * @returns A Buffer containing the Parquet file data.
 */
export async function writeParquetToBuffer(logs: any[]): Promise<Buffer> {
  if (!logs || logs.length === 0) {
    throw new Error('Cannot write empty logs to Parquet');
  }

  const schema = new ParquetSchema({
    userId: { type: 'UTF8' },
    agentId: { type: 'UTF8' },
    userMessage: { type: 'UTF8' },
    log: { type: 'UTF8' },
    embedding: { type: 'FLOAT', repeated: true },
    timestamp: { type: 'UTF8' },
  });

  const memoryStream = new MemoryStream();
  const writer = await ParquetWriter.openStream(schema, memoryStream);

  writer.setRowGroupSize(1000); // Optimize for batch size

  try {
    for (const log of logs) {
      await writer.appendRow(log);
    }
  } catch (error) {
    throw new Error(`Error writing Parquet data: ${error.message}`);
  } finally {
    await writer.close();
  }

  const buffer = memoryStream.getBuffer();
  if (buffer.length === 0) {
    throw new Error('Generated Parquet file is empty.');
  }

  return buffer;
}
