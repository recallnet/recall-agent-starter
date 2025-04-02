import { IAgentRuntime, Memory, Provider, State, ServiceType, elizaLogger } from '@elizaos/core';
import { RecallService } from '../services/recall.service.ts';

export const recallCotProvider: Provider = {
  get: async (
    runtime: IAgentRuntime,
    _message: Memory,
    _state?: State,
  ): Promise<Error | string> => {
    if (!process.env.RECALL_BUCKET_ALIAS) {
      elizaLogger.error('RECALL_BUCKET_ALIAS is not set');
    }
    try {
      const recallService = runtime.services.get('recall' as ServiceType) as RecallService;
      const res = await recallService.retrieveOrderedChainOfThoughtLogs(
        process.env.RECALL_BUCKET_ALIAS,
      );
      return JSON.stringify(res, null, 2);
    } catch (error) {
      return error instanceof Error ? error.message : 'Unable to get storage provider';
    }
  },
};
