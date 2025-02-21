import { Plugin } from '@elizaos/core';
import { recallCotProvider } from './providers/index.ts';
import { RecallService } from './services/recall.service.ts';
import { buyCreditAction } from './actions/buy-credit.ts';
import { getCreditBalanceAction } from './actions/get-balance.ts';
import { getAccountInfoAction } from './actions/get-account.ts';
import { listBucketsAction } from './actions/list-buckets.ts';
import { createBucketAction } from './actions/create-bucket.ts';
import { addObjectAction } from './actions/add-object.ts';
import { getObjectAction } from './actions/get-object.ts';

export const recallStoragePlugin: Plugin = {
  name: 'Recall Storage Plugin',
  description: 'Provides basic Recall storage functionality',
  actions: [
    buyCreditAction,
    getCreditBalanceAction,
    getAccountInfoAction,
    listBucketsAction,
    addObjectAction,
    getObjectAction,
    createBucketAction,
  ],
  providers: [recallCotProvider],
  services: [RecallService.getInstance()],
};

export default recallStoragePlugin;
