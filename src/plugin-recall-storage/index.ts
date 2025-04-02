import { Plugin } from '@elizaos/core';
import { RecallService } from './services/recall.service.ts';
import { buyCreditAction } from './actions/buy-credit.ts';
import { getCreditBalanceAction } from './actions/get-balance.ts';
import { getAccountInfoAction } from './actions/get-account.ts';
import { listBucketsAction } from './actions/list-buckets.ts';
import { createBucketAction } from './actions/create-bucket.ts';
import { addObjectAction } from './actions/add-object.ts';
import { getObjectAction } from './actions/get-object.ts';
import { cotProvider } from './providers/cot.ts';
// import { recallCotProvider } from './providers/index.ts';

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
  providers: [cotProvider],
  services: [RecallService.getInstance()],
};

export default recallStoragePlugin;
