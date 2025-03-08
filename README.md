# Recall AI Agent Starter Kit

An Eliza starter kit that packages Recall functionality into an Eliza plugin, allowing agents to:

- ✅ Create Recall buckets
- ✅ Write objects to buckets
- ✅ Get and download objects from Recall buckets
- ✅ List buckets, account information, and credit availability
- ✅ Purchase credits
- ✅ Sync chain-of-thought log files to Recall buckets
- ✅ Use chain-of-thought historical logs in agent context to improve inference

## 📌 Overview

This plugin integrates **Recall storage** with Eliza AI agents, providing persistent memory capabilities. It enables:

1. **Chain-of-Thought Logging:** The agent logs reasoning steps into a local database.
2. **Periodic Syncing to Recall:** Logs are periodically uploaded to Recall buckets.
3. **Retrieving Thought Logs for Context:** Before each inference cycle, thought logs are retrieved and injected into the agent's state.
4. **Efficient Storage Management:** The agent can create, list, add, and retrieve objects within Recall buckets.

This starter kit also uses a modified `DirectClient` specifically built to extract chain-of-thought logs.

### **🔄 Flow of Operations**

- 1️⃣ User requests an action (e.g., "Create a bucket named 'logs'"), or simply sends a query to the agent.
- 2️⃣ The **RecallService** processes the request and interacts with the Recall API if an action has been invoked.
- 3️⃣ Chain-of-thought logs are stored and periodically synced using the modified database structure.
- 4️⃣ The **Recall Provider** fetches chain-of-thought logs before each agent loop.

## 📌 Actions

Actions define how the agent interacts with Recall. Each action is triggered based on user intent.

| **Action**        | **Trigger Format**                                                                                 | **Description**                                                                                                                                    |
| ----------------- | -------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Create Bucket** | `"Create a bucket for me named \"bucket-alias\""` `OR` `"Create a bucket called \"bucket-alias\""` | Creates a new Recall bucket (or retrieves an existing one).                                                                                        |
| **List Buckets**  | `"Get a list of my buckets"` `OR` `"Show my Recall buckets"`                                       | Retrieves a list of all available Recall buckets.                                                                                                  |
| **Add Object**    | `"Add object \"file.txt\" to bucket \"bucket-alias\""`                                             | Uploads an object (file, text, data) to a specified bucket. **Object must come first in quotes, followed by bucket name.**                         |
| **Get Object**    | `"Get object \"file.txt\" from bucket \"bucket-alias\""`                                           | Downloads an object from a specified bucket and stores in the /downloads directory. **Object must come first in quotes, followed by bucket name.** |
| **Get Account**   | `"Get my account details"` `OR` `"Retrieve my Recall account"`                                     | Fetches the agent's Recall account information.                                                                                                    |
| **Get Balance**   | `"Check my Recall credit balance"` `OR` `"How many credits do I have?"`                            | Retrieves the agent’s available Recall credits.                                                                                                    |
| **Buy Credit**    | `"Buy 3 credits"` `OR` `"Purchase 0.5 Recall credits"`                                             | Purchases additional credits for storage & usage. **Requires a numerical amount.**                                                                 |

### **🔍 Example Triggers**

> ✅ **"Get object \"data.json\" from bucket \"backup\""**  
> ✅ **"Add object \"newFile.txt\" to bucket \"storage-bucket\""**  
> ✅ **"Create a bucket for me named \"project-logs\""**  
> ✅ **"Buy 6 Recall credits"**  
> ✅ **"How many credits do I have?"**

### **🔄 Key Implementation Notes**

- **Order Matters for Add/Get Object**

  - The **object key must always be first**, followed by `"from bucket"` and then the **bucket alias**.
  - Example: `"Get object \"data.json\" from bucket \"backup\""` ✅
  - Incorrect: `"Get bucket \"backup\" and retrieve object \"data.json\""` ❌

- **Bucket Creation Auto-Validates**

  - If a **bucket with the alias already exists**, the system will **return its existing address** instead of creating a new one.

- **Buy Credit Requires Numbers**
  - `"Buy Recall credits"` → **Invalid** ❌
  - `"Buy 0.2 Recall credits"` → **Valid** ✅

---

## 📌 Providers

Providers inject **external data** into the agent’s **context** before inference. The **Recall Provider** retrieves thought logs before the agent processes user input.

```typescript
export const recallCotProvider: Provider = {
  get: async (
    _runtime: IAgentRuntime,
    message: Memory,
    _state?: State,
  ): Promise<Error | string> => {
    if (!process.env.RECALL_BUCKET_ALIAS) {
      elizaLogger.error('RECALL_BUCKET_ALIAS is not set');
    }
    try {
      const recallService = _runtime.services.get('recall' as ServiceType) as RecallService;
      const res = await recallService.retrieveOrderedChainOfThoughtLogs(
        process.env.RECALL_BUCKET_ALIAS,
      );
      return JSON.stringify(res, null, 2);
    } catch (error) {
      return error instanceof Error ? error.message : 'Unable to get storage provider';
    }
  },
};
```

### **📌 How it Works**

- **Before every agent inference cycle**, the provider **retrieves past chain-of-thought logs**.
- The logs are **appended to the agent’s context**, improving **long-term memory recall**.
- The **Recall bucket alias** is configurable via `.env`.

---

## 📌 Services

The **RecallService** manages interaction with the **Recall API**, handling:

- **Bucket Management** (`listBuckets()`, `getOrCreateBucket()`)
- **Object Storage & Retrieval** (`addObject()`, `getObject()`)
- **Credit Management** (`getCreditInfo()`, `buyCredit()`)
- **Chain-of-Thought Syncing** (`syncLogsToRecall()`)

```typescript
const recallService = new RecallService();
await recallService.getOrCreateBucket('my-bucket');
await recallService.addObject('my-bucket', 'log.txt', 'Sample log data');
const retrieved = await recallService.getObject('my-bucket', 'log.txt');
```

## 📌 Instructions

**Note:** The Recall private key you use for this application must have a corresponding registered account with a positive parent balance.

To register:

```bash
curl -X POST -H "Content-Type: application/json" -d '{"address": "<your-evm-public-address>"}' https://faucet.node-0.testnet.recall.network/register
```

To receive testnet tokens, use the same public address when requesting tokens from the [Recall Faucet](https://faucet.recall.network/).

### **1️⃣ Setup Your Environment**

```bash
cp .env.example .env
```

Fill in your Recall credentials and configuration.

```dotenv
RECALL_PRIVATE_KEY="your-private-key"
RECALL_BUCKET_ALIAS="your-default-bucket"
RECALL_COT_LOG_PREFIX="cot/"

# Provider configuration, defined in the `characters/eliza.character.json` file
OPENAI_API_KEY="your-api-key"
# For Story Protocol
STORY_PRIVATE_KEY=
PINATA_JWT=
```

Optionally, you can override the CoT sync period, batch size, and network:

```dotenv
RECALL_SYNC_INTERVAL="120000" # In milliseconds, defaults to 2 minutes
RECALL_BATCH_SIZE="4" # In kilobytes, defaults to 4KB
RECALL_NETWORK="testnet" # Defaults to the Recall testnet
```

To ensure smooth operations and reduce the possibility of dependency errors, please ensure you're using the following node and pnpm versions:

```
pnpm -v 9.15.4
node -v v22.11.0
```

### **2️⃣ Install Dependencies and Start the Server**

Back in the root of this directory, install and start the server:

```bash
pnpm i && pnpm start --characters="characters/eliza.character.json"
```

### **3️⃣ Modify Default Character**

Modify the default character in the [character file](characters/eliza.character.json).

---

## 🚀 **Start Using Recall with Eliza AI!**

This plugin ensures your **agent retains memory**, improving decision-making over time. Happy coding! 🎉
