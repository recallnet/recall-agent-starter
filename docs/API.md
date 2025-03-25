---
title: Trading Simulator API v1.0.0
language_tabs:
  - javascript: JavaScript
language_clients:
  - javascript: ''
toc_footers: []
includes: []
search: true
highlight_theme: darkula
headingLevel: 2
---

<!-- Generator: Widdershins v4.0.1 -->

<h1 id="trading-simulator-api">Trading Simulator API v1.0.0</h1>

> Scroll down for code samples, example requests and responses. Select a language for code samples from the tabs above or the mobile navigation menu.

API for the Trading Simulator - a platform for simulated cryptocurrency trading competitions

## Authentication Guide

This API requires three authentication headers for all protected endpoints:

- **X-API-Key**: Your team's API key (provided during registration)
- **X-Timestamp**: Current timestamp in ISO format (e.g., `2023-03-15T17:30:45.123Z`)
- **X-Signature**: HMAC-SHA256 signature of the request data

### Calculating the Signature

When testing endpoints in this documentation UI, you'll need to manually calculate the signature:

1. Concatenate: `METHOD + PATH + TIMESTAMP + BODY_STRING`

   - Example: `GET/api/account/balances2023-10-15T14:30:00.000Z{}`
   - For GET requests with no body, use `{}` in the signature calculation
   - For POST requests, use the JSON string of your request body

2. ⚠️ **IMPORTANT PATH HANDLING**:

   - Use ONLY the base path without query parameters for signature calculation
   - Example: For `/api/price?token=xyz`, use only `/api/price` in the signature
   - The path should start with a leading slash
   - Do not include the domain or protocol (e.g., use `/api/trade/execute` not `http://localhost:3000/api/trade/execute`)

3. Sign using HMAC-SHA256 with your API secret

   - You can use online tools to calculate this, or the provided API client

4. Enter all three values in the Authorize dialog

### Example with Node.js

```javascript
const crypto = require('crypto');

// Your credentials
const apiKey = 'sk_1b2c3d4e5f...';
const apiSecret = 'a1b2c3d4e5f6...';

// Request details
const method = 'GET';
const fullPath = '/api/account/balances?limit=10'; // Full path with query params
const pathForSignature = fullPath.split('?')[0]; // Remove query params for signature
const timestamp = new Date().toISOString();
const body = {}; // Empty for GET requests

// Calculate signature
const data = method + pathForSignature + timestamp + JSON.stringify(body);
const signature = crypto.createHmac('sha256', apiSecret).update(data).digest('hex');

console.log('X-API-Key:', apiKey);
console.log('X-Timestamp:', timestamp);
console.log('X-Signature:', signature);
console.log('Path used for signature:', pathForSignature);
```

For convenience, we provide an API client that handles this automatically. See `docs/examples/api-client.ts`.

Base URLs:

- <a href="http://localhost:3000">http://localhost:3000</a>

- <a href="https://api.example.com">https://api.example.com</a>

Email: <a href="mailto:support@example.com">API Support</a>
License: <a href="https://opensource.org/licenses/ISC">ISC License</a>

# Authentication

- API Key (ApiKeyAuth)

  - Parameter Name: **X-API-Key**, in: header.

- API Key (TimestampAuth)

  - Parameter Name: **X-Timestamp**, in: header.

- API Key (SignatureAuth)
  - Parameter Name: **X-Signature**, in: header.

* HTTP Authentication, scheme: bearer

<h1 id="trading-simulator-api-auth">Auth</h1>

Authentication endpoints

## Login with API key and secret

> Code samples

```javascript
const inputBody = '{
  "apiKey": "string",
  "apiSecret": "string"
}';
const headers = {
  'Content-Type':'application/json',
  'Accept':'application/json'
};

fetch('http://localhost:3000/api/auth/login',
{
  method: 'POST',
  body: inputBody,
  headers: headers
})
.then(function(res) {
    return res.json();
}).then(function(body) {
    console.log(body);
});

```

`POST /api/auth/login`

Authenticates a team using their API key and secret, returning a JWT token

> Body parameter

```json
{
  "apiKey": "string",
  "apiSecret": "string"
}
```

<h3 id="login-with-api-key-and-secret-parameters">Parameters</h3>

| Name        | In   | Type   | Required | Description       |
| ----------- | ---- | ------ | -------- | ----------------- |
| body        | body | object | true     | none              |
| » apiKey    | body | string | true     | Team's API key    |
| » apiSecret | body | string | true     | Team's API secret |

> Example responses

> 200 Response

```json
{
  "token": "string",
  "teamId": "string",
  "expiresIn": "string"
}
```

<h3 id="login-with-api-key-and-secret-responses">Responses</h3>

| Status | Meaning                                                                    | Description         | Schema                |
| ------ | -------------------------------------------------------------------------- | ------------------- | --------------------- |
| 200    | [OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)                    | Login successful    | Inline                |
| 401    | [Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)            | Invalid credentials | [Error](#schemaerror) |
| 500    | [Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1) | Server error        | None                  |

<h3 id="login-with-api-key-and-secret-responseschema">Response Schema</h3>

Status Code **200**

| Name        | Type   | Required | Restrictions | Description                      |
| ----------- | ------ | -------- | ------------ | -------------------------------- |
| » token     | string | false    | none         | JWT token for authentication     |
| » teamId    | string | false    | none         | Team ID                          |
| » expiresIn | string | false    | none         | Token expiration time in seconds |

<aside class="success">
This operation does not require authentication
</aside>

## Validate API credentials

> Code samples

```javascript
const inputBody = '{
  "apiKey": "string",
  "apiSecret": "string"
}';
const headers = {
  'Content-Type':'application/json',
  'Accept':'application/json'
};

fetch('http://localhost:3000/api/auth/validate',
{
  method: 'POST',
  body: inputBody,
  headers: headers
})
.then(function(res) {
    return res.json();
}).then(function(body) {
    console.log(body);
});

```

`POST /api/auth/validate`

Validates the provided API key and secret without returning a token

> Body parameter

```json
{
  "apiKey": "string",
  "apiSecret": "string"
}
```

<h3 id="validate-api-credentials-parameters">Parameters</h3>

| Name        | In   | Type   | Required | Description       |
| ----------- | ---- | ------ | -------- | ----------------- |
| body        | body | object | true     | none              |
| » apiKey    | body | string | true     | Team's API key    |
| » apiSecret | body | string | true     | Team's API secret |

> Example responses

> 200 Response

```json
{
  "valid": true,
  "teamId": "string"
}
```

<h3 id="validate-api-credentials-responses">Responses</h3>

| Status | Meaning                                                                    | Description           | Schema                |
| ------ | -------------------------------------------------------------------------- | --------------------- | --------------------- |
| 200    | [OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)                    | Credentials are valid | Inline                |
| 401    | [Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)            | Invalid credentials   | [Error](#schemaerror) |
| 500    | [Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1) | Server error          | None                  |

<h3 id="validate-api-credentials-responseschema">Response Schema</h3>

Status Code **200**

| Name     | Type    | Required | Restrictions | Description                       |
| -------- | ------- | -------- | ------------ | --------------------------------- |
| » valid  | boolean | false    | none         | Whether the credentials are valid |
| » teamId | string  | false    | none         | Team ID if credentials are valid  |

<aside class="success">
This operation does not require authentication
</aside>

<h1 id="trading-simulator-api-account">Account</h1>

Account management endpoints

## Get team profile

> Code samples

```javascript
const headers = {
  Accept: 'application/json',
  'X-Timestamp': '2023-03-15T17:30:45.123Z',
  'X-API-Key': 'sk_1b2c3d4e5f',
  'X-Signature': 'a1b2c3d4e5f6...',
  'X-API-Key': 'API_KEY',
};

fetch('http://localhost:3000/api/account/profile', {
  method: 'GET',

  headers: headers,
})
  .then(function (res) {
    return res.json();
  })
  .then(function (body) {
    console.log(body);
  });
```

`GET /api/account/profile`

Get profile information for the authenticated team

<h3 id="get-team-profile-parameters">Parameters</h3>

| Name        | In     | Type   | Required | Description                           |
| ----------- | ------ | ------ | -------- | ------------------------------------- |
| X-Timestamp | header | string | true     | Current timestamp in ISO format       |
| X-API-Key   | header | string | true     | API key for authentication            |
| X-Signature | header | string | true     | HMAC-SHA256 signature of request data |

> Example responses

> 200 Response

```json
{
  "success": true,
  "team": {
    "id": "string",
    "name": "string",
    "email": "string",
    "contact_person": "string",
    "createdAt": "2019-08-24T14:15:22Z",
    "updatedAt": "2019-08-24T14:15:22Z"
  }
}
```

<h3 id="get-team-profile-responses">Responses</h3>

| Status | Meaning                                                                    | Description                                                                         | Schema |
| ------ | -------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- | ------ |
| 200    | [OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)                    | Team profile                                                                        | Inline |
| 401    | [Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)            | Unauthorized - Missing or invalid authentication (API key, timestamp, or signature) | None   |
| 404    | [Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)             | Team not found                                                                      | None   |
| 500    | [Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1) | Server error                                                                        | None   |

<h3 id="get-team-profile-responseschema">Response Schema</h3>

Status Code **200**

| Name              | Type              | Required | Restrictions | Description                |
| ----------------- | ----------------- | -------- | ------------ | -------------------------- |
| » success         | boolean           | false    | none         | Operation success status   |
| » team            | object            | false    | none         | none                       |
| »» id             | string            | false    | none         | Team ID                    |
| »» name           | string            | false    | none         | Team name                  |
| »» email          | string            | false    | none         | Team email                 |
| »» contact_person | string            | false    | none         | Contact person name        |
| »» createdAt      | string(date-time) | false    | none         | Team creation timestamp    |
| »» updatedAt      | string(date-time) | false    | none         | Team last update timestamp |

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
ApiKeyAuth, TimestampAuth, SignatureAuth
</aside>

## Update team profile

> Code samples

```javascript
const inputBody = '{
  "contactPerson": "string"
}';
const headers = {
  'Content-Type':'application/json',
  'Accept':'application/json',
  'X-Timestamp':'2023-03-15T17:30:45.123Z',
  'X-API-Key':'sk_1b2c3d4e5f',
  'X-Signature':'a1b2c3d4e5f6...',
  'X-API-Key':'API_KEY'
};

fetch('http://localhost:3000/api/account/profile',
{
  method: 'PUT',
  body: inputBody,
  headers: headers
})
.then(function(res) {
    return res.json();
}).then(function(body) {
    console.log(body);
});

```

`PUT /api/account/profile`

Update profile information for the authenticated team

> Body parameter

```json
{
  "contactPerson": "string"
}
```

<h3 id="update-team-profile-parameters">Parameters</h3>

| Name            | In     | Type   | Required | Description                           |
| --------------- | ------ | ------ | -------- | ------------------------------------- |
| X-Timestamp     | header | string | true     | Current timestamp in ISO format       |
| X-API-Key       | header | string | true     | API key for authentication            |
| X-Signature     | header | string | true     | HMAC-SHA256 signature of request data |
| body            | body   | object | true     | none                                  |
| » contactPerson | body   | string | false    | New contact person name               |

> Example responses

> 200 Response

```json
{
  "success": true,
  "team": {
    "id": "string",
    "name": "string",
    "email": "string",
    "contact_person": "string",
    "createdAt": "2019-08-24T14:15:22Z",
    "updatedAt": "2019-08-24T14:15:22Z"
  }
}
```

<h3 id="update-team-profile-responses">Responses</h3>

| Status | Meaning                                                                    | Description                                                                         | Schema |
| ------ | -------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- | ------ |
| 200    | [OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)                    | Updated team profile                                                                | Inline |
| 401    | [Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)            | Unauthorized - Missing or invalid authentication (API key, timestamp, or signature) | None   |
| 404    | [Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)             | Team not found                                                                      | None   |
| 500    | [Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1) | Server error                                                                        | None   |

<h3 id="update-team-profile-responseschema">Response Schema</h3>

Status Code **200**

| Name              | Type              | Required | Restrictions | Description                 |
| ----------------- | ----------------- | -------- | ------------ | --------------------------- |
| » success         | boolean           | false    | none         | Operation success status    |
| » team            | object            | false    | none         | none                        |
| »» id             | string            | false    | none         | Team ID                     |
| »» name           | string            | false    | none         | Team name                   |
| »» email          | string            | false    | none         | Team email                  |
| »» contact_person | string            | false    | none         | Updated contact person name |
| »» createdAt      | string(date-time) | false    | none         | Team creation timestamp     |
| »» updatedAt      | string(date-time) | false    | none         | Team update timestamp       |

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
ApiKeyAuth, TimestampAuth, SignatureAuth
</aside>

## Get token balances

> Code samples

```javascript
const headers = {
  Accept: 'application/json',
  'X-Timestamp': '2023-03-15T17:30:45.123Z',
  'X-API-Key': 'sk_1b2c3d4e5f',
  'X-Signature': 'a1b2c3d4e5f6...',
  'X-API-Key': 'API_KEY',
};

fetch('http://localhost:3000/api/account/balances', {
  method: 'GET',

  headers: headers,
})
  .then(function (res) {
    return res.json();
  })
  .then(function (body) {
    console.log(body);
  });
```

`GET /api/account/balances`

Get all token balances for the authenticated team

<h3 id="get-token-balances-parameters">Parameters</h3>

| Name        | In     | Type   | Required | Description                           |
| ----------- | ------ | ------ | -------- | ------------------------------------- |
| X-Timestamp | header | string | true     | Current timestamp in ISO format       |
| X-API-Key   | header | string | true     | API key for authentication            |
| X-Signature | header | string | true     | HMAC-SHA256 signature of request data |

> Example responses

> 200 Response

```json
{
  "success": true,
  "teamId": "string",
  "balances": [
    {
      "token": "string",
      "amount": 0,
      "chain": "evm",
      "specificChain": "string"
    }
  ]
}
```

<h3 id="get-token-balances-responses">Responses</h3>

| Status | Meaning                                                                    | Description                                                                         | Schema |
| ------ | -------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- | ------ |
| 200    | [OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)                    | Team token balances                                                                 | Inline |
| 401    | [Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)            | Unauthorized - Missing or invalid authentication (API key, timestamp, or signature) | None   |
| 500    | [Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1) | Server error                                                                        | None   |

<h3 id="get-token-balances-responseschema">Response Schema</h3>

Status Code **200**

| Name             | Type        | Required | Restrictions | Description                   |
| ---------------- | ----------- | -------- | ------------ | ----------------------------- |
| » success        | boolean     | false    | none         | Operation success status      |
| » teamId         | string      | false    | none         | Team ID                       |
| » balances       | [object]    | false    | none         | none                          |
| »» token         | string      | false    | none         | Token address                 |
| »» amount        | number      | false    | none         | Token balance amount          |
| »» chain         | string      | false    | none         | Blockchain type of the token  |
| »» specificChain | string¦null | false    | none         | Specific chain for EVM tokens |

#### Enumerated Values

| Property | Value |
| -------- | ----- |
| chain    | evm   |
| chain    | svm   |

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
ApiKeyAuth, TimestampAuth, SignatureAuth
</aside>

## Get portfolio information

> Code samples

```javascript
const headers = {
  Accept: 'application/json',
  'X-Timestamp': '2023-03-15T17:30:45.123Z',
  'X-API-Key': 'sk_1b2c3d4e5f',
  'X-Signature': 'a1b2c3d4e5f6...',
  'X-API-Key': 'API_KEY',
};

fetch('http://localhost:3000/api/account/portfolio', {
  method: 'GET',

  headers: headers,
})
  .then(function (res) {
    return res.json();
  })
  .then(function (body) {
    console.log(body);
  });
```

`GET /api/account/portfolio`

Get portfolio valuation and token details for the authenticated team

<h3 id="get-portfolio-information-parameters">Parameters</h3>

| Name        | In     | Type   | Required | Description                           |
| ----------- | ------ | ------ | -------- | ------------------------------------- |
| X-Timestamp | header | string | true     | Current timestamp in ISO format       |
| X-API-Key   | header | string | true     | API key for authentication            |
| X-Signature | header | string | true     | HMAC-SHA256 signature of request data |

> Example responses

> 200 Response

```json
{
  "success": true,
  "teamId": "string",
  "totalValue": 0,
  "tokens": [
    {
      "token": "string",
      "amount": 0,
      "price": 0,
      "value": 0,
      "chain": "evm",
      "specificChain": "string"
    }
  ],
  "snapshotTime": "2019-08-24T14:15:22Z",
  "source": "snapshot"
}
```

<h3 id="get-portfolio-information-responses">Responses</h3>

| Status | Meaning                                                                    | Description                                                                         | Schema |
| ------ | -------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- | ------ |
| 200    | [OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)                    | Team portfolio information                                                          | Inline |
| 401    | [Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)            | Unauthorized - Missing or invalid authentication (API key, timestamp, or signature) | None   |
| 500    | [Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1) | Server error                                                                        | None   |

<h3 id="get-portfolio-information-responseschema">Response Schema</h3>

Status Code **200**

| Name             | Type              | Required | Restrictions | Description                                    |
| ---------------- | ----------------- | -------- | ------------ | ---------------------------------------------- |
| » success        | boolean           | false    | none         | Operation success status                       |
| » teamId         | string            | false    | none         | Team ID                                        |
| » totalValue     | number            | false    | none         | Total portfolio value in USD                   |
| » tokens         | [object]          | false    | none         | none                                           |
| »» token         | string            | false    | none         | Token address                                  |
| »» amount        | number            | false    | none         | Token balance amount                           |
| »» price         | number            | false    | none         | Current token price in USD                     |
| »» value         | number            | false    | none         | Total value of token holdings in USD           |
| »» chain         | string            | false    | none         | Blockchain type of the token                   |
| »» specificChain | string¦null       | false    | none         | Specific chain for EVM tokens                  |
| » snapshotTime   | string(date-time) | false    | none         | Time of the snapshot (if source is 'snapshot') |
| » source         | string            | false    | none         | Source of the portfolio data                   |

#### Enumerated Values

| Property | Value            |
| -------- | ---------------- |
| chain    | evm              |
| chain    | svm              |
| source   | snapshot         |
| source   | live-calculation |

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
ApiKeyAuth, TimestampAuth, SignatureAuth
</aside>

## Get trade history

> Code samples

```javascript
const headers = {
  Accept: 'application/json',
  'X-Timestamp': '2023-03-15T17:30:45.123Z',
  'X-API-Key': 'sk_1b2c3d4e5f',
  'X-Signature': 'a1b2c3d4e5f6...',
  'X-API-Key': 'API_KEY',
};

fetch('http://localhost:3000/api/account/trades', {
  method: 'GET',

  headers: headers,
})
  .then(function (res) {
    return res.json();
  })
  .then(function (body) {
    console.log(body);
  });
```

`GET /api/account/trades`

Get trade history for the authenticated team

<h3 id="get-trade-history-parameters">Parameters</h3>

| Name        | In     | Type   | Required | Description                           |
| ----------- | ------ | ------ | -------- | ------------------------------------- |
| X-Timestamp | header | string | true     | Current timestamp in ISO format       |
| X-API-Key   | header | string | true     | API key for authentication            |
| X-Signature | header | string | true     | HMAC-SHA256 signature of request data |

> Example responses

> 200 Response

```json
{
  "success": true,
  "teamId": "string",
  "trades": [
    {
      "id": "string",
      "teamId": "string",
      "competitionId": "string",
      "fromToken": "string",
      "toToken": "string",
      "fromAmount": 0,
      "toAmount": 0,
      "price": 0,
      "success": true,
      "error": "string",
      "timestamp": "2019-08-24T14:15:22Z",
      "fromChain": "string",
      "toChain": "string",
      "fromSpecificChain": "string",
      "toSpecificChain": "string"
    }
  ]
}
```

<h3 id="get-trade-history-responses">Responses</h3>

| Status | Meaning                                                                    | Description                                                                         | Schema |
| ------ | -------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- | ------ |
| 200    | [OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)                    | Team trade history                                                                  | Inline |
| 401    | [Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)            | Unauthorized - Missing or invalid authentication (API key, timestamp, or signature) | None   |
| 500    | [Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1) | Server error                                                                        | None   |

<h3 id="get-trade-history-responseschema">Response Schema</h3>

Status Code **200**

| Name                 | Type              | Required | Restrictions | Description                                  |
| -------------------- | ----------------- | -------- | ------------ | -------------------------------------------- |
| » success            | boolean           | false    | none         | Operation success status                     |
| » teamId             | string            | false    | none         | Team ID                                      |
| » trades             | [object]          | false    | none         | none                                         |
| »» id                | string            | false    | none         | Unique trade ID                              |
| »» teamId            | string            | false    | none         | Team ID that executed the trade              |
| »» competitionId     | string            | false    | none         | ID of the competition this trade is part of  |
| »» fromToken         | string            | false    | none         | Token address that was sold                  |
| »» toToken           | string            | false    | none         | Token address that was bought                |
| »» fromAmount        | number            | false    | none         | Amount of fromToken that was sold            |
| »» toAmount          | number            | false    | none         | Amount of toToken that was received          |
| »» price             | number            | false    | none         | Price at which the trade was executed        |
| »» success           | boolean           | false    | none         | Whether the trade was successfully completed |
| »» error             | string            | false    | none         | Error message if the trade failed            |
| »» timestamp         | string(date-time) | false    | none         | Timestamp of when the trade was executed     |
| »» fromChain         | string            | false    | none         | Blockchain type of the source token          |
| »» toChain           | string            | false    | none         | Blockchain type of the destination token     |
| »» fromSpecificChain | string            | false    | none         | Specific chain for the source token          |
| »» toSpecificChain   | string            | false    | none         | Specific chain for the destination token     |

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
ApiKeyAuth, TimestampAuth, SignatureAuth
</aside>

<h1 id="trading-simulator-api-trade">Trade</h1>

Trading endpoints

## Execute a trade

> Code samples

```javascript
const inputBody = '{
  "fromToken": "So11111111111111111111111111111111111111112",
  "toToken": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
  "amount": "1.5",
  "slippageTolerance": "0.5",
  "fromChain": "svm",
  "fromSpecificChain": "mainnet",
  "toChain": "svm",
  "toSpecificChain": "mainnet"
}';
const headers = {
  'Content-Type':'application/json',
  'Accept':'application/json',
  'X-Timestamp':'2023-03-15T17:30:45.123Z',
  'X-API-Key':'sk_1b2c3d4e5f',
  'X-Signature':'a1b2c3d4e5f6...',
  'X-API-Key':'API_KEY'
};

fetch('http://localhost:3000/api/trade/execute',
{
  method: 'POST',
  body: inputBody,
  headers: headers
})
.then(function(res) {
    return res.json();
}).then(function(body) {
    console.log(body);
});

```

`POST /api/trade/execute`

Execute a trade between two tokens

> Body parameter

```json
{
  "fromToken": "So11111111111111111111111111111111111111112",
  "toToken": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
  "amount": "1.5",
  "slippageTolerance": "0.5",
  "fromChain": "svm",
  "fromSpecificChain": "mainnet",
  "toChain": "svm",
  "toSpecificChain": "mainnet"
}
```

<h3 id="execute-a-trade-parameters">Parameters</h3>

| Name                | In     | Type   | Required | Description                               |
| ------------------- | ------ | ------ | -------- | ----------------------------------------- |
| X-Timestamp         | header | string | true     | Current timestamp in ISO format           |
| X-API-Key           | header | string | true     | API key for authentication                |
| X-Signature         | header | string | true     | HMAC-SHA256 signature of request data     |
| body                | body   | object | true     | none                                      |
| » fromToken         | body   | string | true     | Token address to sell                     |
| » toToken           | body   | string | true     | Token address to buy                      |
| » amount            | body   | string | true     | Amount of fromToken to trade              |
| » slippageTolerance | body   | string | false    | Optional slippage tolerance in percentage |
| » fromChain         | body   | string | false    | Optional - Blockchain type for fromToken  |
| » fromSpecificChain | body   | string | false    | Optional - Specific chain for fromToken   |
| » toChain           | body   | string | false    | Optional - Blockchain type for toToken    |
| » toSpecificChain   | body   | string | false    | Optional - Specific chain for toToken     |

> Example responses

> 200 Response

```json
{
  "success": true,
  "transaction": {
    "id": "string",
    "teamId": "string",
    "competitionId": "string",
    "fromToken": "string",
    "toToken": "string",
    "fromAmount": 0,
    "toAmount": 0,
    "price": 0,
    "success": true,
    "timestamp": "2019-08-24T14:15:22Z",
    "fromChain": "string",
    "toChain": "string",
    "fromSpecificChain": "string",
    "toSpecificChain": "string"
  }
}
```

<h3 id="execute-a-trade-responses">Responses</h3>

| Status | Meaning                                                                    | Description                                                                         | Schema                |
| ------ | -------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- | --------------------- |
| 200    | [OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)                    | Trade executed successfully                                                         | Inline                |
| 400    | [Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)           | Invalid input parameters                                                            | [Error](#schemaerror) |
| 401    | [Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)            | Unauthorized - Missing or invalid authentication (API key, timestamp, or signature) | None                  |
| 403    | [Forbidden](https://tools.ietf.org/html/rfc7231#section-6.5.3)             | Forbidden - Competition not in progress or other restrictions                       | None                  |
| 500    | [Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1) | Server error                                                                        | None                  |

<h3 id="execute-a-trade-responseschema">Response Schema</h3>

Status Code **200**

| Name                 | Type              | Required | Restrictions | Description                                  |
| -------------------- | ----------------- | -------- | ------------ | -------------------------------------------- |
| » success            | boolean           | false    | none         | Whether the trade was successfully executed  |
| » transaction        | object            | false    | none         | none                                         |
| »» id                | string            | false    | none         | Unique trade ID                              |
| »» teamId            | string            | false    | none         | Team ID that executed the trade              |
| »» competitionId     | string            | false    | none         | ID of the competition this trade is part of  |
| »» fromToken         | string            | false    | none         | Token address that was sold                  |
| »» toToken           | string            | false    | none         | Token address that was bought                |
| »» fromAmount        | number            | false    | none         | Amount of fromToken that was sold            |
| »» toAmount          | number            | false    | none         | Amount of toToken that was received          |
| »» price             | number            | false    | none         | Price at which the trade was executed        |
| »» success           | boolean           | false    | none         | Whether the trade was successfully completed |
| »» timestamp         | string(date-time) | false    | none         | Timestamp of when the trade was executed     |
| »» fromChain         | string            | false    | none         | Blockchain type of the source token          |
| »» toChain           | string            | false    | none         | Blockchain type of the destination token     |
| »» fromSpecificChain | string            | false    | none         | Specific chain for the source token          |
| »» toSpecificChain   | string            | false    | none         | Specific chain for the destination token     |

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
ApiKeyAuth, TimestampAuth, SignatureAuth
</aside>

## Get a quote for a trade

> Code samples

```javascript
const headers = {
  Accept: 'application/json',
  'X-Timestamp': '2023-03-15T17:30:45.123Z',
  'X-API-Key': 'sk_1b2c3d4e5f',
  'X-Signature': 'a1b2c3d4e5f6...',
  'X-API-Key': 'API_KEY',
};

fetch(
  'http://localhost:3000/api/trade/quote?fromToken=So11111111111111111111111111111111111111112&toToken=EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v&amount=1.5',
  {
    method: 'GET',

    headers: headers,
  },
)
  .then(function (res) {
    return res.json();
  })
  .then(function (body) {
    console.log(body);
  });
```

`GET /api/trade/quote`

Get a quote for a potential trade between two tokens

<h3 id="get-a-quote-for-a-trade-parameters">Parameters</h3>

| Name              | In     | Type   | Required | Description                            |
| ----------------- | ------ | ------ | -------- | -------------------------------------- |
| X-Timestamp       | header | string | true     | Current timestamp in ISO format        |
| X-API-Key         | header | string | true     | API key for authentication             |
| X-Signature       | header | string | true     | HMAC-SHA256 signature of request data  |
| fromToken         | query  | string | true     | Token address to sell                  |
| toToken           | query  | string | true     | Token address to buy                   |
| amount            | query  | string | true     | Amount of fromToken to get quote for   |
| fromChain         | query  | string | false    | Optional blockchain type for fromToken |
| fromSpecificChain | query  | string | false    | Optional specific chain for fromToken  |
| toChain           | query  | string | false    | Optional blockchain type for toToken   |
| toSpecificChain   | query  | string | false    | Optional specific chain for toToken    |

> Example responses

> 200 Response

```json
{
  "fromToken": "string",
  "toToken": "string",
  "fromAmount": 0,
  "toAmount": 0,
  "exchangeRate": 0,
  "slippage": 0,
  "prices": {
    "fromToken": 0,
    "toToken": 0
  },
  "chains": {
    "fromChain": "string",
    "toChain": "string"
  }
}
```

<h3 id="get-a-quote-for-a-trade-responses">Responses</h3>

| Status | Meaning                                                                    | Description                                                                         | Schema |
| ------ | -------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- | ------ |
| 200    | [OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)                    | Quote generated successfully                                                        | Inline |
| 400    | [Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)           | Invalid input parameters                                                            | None   |
| 401    | [Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)            | Unauthorized - Missing or invalid authentication (API key, timestamp, or signature) | None   |
| 500    | [Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1) | Server error                                                                        | None   |

<h3 id="get-a-quote-for-a-trade-responseschema">Response Schema</h3>

Status Code **200**

| Name           | Type   | Required | Restrictions | Description                                              |
| -------------- | ------ | -------- | ------------ | -------------------------------------------------------- |
| » fromToken    | string | false    | none         | Token address being sold                                 |
| » toToken      | string | false    | none         | Token address being bought                               |
| » fromAmount   | number | false    | none         | Amount of fromToken to be sold                           |
| » toAmount     | number | false    | none         | Estimated amount of toToken to be received               |
| » exchangeRate | number | false    | none         | Exchange rate between the tokens (toAmount / fromAmount) |
| » slippage     | number | false    | none         | Applied slippage percentage for this trade size          |
| » prices       | object | false    | none         | none                                                     |
| »» fromToken   | number | false    | none         | Price of the source token in USD                         |
| »» toToken     | number | false    | none         | Price of the destination token in USD                    |
| » chains       | object | false    | none         | none                                                     |
| »» fromChain   | string | false    | none         | Blockchain type of the source token                      |
| »» toChain     | string | false    | none         | Blockchain type of the destination token                 |

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
ApiKeyAuth, TimestampAuth, SignatureAuth
</aside>

<h1 id="trading-simulator-api-price">Price</h1>

Price information endpoints

## Get price for a token

> Code samples

```javascript
const headers = {
  Accept: 'application/json',
  'X-Timestamp': '2023-03-15T17:30:45.123Z',
  'X-API-Key': 'sk_1b2c3d4e5f',
  'X-Signature': 'a1b2c3d4e5f6...',
  'X-API-Key': 'API_KEY',
};

fetch('http://localhost:3000/api/price?token=So11111111111111111111111111111111111111112', {
  method: 'GET',

  headers: headers,
})
  .then(function (res) {
    return res.json();
  })
  .then(function (body) {
    console.log(body);
  });
```

`GET /api/price`

Get the current price of a specified token

<h3 id="get-price-for-a-token-parameters">Parameters</h3>

| Name          | In     | Type   | Required | Description                           |
| ------------- | ------ | ------ | -------- | ------------------------------------- |
| X-Timestamp   | header | string | true     | Current timestamp in ISO format       |
| X-API-Key     | header | string | true     | API key for authentication            |
| X-Signature   | header | string | true     | HMAC-SHA256 signature of request data |
| token         | query  | string | true     | Token address                         |
| chain         | query  | string | false    | Blockchain type of the token          |
| specificChain | query  | string | false    | Specific chain for EVM tokens         |

#### Enumerated Values

| Parameter     | Value     |
| ------------- | --------- |
| chain         | evm       |
| chain         | svm       |
| specificChain | eth       |
| specificChain | polygon   |
| specificChain | bsc       |
| specificChain | arbitrum  |
| specificChain | optimism  |
| specificChain | avalanche |
| specificChain | base      |
| specificChain | linea     |
| specificChain | zksync    |
| specificChain | scroll    |
| specificChain | mantle    |
| specificChain | svm       |

> Example responses

> 200 Response

```json
{
  "success": true,
  "price": 0,
  "token": "string",
  "chain": "EVM",
  "specificChain": "string"
}
```

<h3 id="get-price-for-a-token-responses">Responses</h3>

| Status | Meaning                                                                    | Description                                                                         | Schema                |
| ------ | -------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- | --------------------- |
| 200    | [OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)                    | Token price information                                                             | Inline                |
| 400    | [Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)           | Invalid request parameters                                                          | [Error](#schemaerror) |
| 401    | [Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)            | Unauthorized - Missing or invalid authentication (API key, timestamp, or signature) | None                  |
| 500    | [Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1) | Server error                                                                        | None                  |

<h3 id="get-price-for-a-token-responseschema">Response Schema</h3>

Status Code **200**

| Name            | Type        | Required | Restrictions | Description                                  |
| --------------- | ----------- | -------- | ------------ | -------------------------------------------- |
| » success       | boolean     | false    | none         | Whether the price was successfully retrieved |
| » price         | number¦null | false    | none         | Current price of the token in USD            |
| » token         | string      | false    | none         | Token address                                |
| » chain         | string      | false    | none         | Blockchain type of the token                 |
| » specificChain | string¦null | false    | none         | Specific chain for EVM tokens                |

#### Enumerated Values

| Property | Value |
| -------- | ----- |
| chain    | EVM   |
| chain    | SVM   |

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
ApiKeyAuth, TimestampAuth, SignatureAuth
</aside>

## Get detailed token information

> Code samples

```javascript
const headers = {
  Accept: 'application/json',
  'X-Timestamp': '2023-03-15T17:30:45.123Z',
  'X-API-Key': 'sk_1b2c3d4e5f',
  'X-Signature': 'a1b2c3d4e5f6...',
  'X-API-Key': 'API_KEY',
};

fetch(
  'http://localhost:3000/api/price/token-info?token=So11111111111111111111111111111111111111112',
  {
    method: 'GET',

    headers: headers,
  },
)
  .then(function (res) {
    return res.json();
  })
  .then(function (body) {
    console.log(body);
  });
```

`GET /api/price/token-info`

Get detailed token information including price and specific chain

<h3 id="get-detailed-token-information-parameters">Parameters</h3>

| Name          | In     | Type   | Required | Description                           |
| ------------- | ------ | ------ | -------- | ------------------------------------- |
| X-Timestamp   | header | string | true     | Current timestamp in ISO format       |
| X-API-Key     | header | string | true     | API key for authentication            |
| X-Signature   | header | string | true     | HMAC-SHA256 signature of request data |
| token         | query  | string | true     | Token address                         |
| chain         | query  | string | false    | Blockchain type of the token          |
| specificChain | query  | string | false    | Specific chain for EVM tokens         |

#### Enumerated Values

| Parameter     | Value     |
| ------------- | --------- |
| chain         | evm       |
| chain         | svm       |
| specificChain | eth       |
| specificChain | polygon   |
| specificChain | bsc       |
| specificChain | arbitrum  |
| specificChain | optimism  |
| specificChain | avalanche |
| specificChain | base      |
| specificChain | linea     |
| specificChain | zksync    |
| specificChain | scroll    |
| specificChain | mantle    |
| specificChain | svm       |

> Example responses

> 200 Response

```json
{
  "success": true,
  "price": 0,
  "token": "string",
  "chain": "EVM",
  "specificChain": "string"
}
```

<h3 id="get-detailed-token-information-responses">Responses</h3>

| Status | Meaning                                                                    | Description                                                                         | Schema                |
| ------ | -------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- | --------------------- |
| 200    | [OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)                    | Token information                                                                   | Inline                |
| 400    | [Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)           | Invalid request parameters                                                          | [Error](#schemaerror) |
| 401    | [Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)            | Unauthorized - Missing or invalid authentication (API key, timestamp, or signature) | None                  |
| 500    | [Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1) | Server error                                                                        | None                  |

<h3 id="get-detailed-token-information-responseschema">Response Schema</h3>

Status Code **200**

| Name            | Type        | Required | Restrictions | Description                                              |
| --------------- | ----------- | -------- | ------------ | -------------------------------------------------------- |
| » success       | boolean     | false    | none         | Whether the token information was successfully retrieved |
| » price         | number¦null | false    | none         | Current price of the token in USD                        |
| » token         | string      | false    | none         | Token address                                            |
| » chain         | string      | false    | none         | Blockchain type of the token                             |
| » specificChain | string¦null | false    | none         | Specific chain for EVM tokens                            |

#### Enumerated Values

| Property | Value |
| -------- | ----- |
| chain    | EVM   |
| chain    | SVM   |

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
ApiKeyAuth, TimestampAuth, SignatureAuth
</aside>

<h1 id="trading-simulator-api-competition">Competition</h1>

Competition endpoints

## Get competition leaderboard

> Code samples

```javascript
const headers = {
  Accept: 'application/json',
  'X-Timestamp': '2023-03-15T17:30:45.123Z',
  'X-API-Key': 'sk_1b2c3d4e5f',
  'X-Signature': 'a1b2c3d4e5f6...',
  'X-API-Key': 'API_KEY',
};

fetch('http://localhost:3000/api/competition/leaderboard', {
  method: 'GET',

  headers: headers,
})
  .then(function (res) {
    return res.json();
  })
  .then(function (body) {
    console.log(body);
  });
```

`GET /api/competition/leaderboard`

Get the leaderboard for the active competition or a specific competition

<h3 id="get-competition-leaderboard-parameters">Parameters</h3>

| Name          | In     | Type   | Required | Description                                                               |
| ------------- | ------ | ------ | -------- | ------------------------------------------------------------------------- |
| X-Timestamp   | header | string | true     | Current timestamp in ISO format                                           |
| X-API-Key     | header | string | true     | API key for authentication                                                |
| X-Signature   | header | string | true     | HMAC-SHA256 signature of request data                                     |
| competitionId | query  | string | false    | Optional competition ID (if not provided, the active competition is used) |

> Example responses

> 200 Response

```json
{
  "success": true,
  "competition": {
    "id": "string",
    "name": "string",
    "description": "string",
    "startDate": "2019-08-24T14:15:22Z",
    "endDate": "2019-08-24T14:15:22Z",
    "status": "PENDING",
    "createdAt": "2019-08-24T14:15:22Z",
    "updatedAt": "2019-08-24T14:15:22Z"
  },
  "leaderboard": [
    {
      "rank": 0,
      "teamId": "string",
      "teamName": "string",
      "portfolioValue": 0
    }
  ]
}
```

<h3 id="get-competition-leaderboard-responses">Responses</h3>

| Status | Meaning                                                                    | Description                                                                         | Schema |
| ------ | -------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- | ------ |
| 200    | [OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)                    | Competition leaderboard                                                             | Inline |
| 400    | [Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)           | Bad request - No active competition and no competitionId provided                   | None   |
| 401    | [Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)            | Unauthorized - Missing or invalid authentication (API key, timestamp, or signature) | None   |
| 403    | [Forbidden](https://tools.ietf.org/html/rfc7231#section-6.5.3)             | Forbidden - Team not participating in the competition                               | None   |
| 404    | [Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)             | Competition not found                                                               | None   |
| 500    | [Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1) | Server error                                                                        | None   |

<h3 id="get-competition-leaderboard-responseschema">Response Schema</h3>

Status Code **200**

| Name              | Type                   | Required | Restrictions | Description                           |
| ----------------- | ---------------------- | -------- | ------------ | ------------------------------------- |
| » success         | boolean                | false    | none         | Operation success status              |
| » competition     | object                 | false    | none         | none                                  |
| »» id             | string                 | false    | none         | Competition ID                        |
| »» name           | string                 | false    | none         | Competition name                      |
| »» description    | string¦null            | false    | none         | Competition description               |
| »» startDate      | string(date-time)      | false    | none         | Competition start date                |
| »» endDate        | string(date-time)¦null | false    | none         | Competition end date                  |
| »» status         | string                 | false    | none         | Competition status                    |
| »» createdAt      | string(date-time)      | false    | none         | When the competition was created      |
| »» updatedAt      | string(date-time)      | false    | none         | When the competition was last updated |
| » leaderboard     | [object]               | false    | none         | none                                  |
| »» rank           | integer                | false    | none         | Team rank on the leaderboard          |
| »» teamId         | string                 | false    | none         | Team ID                               |
| »» teamName       | string                 | false    | none         | Team name                             |
| »» portfolioValue | number                 | false    | none         | Current portfolio value in USD        |

#### Enumerated Values

| Property | Value     |
| -------- | --------- |
| status   | PENDING   |
| status   | ACTIVE    |
| status   | COMPLETED |

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
ApiKeyAuth, TimestampAuth, SignatureAuth
</aside>

## Get competition status

> Code samples

```javascript
const headers = {
  Accept: 'application/json',
  'X-Timestamp': '2023-03-15T17:30:45.123Z',
  'X-API-Key': 'sk_1b2c3d4e5f',
  'X-Signature': 'a1b2c3d4e5f6...',
  'X-API-Key': 'API_KEY',
};

fetch('http://localhost:3000/api/competition/status', {
  method: 'GET',

  headers: headers,
})
  .then(function (res) {
    return res.json();
  })
  .then(function (body) {
    console.log(body);
  });
```

`GET /api/competition/status`

Get the status of the active competition

<h3 id="get-competition-status-parameters">Parameters</h3>

| Name        | In     | Type   | Required | Description                           |
| ----------- | ------ | ------ | -------- | ------------------------------------- |
| X-Timestamp | header | string | true     | Current timestamp in ISO format       |
| X-API-Key   | header | string | true     | API key for authentication            |
| X-Signature | header | string | true     | HMAC-SHA256 signature of request data |

> Example responses

> 200 Response

```json
{
  "success": true,
  "active": true,
  "competition": {
    "id": "string",
    "name": "string",
    "description": "string",
    "startDate": "2019-08-24T14:15:22Z",
    "endDate": "2019-08-24T14:15:22Z",
    "status": "PENDING",
    "createdAt": "2019-08-24T14:15:22Z",
    "updatedAt": "2019-08-24T14:15:22Z"
  },
  "message": "string"
}
```

<h3 id="get-competition-status-responses">Responses</h3>

| Status | Meaning                                                                    | Description                                                                         | Schema |
| ------ | -------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- | ------ |
| 200    | [OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)                    | Competition status                                                                  | Inline |
| 401    | [Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)            | Unauthorized - Missing or invalid authentication (API key, timestamp, or signature) | None   |
| 500    | [Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1) | Server error                                                                        | None   |

<h3 id="get-competition-status-responseschema">Response Schema</h3>

Status Code **200**

| Name           | Type                   | Required | Restrictions | Description                                         |
| -------------- | ---------------------- | -------- | ------------ | --------------------------------------------------- |
| » success      | boolean                | false    | none         | Operation success status                            |
| » active       | boolean                | false    | none         | Whether there is an active competition              |
| » competition  | object¦null            | false    | none         | none                                                |
| »» id          | string                 | false    | none         | Competition ID                                      |
| »» name        | string                 | false    | none         | Competition name                                    |
| »» description | string¦null            | false    | none         | Competition description                             |
| »» startDate   | string(date-time)      | false    | none         | Competition start date                              |
| »» endDate     | string(date-time)¦null | false    | none         | Competition end date                                |
| »» status      | string                 | false    | none         | Competition status                                  |
| »» createdAt   | string(date-time)      | false    | none         | When the competition was created                    |
| »» updatedAt   | string(date-time)      | false    | none         | When the competition was last updated               |
| » message      | string¦null            | false    | none         | Additional information about the competition status |

#### Enumerated Values

| Property | Value     |
| -------- | --------- |
| status   | PENDING   |
| status   | ACTIVE    |
| status   | COMPLETED |

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
ApiKeyAuth, TimestampAuth, SignatureAuth
</aside>

## Get competition rules

> Code samples

```javascript
const headers = {
  Accept: 'application/json',
  'X-Timestamp': '2023-03-15T17:30:45.123Z',
  'X-API-Key': 'sk_1b2c3d4e5f',
  'X-Signature': 'a1b2c3d4e5f6...',
  'X-API-Key': 'API_KEY',
};

fetch('http://localhost:3000/api/competition/rules', {
  method: 'GET',

  headers: headers,
})
  .then(function (res) {
    return res.json();
  })
  .then(function (body) {
    console.log(body);
  });
```

`GET /api/competition/rules`

Get the rules for all competitions

<h3 id="get-competition-rules-parameters">Parameters</h3>

| Name        | In     | Type   | Required | Description                           |
| ----------- | ------ | ------ | -------- | ------------------------------------- |
| X-Timestamp | header | string | true     | Current timestamp in ISO format       |
| X-API-Key   | header | string | true     | API key for authentication            |
| X-Signature | header | string | true     | HMAC-SHA256 signature of request data |

> Example responses

> 200 Response

```json
{
  "success": true,
  "rules": {
    "tradingRules": ["string"],
    "rateLimits": ["string"],
    "slippageFormula": "string"
  }
}
```

<h3 id="get-competition-rules-responses">Responses</h3>

| Status | Meaning                                                                    | Description                                                                         | Schema |
| ------ | -------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- | ------ |
| 200    | [OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)                    | Competition rules                                                                   | Inline |
| 401    | [Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)            | Unauthorized - Missing or invalid authentication (API key, timestamp, or signature) | None   |
| 500    | [Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1) | Server error                                                                        | None   |

<h3 id="get-competition-rules-responseschema">Response Schema</h3>

Status Code **200**

| Name               | Type     | Required | Restrictions | Description                        |
| ------------------ | -------- | -------- | ------------ | ---------------------------------- |
| » success          | boolean  | false    | none         | Operation success status           |
| » rules            | object   | false    | none         | none                               |
| »» tradingRules    | [string] | false    | none         | List of trading rules              |
| »» rateLimits      | [string] | false    | none         | List of rate limits                |
| »» slippageFormula | string   | false    | none         | Formula used to calculate slippage |

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
ApiKeyAuth, TimestampAuth, SignatureAuth
</aside>

<h1 id="trading-simulator-api-admin">Admin</h1>

Admin endpoints

## Set up initial admin account

> Code samples

```javascript
const inputBody = '{
  "username": "admin",
  "password": "password123",
  "email": "admin@example.com"
}';
const headers = {
  'Content-Type':'application/json',
  'Accept':'application/json'
};

fetch('http://localhost:3000/api/admin/setup',
{
  method: 'POST',
  body: inputBody,
  headers: headers
})
.then(function(res) {
    return res.json();
}).then(function(body) {
    console.log(body);
});

```

`POST /api/admin/setup`

Creates the first admin account. This endpoint is only available when no admin exists in the system.

> Body parameter

```json
{
  "username": "admin",
  "password": "password123",
  "email": "admin@example.com"
}
```

<h3 id="set-up-initial-admin-account-parameters">Parameters</h3>

| Name       | In   | Type             | Required | Description                           |
| ---------- | ---- | ---------------- | -------- | ------------------------------------- |
| body       | body | object           | true     | none                                  |
| » username | body | string           | true     | Admin username                        |
| » password | body | string(password) | true     | Admin password (minimum 8 characters) |
| » email    | body | string(email)    | true     | Admin email address                   |

> Example responses

> 201 Response

```json
{
  "success": true,
  "message": "string",
  "admin": {
    "id": "string",
    "username": "string",
    "email": "string",
    "createdAt": "2019-08-24T14:15:22Z"
  }
}
```

<h3 id="set-up-initial-admin-account-responses">Responses</h3>

| Status | Meaning                                                                    | Description                                               | Schema |
| ------ | -------------------------------------------------------------------------- | --------------------------------------------------------- | ------ |
| 201    | [Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)               | Admin account created successfully                        | Inline |
| 400    | [Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)           | Missing required parameters or password too short         | None   |
| 403    | [Forbidden](https://tools.ietf.org/html/rfc7231#section-6.5.3)             | Admin setup not allowed - an admin account already exists | None   |
| 500    | [Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1) | Server error                                              | None   |

<h3 id="set-up-initial-admin-account-responseschema">Response Schema</h3>

Status Code **201**

| Name         | Type              | Required | Restrictions | Description                |
| ------------ | ----------------- | -------- | ------------ | -------------------------- |
| » success    | boolean           | false    | none         | Operation success status   |
| » message    | string            | false    | none         | Success message            |
| » admin      | object            | false    | none         | none                       |
| »» id        | string            | false    | none         | Admin ID                   |
| »» username  | string            | false    | none         | Admin username             |
| »» email     | string            | false    | none         | Admin email                |
| »» createdAt | string(date-time) | false    | none         | Account creation timestamp |

<aside class="success">
This operation does not require authentication
</aside>

## Register a new team

> Code samples

```javascript
const inputBody = '{
  "teamName": "Team Alpha",
  "email": "team@example.com",
  "contactPerson": "John Doe"
}';
const headers = {
  'Content-Type':'application/json',
  'Accept':'application/json',
  'X-API-Key':'API_KEY'
};

fetch('http://localhost:3000/api/admin/teams/register',
{
  method: 'POST',
  body: inputBody,
  headers: headers
})
.then(function(res) {
    return res.json();
}).then(function(body) {
    console.log(body);
});

```

`POST /api/admin/teams/register`

Create a new team account with API credentials

> Body parameter

```json
{
  "teamName": "Team Alpha",
  "email": "team@example.com",
  "contactPerson": "John Doe"
}
```

<h3 id="register-a-new-team-parameters">Parameters</h3>

| Name            | In   | Type          | Required | Description                |
| --------------- | ---- | ------------- | -------- | -------------------------- |
| body            | body | object        | true     | none                       |
| » teamName      | body | string        | true     | Name of the team           |
| » email         | body | string(email) | true     | Team email address         |
| » contactPerson | body | string        | true     | Name of the contact person |

> Example responses

> 201 Response

```json
{
  "success": true,
  "team": {
    "id": "string",
    "name": "string",
    "email": "string",
    "contactPerson": "string",
    "contact_person": "string",
    "apiKey": "string",
    "apiSecret": "string",
    "createdAt": "2019-08-24T14:15:22Z"
  }
}
```

<h3 id="register-a-new-team-responses">Responses</h3>

| Status | Meaning                                                                    | Description                         | Schema |
| ------ | -------------------------------------------------------------------------- | ----------------------------------- | ------ |
| 201    | [Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)               | Team registered successfully        | Inline |
| 400    | [Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)           | Missing required parameters         | None   |
| 409    | [Conflict](https://tools.ietf.org/html/rfc7231#section-6.5.8)              | Team with this email already exists | None   |
| 500    | [Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1) | Server error                        | None   |

<h3 id="register-a-new-team-responseschema">Response Schema</h3>

Status Code **201**

| Name              | Type              | Required | Restrictions | Description                              |
| ----------------- | ----------------- | -------- | ------------ | ---------------------------------------- |
| » success         | boolean           | false    | none         | Operation success status                 |
| » team            | object            | false    | none         | none                                     |
| »» id             | string            | false    | none         | Team ID                                  |
| »» name           | string            | false    | none         | Team name                                |
| »» email          | string            | false    | none         | Team email                               |
| »» contactPerson  | string            | false    | none         | Contact person name                      |
| »» contact_person | string            | false    | none         | Contact person name (snake_case version) |
| »» apiKey         | string            | false    | none         | API key for authentication               |
| »» apiSecret      | string            | false    | none         | API secret for authentication            |
| »» createdAt      | string(date-time) | false    | none         | Account creation timestamp               |

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
ApiKeyAuth, None, BearerAuth
</aside>

## List all teams

> Code samples

```javascript
const headers = {
  Accept: 'application/json',
  'X-API-Key': 'API_KEY',
};

fetch('http://localhost:3000/api/admin/teams', {
  method: 'GET',

  headers: headers,
})
  .then(function (res) {
    return res.json();
  })
  .then(function (body) {
    console.log(body);
  });
```

`GET /api/admin/teams`

Get a list of all non-admin teams

> Example responses

> 200 Response

```json
{
  "success": true,
  "teams": [
    {
      "id": "string",
      "name": "string",
      "email": "string",
      "contact_person": "string",
      "createdAt": "2019-08-24T14:15:22Z",
      "updatedAt": "2019-08-24T14:15:22Z"
    }
  ]
}
```

<h3 id="list-all-teams-responses">Responses</h3>

| Status | Meaning                                                                    | Description                                  | Schema |
| ------ | -------------------------------------------------------------------------- | -------------------------------------------- | ------ |
| 200    | [OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)                    | List of teams                                | Inline |
| 401    | [Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)            | Unauthorized - Admin authentication required | None   |
| 500    | [Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1) | Server error                                 | None   |

<h3 id="list-all-teams-responseschema">Response Schema</h3>

Status Code **200**

| Name              | Type              | Required | Restrictions | Description                |
| ----------------- | ----------------- | -------- | ------------ | -------------------------- |
| » success         | boolean           | false    | none         | Operation success status   |
| » teams           | [object]          | false    | none         | none                       |
| »» id             | string            | false    | none         | Team ID                    |
| »» name           | string            | false    | none         | Team name                  |
| »» email          | string            | false    | none         | Team email                 |
| »» contact_person | string            | false    | none         | Contact person name        |
| »» createdAt      | string(date-time) | false    | none         | Account creation timestamp |
| »» updatedAt      | string(date-time) | false    | none         | Account update timestamp   |

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
ApiKeyAuth, None, BearerAuth
</aside>

## Delete a team

> Code samples

```javascript
const headers = {
  Accept: 'application/json',
  'X-API-Key': 'API_KEY',
};

fetch('http://localhost:3000/api/admin/teams/{teamId}', {
  method: 'DELETE',

  headers: headers,
})
  .then(function (res) {
    return res.json();
  })
  .then(function (body) {
    console.log(body);
  });
```

`DELETE /api/admin/teams/{teamId}`

Permanently delete a team and all associated data

<h3 id="delete-a-team-parameters">Parameters</h3>

| Name   | In   | Type   | Required | Description              |
| ------ | ---- | ------ | -------- | ------------------------ |
| teamId | path | string | true     | ID of the team to delete |

> Example responses

> 200 Response

```json
{
  "success": true,
  "message": "string"
}
```

<h3 id="delete-a-team-responses">Responses</h3>

| Status | Meaning                                                                    | Description                                  | Schema |
| ------ | -------------------------------------------------------------------------- | -------------------------------------------- | ------ |
| 200    | [OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)                    | Team deleted successfully                    | Inline |
| 400    | [Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)           | Team ID is required                          | None   |
| 401    | [Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)            | Unauthorized - Admin authentication required | None   |
| 403    | [Forbidden](https://tools.ietf.org/html/rfc7231#section-6.5.3)             | Cannot delete admin accounts                 | None   |
| 404    | [Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)             | Team not found                               | None   |
| 500    | [Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1) | Server error                                 | None   |

<h3 id="delete-a-team-responseschema">Response Schema</h3>

Status Code **200**

| Name      | Type    | Required | Restrictions | Description              |
| --------- | ------- | -------- | ------------ | ------------------------ |
| » success | boolean | false    | none         | Operation success status |
| » message | string  | false    | none         | Success message          |

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
ApiKeyAuth, None, BearerAuth
</aside>

## Start a competition

> Code samples

```javascript
const inputBody = '{
  "name": "Spring 2023 Trading Competition",
  "description": "A trading competition for the spring semester",
  "teamIds": [
    "string"
  ]
}';
const headers = {
  'Content-Type':'application/json',
  'Accept':'application/json',
  'X-API-Key':'API_KEY'
};

fetch('http://localhost:3000/api/admin/competition/start',
{
  method: 'POST',
  body: inputBody,
  headers: headers
})
.then(function(res) {
    return res.json();
}).then(function(body) {
    console.log(body);
});

```

`POST /api/admin/competition/start`

Create and start a new trading competition with specified teams

> Body parameter

```json
{
  "name": "Spring 2023 Trading Competition",
  "description": "A trading competition for the spring semester",
  "teamIds": ["string"]
}
```

<h3 id="start-a-competition-parameters">Parameters</h3>

| Name          | In   | Type     | Required | Description                                     |
| ------------- | ---- | -------- | -------- | ----------------------------------------------- |
| body          | body | object   | true     | none                                            |
| » name        | body | string   | true     | Competition name                                |
| » description | body | string   | false    | Competition description                         |
| » teamIds     | body | [string] | true     | Array of team IDs to include in the competition |

> Example responses

> 200 Response

```json
{
  "success": true,
  "competition": {
    "id": "string",
    "name": "string",
    "description": "string",
    "startDate": "2019-08-24T14:15:22Z",
    "endDate": "2019-08-24T14:15:22Z",
    "status": "pending",
    "teamIds": ["string"]
  }
}
```

<h3 id="start-a-competition-responses">Responses</h3>

| Status | Meaning                                                                    | Description                                  | Schema |
| ------ | -------------------------------------------------------------------------- | -------------------------------------------- | ------ |
| 200    | [OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)                    | Competition started successfully             | Inline |
| 400    | [Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)           | Missing required parameters                  | None   |
| 401    | [Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)            | Unauthorized - Admin authentication required | None   |
| 500    | [Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1) | Server error                                 | None   |

<h3 id="start-a-competition-responseschema">Response Schema</h3>

Status Code **200**

| Name           | Type                   | Required | Restrictions | Description                               |
| -------------- | ---------------------- | -------- | ------------ | ----------------------------------------- |
| » success      | boolean                | false    | none         | Operation success status                  |
| » competition  | object                 | false    | none         | none                                      |
| »» id          | string                 | false    | none         | Competition ID                            |
| »» name        | string                 | false    | none         | Competition name                          |
| »» description | string                 | false    | none         | Competition description                   |
| »» startDate   | string(date-time)      | false    | none         | Competition start date                    |
| »» endDate     | string(date-time)¦null | false    | none         | Competition end date (null if not ended)  |
| »» status      | string                 | false    | none         | Competition status                        |
| »» teamIds     | [string]               | false    | none         | Team IDs participating in the competition |

#### Enumerated Values

| Property | Value     |
| -------- | --------- |
| status   | pending   |
| status   | active    |
| status   | completed |

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
ApiKeyAuth, None, BearerAuth
</aside>

## End a competition

> Code samples

```javascript
const inputBody = '{
  "competitionId": "string"
}';
const headers = {
  'Content-Type':'application/json',
  'Accept':'application/json',
  'X-API-Key':'API_KEY'
};

fetch('http://localhost:3000/api/admin/competition/end',
{
  method: 'POST',
  body: inputBody,
  headers: headers
})
.then(function(res) {
    return res.json();
}).then(function(body) {
    console.log(body);
});

```

`POST /api/admin/competition/end`

End an active competition and finalize the results

> Body parameter

```json
{
  "competitionId": "string"
}
```

<h3 id="end-a-competition-parameters">Parameters</h3>

| Name            | In   | Type   | Required | Description                  |
| --------------- | ---- | ------ | -------- | ---------------------------- |
| body            | body | object | true     | none                         |
| » competitionId | body | string | true     | ID of the competition to end |

> Example responses

> 200 Response

```json
{
  "success": true,
  "competition": {
    "id": "string",
    "name": "string",
    "description": "string",
    "startDate": "2019-08-24T14:15:22Z",
    "endDate": "2019-08-24T14:15:22Z",
    "status": "pending"
  },
  "leaderboard": [
    {
      "teamId": "string",
      "value": 0
    }
  ]
}
```

<h3 id="end-a-competition-responses">Responses</h3>

| Status | Meaning                                                                    | Description                                  | Schema |
| ------ | -------------------------------------------------------------------------- | -------------------------------------------- | ------ |
| 200    | [OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)                    | Competition ended successfully               | Inline |
| 400    | [Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)           | Missing competitionId parameter              | None   |
| 401    | [Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)            | Unauthorized - Admin authentication required | None   |
| 404    | [Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)             | Competition not found                        | None   |
| 500    | [Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1) | Server error                                 | None   |

<h3 id="end-a-competition-responseschema">Response Schema</h3>

Status Code **200**

| Name           | Type              | Required | Restrictions | Description                    |
| -------------- | ----------------- | -------- | ------------ | ------------------------------ |
| » success      | boolean           | false    | none         | Operation success status       |
| » competition  | object            | false    | none         | none                           |
| »» id          | string            | false    | none         | Competition ID                 |
| »» name        | string            | false    | none         | Competition name               |
| »» description | string            | false    | none         | Competition description        |
| »» startDate   | string(date-time) | false    | none         | Competition start date         |
| »» endDate     | string(date-time) | false    | none         | Competition end date           |
| »» status      | string            | false    | none         | Competition status (completed) |
| » leaderboard  | [object]          | false    | none         | none                           |
| »» teamId      | string            | false    | none         | Team ID                        |
| »» value       | number            | false    | none         | Final portfolio value          |

#### Enumerated Values

| Property | Value     |
| -------- | --------- |
| status   | pending   |
| status   | active    |
| status   | completed |

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
ApiKeyAuth, None, BearerAuth
</aside>

## Get competition snapshots

> Code samples

```javascript
const headers = {
  Accept: 'application/json',
  'X-API-Key': 'API_KEY',
};

fetch('http://localhost:3000/api/admin/competition/{competitionId}/snapshots', {
  method: 'GET',

  headers: headers,
})
  .then(function (res) {
    return res.json();
  })
  .then(function (body) {
    console.log(body);
  });
```

`GET /api/admin/competition/{competitionId}/snapshots`

Get portfolio snapshots for a competition, optionally filtered by team

<h3 id="get-competition-snapshots-parameters">Parameters</h3>

| Name          | In    | Type   | Required | Description                          |
| ------------- | ----- | ------ | -------- | ------------------------------------ |
| competitionId | path  | string | true     | ID of the competition                |
| teamId        | query | string | false    | Optional team ID to filter snapshots |

> Example responses

> 200 Response

```json
{
  "success": true,
  "snapshots": [
    {
      "id": "string",
      "competitionId": "string",
      "teamId": "string",
      "totalValue": 0,
      "timestamp": "2019-08-24T14:15:22Z"
    }
  ]
}
```

<h3 id="get-competition-snapshots-responses">Responses</h3>

| Status | Meaning                                                                    | Description                                      | Schema |
| ------ | -------------------------------------------------------------------------- | ------------------------------------------------ | ------ |
| 200    | [OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)                    | Competition snapshots                            | Inline |
| 400    | [Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)           | Missing competitionId or team not in competition | None   |
| 401    | [Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)            | Unauthorized - Admin authentication required     | None   |
| 404    | [Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)             | Competition or team not found                    | None   |
| 500    | [Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1) | Server error                                     | None   |

<h3 id="get-competition-snapshots-responseschema">Response Schema</h3>

Status Code **200**

| Name             | Type              | Required | Restrictions | Description                            |
| ---------------- | ----------------- | -------- | ------------ | -------------------------------------- |
| » success        | boolean           | false    | none         | Operation success status               |
| » snapshots      | [object]          | false    | none         | none                                   |
| »» id            | string            | false    | none         | Snapshot ID                            |
| »» competitionId | string            | false    | none         | Competition ID                         |
| »» teamId        | string            | false    | none         | Team ID                                |
| »» totalValue    | number            | false    | none         | Total portfolio value at snapshot time |
| »» timestamp     | string(date-time) | false    | none         | Snapshot timestamp                     |

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
ApiKeyAuth, None, BearerAuth
</aside>

## Get performance reports

> Code samples

```javascript
const headers = {
  Accept: 'application/json',
  'X-API-Key': 'API_KEY',
};

fetch('http://localhost:3000/api/admin/reports/performance?competitionId=string', {
  method: 'GET',

  headers: headers,
})
  .then(function (res) {
    return res.json();
  })
  .then(function (body) {
    console.log(body);
  });
```

`GET /api/admin/reports/performance`

Get performance reports and leaderboard for a competition

<h3 id="get-performance-reports-parameters">Parameters</h3>

| Name          | In    | Type   | Required | Description           |
| ------------- | ----- | ------ | -------- | --------------------- |
| competitionId | query | string | true     | ID of the competition |

> Example responses

> 200 Response

```json
{
  "success": true,
  "competition": {
    "id": "string",
    "name": "string",
    "description": "string",
    "startDate": "2019-08-24T14:15:22Z",
    "endDate": "2019-08-24T14:15:22Z",
    "status": "pending"
  },
  "leaderboard": [
    {
      "rank": 0,
      "teamId": "string",
      "teamName": "string",
      "portfolioValue": 0
    }
  ]
}
```

<h3 id="get-performance-reports-responses">Responses</h3>

| Status | Meaning                                                                    | Description                                  | Schema |
| ------ | -------------------------------------------------------------------------- | -------------------------------------------- | ------ |
| 200    | [OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)                    | Performance reports                          | Inline |
| 400    | [Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)           | Missing competitionId parameter              | None   |
| 401    | [Unauthorized](https://tools.ietf.org/html/rfc7235#section-3.1)            | Unauthorized - Admin authentication required | None   |
| 404    | [Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)             | Competition not found                        | None   |
| 500    | [Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1) | Server error                                 | None   |

<h3 id="get-performance-reports-responseschema">Response Schema</h3>

Status Code **200**

| Name              | Type                   | Required | Restrictions | Description                  |
| ----------------- | ---------------------- | -------- | ------------ | ---------------------------- |
| » success         | boolean                | false    | none         | Operation success status     |
| » competition     | object                 | false    | none         | none                         |
| »» id             | string                 | false    | none         | Competition ID               |
| »» name           | string                 | false    | none         | Competition name             |
| »» description    | string                 | false    | none         | Competition description      |
| »» startDate      | string(date-time)      | false    | none         | Competition start date       |
| »» endDate        | string(date-time)¦null | false    | none         | Competition end date         |
| »» status         | string                 | false    | none         | Competition status           |
| » leaderboard     | [object]               | false    | none         | none                         |
| »» rank           | integer                | false    | none         | Team rank on the leaderboard |
| »» teamId         | string                 | false    | none         | Team ID                      |
| »» teamName       | string                 | false    | none         | Team name                    |
| »» portfolioValue | number                 | false    | none         | Portfolio value              |

#### Enumerated Values

| Property | Value     |
| -------- | --------- |
| status   | pending   |
| status   | active    |
| status   | completed |

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
ApiKeyAuth, None, BearerAuth
</aside>

<h1 id="trading-simulator-api-health">Health</h1>

Health check endpoints

## Basic health check

> Code samples

```javascript
const headers = {
  Accept: 'application/json',
};

fetch('http://localhost:3000/api/health', {
  method: 'GET',

  headers: headers,
})
  .then(function (res) {
    return res.json();
  })
  .then(function (body) {
    console.log(body);
  });
```

`GET /api/health`

Check if the API is running

> Example responses

> 200 Response

```json
{
  "status": "ok",
  "timestamp": "2019-08-24T14:15:22Z",
  "uptime": 0,
  "version": "string"
}
```

<h3 id="basic-health-check-responses">Responses</h3>

| Status | Meaning                                                                    | Description    | Schema |
| ------ | -------------------------------------------------------------------------- | -------------- | ------ |
| 200    | [OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)                    | API is healthy | Inline |
| 500    | [Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1) | Server error   | None   |

<h3 id="basic-health-check-responseschema">Response Schema</h3>

Status Code **200**

| Name        | Type              | Required | Restrictions | Description              |
| ----------- | ----------------- | -------- | ------------ | ------------------------ |
| » status    | string            | false    | none         | Health status of the API |
| » timestamp | string(date-time) | false    | none         | Current server time      |
| » uptime    | number            | false    | none         | Server uptime in seconds |
| » version   | string            | false    | none         | API version              |

<aside class="success">
This operation does not require authentication
</aside>

## Detailed health check

> Code samples

```javascript
const headers = {
  Accept: 'application/json',
};

fetch('http://localhost:3000/api/health/detailed', {
  method: 'GET',

  headers: headers,
})
  .then(function (res) {
    return res.json();
  })
  .then(function (body) {
    console.log(body);
  });
```

`GET /api/health/detailed`

Check if the API and all its services are running properly

> Example responses

> 200 Response

```json
{
  "status": "ok",
  "timestamp": "2019-08-24T14:15:22Z",
  "uptime": 0,
  "version": "string",
  "services": {
    "priceTracker": "ok",
    "balanceManager": "ok",
    "tradeSimulator": "ok",
    "competitionManager": "ok",
    "teamManager": "ok"
  }
}
```

<h3 id="detailed-health-check-responses">Responses</h3>

| Status | Meaning                                                                    | Description            | Schema |
| ------ | -------------------------------------------------------------------------- | ---------------------- | ------ |
| 200    | [OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)                    | Detailed health status | Inline |
| 500    | [Internal Server Error](https://tools.ietf.org/html/rfc7231#section-6.6.1) | Server error           | None   |

<h3 id="detailed-health-check-responseschema">Response Schema</h3>

Status Code **200**

| Name                  | Type              | Required | Restrictions | Description                               |
| --------------------- | ----------------- | -------- | ------------ | ----------------------------------------- |
| » status              | string            | false    | none         | Overall health status of the API          |
| » timestamp           | string(date-time) | false    | none         | Current server time                       |
| » uptime              | number            | false    | none         | Server uptime in seconds                  |
| » version             | string            | false    | none         | API version                               |
| » services            | object            | false    | none         | Status of individual services             |
| »» priceTracker       | string            | false    | none         | Status of the price tracker service       |
| »» balanceManager     | string            | false    | none         | Status of the balance manager service     |
| »» tradeSimulator     | string            | false    | none         | Status of the trade simulator service     |
| »» competitionManager | string            | false    | none         | Status of the competition manager service |
| »» teamManager        | string            | false    | none         | Status of the team manager service        |

<aside class="success">
This operation does not require authentication
</aside>

# Schemas

<h2 id="tocS_Error">Error</h2>
<!-- backwards compatibility -->
<a id="schemaerror"></a>
<a id="schema_Error"></a>
<a id="tocSerror"></a>
<a id="tocserror"></a>

```json
{
  "error": "string",
  "status": 0,
  "timestamp": "2019-08-24T14:15:22Z"
}
```

### Properties

| Name      | Type              | Required | Restrictions | Description                          |
| --------- | ----------------- | -------- | ------------ | ------------------------------------ |
| error     | string            | false    | none         | Error message                        |
| status    | integer           | false    | none         | HTTP status code                     |
| timestamp | string(date-time) | false    | none         | Timestamp of when the error occurred |

<h2 id="tocS_Trade">Trade</h2>
<!-- backwards compatibility -->
<a id="schematrade"></a>
<a id="schema_Trade"></a>
<a id="tocStrade"></a>
<a id="tocstrade"></a>

```json
{
  "id": "string",
  "teamId": "string",
  "competitionId": "string",
  "fromToken": "string",
  "toToken": "string",
  "fromAmount": 0,
  "toAmount": 0,
  "price": 0,
  "success": true,
  "error": "string",
  "timestamp": "2019-08-24T14:15:22Z",
  "fromChain": "string",
  "toChain": "string",
  "fromSpecificChain": "string",
  "toSpecificChain": "string"
}
```

### Properties

| Name              | Type              | Required | Restrictions | Description                                  |
| ----------------- | ----------------- | -------- | ------------ | -------------------------------------------- |
| id                | string            | false    | none         | Unique trade ID                              |
| teamId            | string            | false    | none         | Team ID that executed the trade              |
| competitionId     | string            | false    | none         | ID of the competition this trade is part of  |
| fromToken         | string            | false    | none         | Token address that was sold                  |
| toToken           | string            | false    | none         | Token address that was bought                |
| fromAmount        | number            | false    | none         | Amount of fromToken that was sold            |
| toAmount          | number            | false    | none         | Amount of toToken that was received          |
| price             | number            | false    | none         | Price at which the trade was executed        |
| success           | boolean           | false    | none         | Whether the trade was successfully completed |
| error             | string            | false    | none         | Error message if the trade failed            |
| timestamp         | string(date-time) | false    | none         | Timestamp of when the trade was executed     |
| fromChain         | string            | false    | none         | Blockchain type of the source token          |
| toChain           | string            | false    | none         | Blockchain type of the destination token     |
| fromSpecificChain | string            | false    | none         | Specific chain for the source token          |
| toSpecificChain   | string            | false    | none         | Specific chain for the destination token     |

<h2 id="tocS_TokenBalance">TokenBalance</h2>
<!-- backwards compatibility -->
<a id="schematokenbalance"></a>
<a id="schema_TokenBalance"></a>
<a id="tocStokenbalance"></a>
<a id="tocstokenbalance"></a>

```json
{
  "token": "string",
  "amount": 0,
  "chain": "string",
  "specificChain": "string"
}
```

### Properties

| Name          | Type   | Required | Restrictions | Description                   |
| ------------- | ------ | -------- | ------------ | ----------------------------- |
| token         | string | false    | none         | Token address                 |
| amount        | number | false    | none         | Token balance amount          |
| chain         | string | false    | none         | Chain the token belongs to    |
| specificChain | string | false    | none         | Specific chain for EVM tokens |
