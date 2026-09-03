# Milk Parlor

Milk Parlor is a small OpenAI-compatible Rust gateway between applications and model providers.
It checks an operator-issued key, forwards Chat Completions and Responses
requests without translating them, and streams the answer back. After a complete
answer, it saves the request and response to object storage in the background.
If that write fails, the customer request still succeeds.

Milk Parlor needs no database, external queue service, model weights, or GPU.
Milk Man reads the saved conversations and does the heavier work.

## Connect an application

There is no replacement Milk SDK. Keep the official OpenAI package and change
only its base URL and key:

```bash
pip install openai # or: npm install openai
export OPENAI_BASE_URL=https://parlor.milkinfrastructure.com/v1
export OPENAI_API_KEY='your operator-issued Milk key'
```

Existing Python code remains unchanged:

```python
from openai import OpenAI

client = OpenAI()
response = client.responses.create(model="your-model", input="Reply with milk.")
print(response.output_text)
```

Existing JavaScript code remains unchanged:

```javascript
import OpenAI from "openai";

const client = new OpenAI();
const response = await client.responses.create({model: "your-model", input: "Reply with milk."});
console.log(response.output_text);
```

Milk tunnels OpenAI's
[Responses](https://developers.openai.com/api/reference/cli/resources/responses/methods/create)
and [Chat Completions](https://developers.openai.com/api/reference/cli/resources/chat/subresources/completions)
create routes, including streaming bodies, without protocol translation. It
does not claim compatibility with unrelated OpenAI endpoints.

## Run

```bash
export MILK_BASELINE_CHAT_BASE_URL=https://api.openai.com
export MILK_BASELINE_CHAT_API_KEY=...
export MILK_BASELINE_RESPONSES_BASE_URL=https://api.openai.com
export MILK_BASELINE_RESPONSES_API_KEY=...
export MILK_ROUTE_VERIFY_KEY=... # standard base64 Ed25519 public key
export MILK_STORE_KIND=local
export MILK_STORE_ROOT="$PWD/data"

export MILK_API_KEY='replace-with-an-operator-key'
DIGEST="$(printf %s "$MILK_API_KEY" | shasum -a 256 | cut -d' ' -f1)"
export MILK_KEYS_JSON="{\"$DIGEST\":{\"scope_id\":\"11111111-1111-4111-8111-111111111111\",\"profile\":\"mechanics\"}}"

cargo run
```

Use the key with the official Python SDK against a local Parlor:

```python
import os
from openai import OpenAI

client = OpenAI(
    base_url="http://127.0.0.1:8080/v1",
    api_key=os.environ["MILK_API_KEY"],
)

response = client.responses.create(
    model="gpt-5.6-luna",
    input="Reply with milk.",
)
print(response.output_text)
```

Each complete conversation is compressed and stored at:

```text
milk/v2/scopes/<scope_uuid>/c/<exchange_uuidv7>.json.zst
```

`GET /healthz` is public. `GET /status` shows a small status page. That page uses
the operator key to read `milk/v2/scopes/<scope_uuid>/status/current.json`.

## Configuration

Required:

- `MILK_KEYS_JSON`: maps each key's lowercase SHA-256 digest to its customer
  `scope_id` and `profile` (`production` or `mechanics`). A production entry also
  names the exact nonzero `route_revision` it may use.
- `MILK_BASELINE_CHAT_BASE_URL` and `MILK_BASELINE_CHAT_API_KEY`: the default
  Chat Completions provider.
- `MILK_BASELINE_RESPONSES_BASE_URL` and `MILK_BASELINE_RESPONSES_API_KEY`: the
  default Responses provider. The two APIs may use different providers and
  keys. Use provider base URLs before `/v1`.
- `MILK_ROUTE_VERIFY_KEY`: standard-base64 encoding of the 32-byte Ed25519 route verification key.

Optional:

- `MILK_LISTEN` (`0.0.0.0:8080`)
- `MILK_STORE_KIND` (`local`; set `s3` for S3-compatible storage)
- `MILK_STORE_ROOT` (`./data`)
- `MILK_MAX_REQUEST_BYTES` (8 MiB)
- `MILK_MAX_RESPONSE_BYTES` (16 MiB)
- `MILK_CAPTURE_MEMORY_BYTES` (64 MiB across active and queued captures)
- `MILK_CAPTURE_QUEUE` (64)
- `MILK_ROUTE_POLL_SECONDS` (30)
- `MILK_CANDIDATE_A_ARTIFACT_SHA256` plus a complete candidate pair for Chat
  Completions, Responses, or both. Each pair is its `*_BASE_URL` and
  `*_API_KEY`. A route can use only the APIs that candidate implements.
- `MILK_CANDIDATE_HEADER_TIMEOUT_SECONDS` (30)
- `MILK_CANDIDATE_FIRST_BYTE_TIMEOUT_SECONDS` (120)

For `MILK_STORE_KIND=s3`, set `MILK_STORE_ENDPOINT`, `MILK_STORE_REGION`,
`MILK_STORE_BUCKET`, `MILK_STORE_ACCESS_KEY_ID`, and
`MILK_STORE_SECRET_ACCESS_KEY`. `MILK_STORE_SESSION_TOKEN` is optional.
`MILK_STORE_PATH_STYLE` defaults to `true` and `MILK_STORE_TIMEOUT_SECONDS` to
`30`. A captured conversation is created once and never overwritten.

## Signed routes

Only production traffic uses signed routes; mechanics traffic always uses the
default provider. Milk Parlor checks routes in the background, so a request
never waits for object storage. Missing, invalid, or expired routes use the
default provider.

A route fixes the candidate model, eligible API, and traffic percentage. Each
exchange gets a deterministic assignment. If a candidate cannot connect, times
out, returns an error, or fails before its first response byte, Milk Parlor
retries that request with the matching default provider. It never moves a
request after streaming has begun.

Route signing is an operator action, not a Milk Man tool. Generate a key and derive the deployment value without exposing the private key:

```bash
umask 077
openssl genpkey -algorithm ED25519 -out /secure/milk-route.pem
openssl pkey -in /secure/milk-route.pem -pubout -outform DER \
  | tail -c 32 | base64 | tr -d '\n'
```

With the S3 variables above exported, publish a higher revision:

```bash
export MILK_ROUTE_EXPIRES_AT='<future RFC3339 UTC timestamp>'

./ops/publish-route.py \
  --signing-key /secure/milk-route.pem \
  --scope-id 11111111-1111-4111-8111-111111111111 \
  --revision 1 --candidate-bps 100 \
  --candidate-chat-base-url https://candidate.example.com \
  --candidate-artifact-sha256 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef \
  --expires-at "$MILK_ROUTE_EXPIRES_AT"
```

The command creates a new immutable route and advances `r/current.json` only if
it has not changed underneath the operator. Activate it by redeploying each
production key entry with that exact `route_revision`. Roll back with a higher
revision that selects the prior model. Stop candidate traffic with a higher
revision and `--candidate-bps 0`.

Keep the private signing key outside Milk Parlor, Milk Man, CI, and HTTP
requests.
