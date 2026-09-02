# Milk Parlor

Milk Parlor is the small Rust gateway for Milk. It authenticates operator-issued keys, tunnels OpenAI-compatible Chat Completions and Responses requests to protocol-native upstreams, streams the response, and writes a compressed two-sided exchange to object storage after a complete response. Capture failure never changes the customer response.

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

Use the key with the official Python SDK:

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

Complete captures are stored at:

```text
milk/v2/scopes/<scope_uuid>/c/<exchange_uuidv7>.json.zst
```

`GET /healthz` is public. `GET /status` is a small status reader; its `GET /api/status` request uses the same operator key and reads `milk/v2/scopes/<scope_uuid>/status/current.json`.

## Configuration

Required:

- `MILK_KEYS_JSON`: object mapping lowercase SHA-256 key digests to `scope_id` and `profile` (`production` or `mechanics`). Every production binding also carries its exact nonzero `route_revision`; mechanics bindings omit it.
- `MILK_BASELINE_CHAT_BASE_URL` and `MILK_BASELINE_CHAT_API_KEY`: known-good native Chat Completions binding.
- `MILK_BASELINE_RESPONSES_BASE_URL` and `MILK_BASELINE_RESPONSES_API_KEY`: known-good native Responses binding. The two protocols may use different providers and keys. Base URLs are provider prefixes before `/v1`; Parlor appends the exact client endpoint and never translates one API into the other.
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
- `MILK_CANDIDATE_A_ARTIFACT_SHA256` plus one or both complete native protocol pairs: `MILK_CANDIDATE_A_CHAT_BASE_URL` with `MILK_CANDIDATE_A_CHAT_API_KEY`, and `MILK_CANDIDATE_A_RESPONSES_BASE_URL` with `MILK_CANDIDATE_A_RESPONSES_API_KEY`. A signed route can select only protocols explicitly implemented by the candidate.
- `MILK_CANDIDATE_HEADER_TIMEOUT_SECONDS` (30)
- `MILK_CANDIDATE_FIRST_BYTE_TIMEOUT_SECONDS` (120)

For `MILK_STORE_KIND=s3`, set `MILK_STORE_ENDPOINT`, `MILK_STORE_REGION`, `MILK_STORE_BUCKET`, `MILK_STORE_ACCESS_KEY_ID`, and `MILK_STORE_SECRET_ACCESS_KEY`. `MILK_STORE_SESSION_TOKEN` is optional; `MILK_STORE_PATH_STYLE` defaults to `true` and `MILK_STORE_TIMEOUT_SECONDS` defaults to `30`. The S3 writer uses signed create-only puts and never overwrites a capture.

## Signed routes

Only production scopes consume routes. Mechanics traffic always uses `baseline`. Requests read the in-memory route cache and never wait for object storage; one background refresh per scope verifies the canonical `milk.route-pointer.v2` and immutable `milk.route.v3` objects. A miss, failed refresh, or expired route uses `baseline` or the last unexpired verified route.

An active route signs the exact candidate artifact SHA-256, basis points, and a binding digest for every eligible protocol. Each binding digest covers the protocol, provider base URL, and artifact digest. Parlor considers the candidate only when the request protocol is signed and the deployed native binding matches; unsupported protocols go directly to their baseline without a failed candidate call. An absent or mismatched signed binding fails to baseline. Captures retain the client protocol, target, artifact digest, binding digest, and fallback reason. Assignment is deterministic from SHA-256 over the raw route UUID followed by the exchange UUID. Candidate connection/header timeout, 429, 5xx, invalid headers, or failure before the first response byte uses the same protocol's baseline. After the first byte, the response streams without a gateway deadline.

Route signing is an operator action, not a Milk Man tool. Generate a key and derive the deployment value without exposing the private key:

```bash
umask 077
openssl genpkey -algorithm ED25519 -out /secure/milk-route.pem
openssl pkey -in /secure/milk-route.pem -pubout -outform DER \
  | tail -c 32 | base64 | tr -d '\n'
```

With the S3 variables above exported, publish a higher revision:

```bash
./ops/publish-route.py \
  --signing-key /secure/milk-route.pem \
  --scope-id 11111111-1111-4111-8111-111111111111 \
  --revision 1 --candidate-bps 100 \
  --candidate-chat-base-url https://candidate.example.com \
  --candidate-artifact-sha256 0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef \
  --expires-at 2026-09-02T00:00:00Z
```

The publisher creates `r/<route_uuid>.json` with `If-None-Match: *`, then advances `r/current.json` with an ETag precondition. Activate it by atomically redeploying each production key binding with that exact `route_revision`; a process never accepts another revision, including after restart. Keep route expiries short so an uploaded route that is not deployed returns to baseline quickly. Rollback is a higher revision restoring a prior known-good artifact. A zero route is a higher revision with `--candidate-bps 0`; it needs no candidate binding. The private signing key must never enter Parlor, Milk Man, CI, or an HTTP request.
