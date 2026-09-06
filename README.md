# Milk Parlor

[Read the Milk documentation](https://milkinfrastructure.com/docs/).

Milk Parlor is a small Rust gateway between applications and model providers.
It accepts an operator-issued Milk key, forwards supported OpenAI requests
without changing their bodies, and streams the provider response back.

After a response completes, Parlor attempts to store selected request and
response pairs in folder-like object storage such as Cloudflare R2 or Amazon
S3. Capture runs in the background: a capture
failure does not fail the customer request. Parlor needs no database, external
queue, model weights, or GPU. Milk Man reads the captured exchanges and performs
the heavier work.

[How Milk works](https://milkinfrastructure.com/) ·
[Run Milk Man](https://github.com/milkinfrastructure/milk-man#quickstart)

![Official SDK setup in the Milk dashboard](https://raw.githubusercontent.com/milkinfrastructure/milk-man/main/docs/dashboard-overview.png)

The optional local dashboard shows the gateway connection, saved traffic and
Milk Man's progress. This screenshot is a development snapshot, not live status.

## Use the hosted gateway

Keep the official OpenAI SDK. Change only its base URL and API key:

```bash
pip install openai                 # Python
# or: npm install openai           # JavaScript

export OPENAI_BASE_URL='https://parlor.milkinfrastructure.com/v1'
export OPENAI_API_KEY='your-operator-issued-milk-key'
```

Python:

```python
from openai import OpenAI

client = OpenAI()
response = client.responses.create(model="your-model", input="Reply with milk.")
print(response.output_text)
```

JavaScript:

```javascript
import OpenAI from "openai";

const client = new OpenAI();
const response = await client.responses.create({
  model: "your-model",
  input: "Reply with milk.",
});
console.log(response.output_text);
```

## Routes

| Method | Path | Authentication | Purpose |
| --- | --- | --- | --- |
| `GET` | `/` | None | Status page |
| `GET` | `/status` | None | Same status page |
| `GET` | `/healthz` | None | Gateway configuration and capture counters |
| `GET` | `/api/status` | Milk bearer key | JSON status for the key's scope |
| `POST` | `/v1/chat/completions` | Milk bearer key | Chat Completions create |
| `POST` | `/v1/responses` | Milk bearer key | Responses create |

`/healthz` reports gateway process state; it does not probe the provider or
object store. Parlor supports streaming on both create routes. It does not
claim compatibility with other OpenAI endpoints.

## Run locally

Use Rust 1.93, OpenSSL, and provider credentials for both supported APIs. The
two APIs may use different providers. Provider base URLs must stop before
`/v1`.

```bash
export MILK_BASELINE_CHAT_BASE_URL='https://api.openai.com'
export MILK_BASELINE_CHAT_API_KEY='replace-with-provider-key'
export MILK_BASELINE_RESPONSES_BASE_URL='https://api.openai.com'
export MILK_BASELINE_RESPONSES_API_KEY='replace-with-provider-key'

# Parlor requires a route public key at startup, including for a local test.
umask 077
route_private_key="$(mktemp)"
trap 'rm -f "$route_private_key"' EXIT
openssl genpkey -algorithm ED25519 -out "$route_private_key"
export MILK_ROUTE_VERIFY_KEY="$(
  openssl pkey -in "$route_private_key" -pubout -outform DER \
    | tail -c 32 | base64 | tr -d '\n'
)"

export MILK_STORE_KIND=local
export MILK_STORE_ROOT="$PWD/data"

# This plain-text key is for local development only. Parlor stores only its digest.
milk_key='local-milk-operator-key-change-me'
DIGEST="$(printf %s "$milk_key" | openssl dgst -sha256 -r | awk '{print $1}')"
export MILK_KEYS_JSON="{\"$DIGEST\":{\"scope_id\":\"11111111-1111-4111-8111-111111111111\",\"profile\":\"mechanics\"}}"

cargo run --locked
```

In another terminal, use the same local key with the official SDK:

```python
from openai import OpenAI

client = OpenAI(
    base_url="http://127.0.0.1:8080/v1",
    api_key="local-milk-operator-key-change-me",
)
response = client.responses.create(model="your-model", input="Reply with milk.")
print(response.output_text)
```

Captured complete exchanges are stored as immutable compressed objects at:

```text
milk/v2/scopes/<scope_uuid>/c/<exchange_uuidv7>.json.zst
```

Clients may send one nonzero UUID in `X-Milk-Trajectory-Id`. Parlor removes the
header before forwarding and saves it as optional top-level `trajectory_id`
metadata so related model calls can be read as one ordered task.

## Configuration

Always required:

| Variable | Meaning |
| --- | --- |
| `MILK_KEYS_JSON` | Map of lowercase SHA-256 key digests to `scope_id`, `profile`, and, for production only, a nonzero `route_revision`. Mechanics entries must omit `route_revision`. |
| `MILK_BASELINE_CHAT_BASE_URL`, `MILK_BASELINE_CHAT_API_KEY` | Default Chat Completions provider. |
| `MILK_BASELINE_RESPONSES_BASE_URL`, `MILK_BASELINE_RESPONSES_API_KEY` | Default Responses provider. |
| `MILK_ROUTE_VERIFY_KEY` | Standard-base64 encoding of the 32-byte Ed25519 public key. |

To give one mechanics scope its own model endpoint, set
`MILK_MECHANICS_UPSTREAMS_JSON`. Other scopes and protocols keep their defaults;
production scopes still use signed routes. For example:

```json
{"<scope UUID>":{"chat_completions":{"base_url":"https://your-model.example.com","api_key_env":"MILK_OWN_MODEL_API_KEY"}}}
```

Set the named key separately in the process environment (a Worker secret on
Cloudflare). The URL stops before `/v1`. Restart Parlor after changing these
settings. Requests, responses and trajectory IDs use the existing capture path.
This selects a baseline for experiments; it does not qualify a production route.

Storage:

| Variable | Default or rule |
| --- | --- |
| `MILK_STORE_KIND` | `local`; the other accepted value is `s3`. |
| `MILK_STORE_ROOT` | `./data`; used only for local storage. |
| `MILK_STORE_ENDPOINT`, `MILK_STORE_REGION`, `MILK_STORE_BUCKET` | Required for `s3`. |
| `MILK_STORE_ACCESS_KEY_ID`, `MILK_STORE_SECRET_ACCESS_KEY` | Required for `s3`. |
| `MILK_STORE_SESSION_TOKEN` | Optional for `s3`. |
| `MILK_STORE_PATH_STYLE` | `true`. |
| `MILK_STORE_TIMEOUT_SECONDS` | `30`; accepted range is 1–120. |

Optional candidate routing:

- Set `MILK_CANDIDATE_A_ARTIFACT_SHA256` to 64 lowercase hexadecimal
  characters.
- Set at least one complete URL/key pair:
  `MILK_CANDIDATE_A_CHAT_BASE_URL` with `MILK_CANDIDATE_A_CHAT_API_KEY`, or
  `MILK_CANDIDATE_A_RESPONSES_BASE_URL` with
  `MILK_CANDIDATE_A_RESPONSES_API_KEY`.
- Candidate base URLs also stop before `/v1`.

Optional runtime settings:

| Variable | Default | Accepted range |
| --- | ---: | ---: |
| `MILK_LISTEN` | `0.0.0.0:8080` | Socket address |
| `MILK_MAX_REQUEST_BYTES` | 8 MiB | 1 byte–64 MiB |
| `MILK_MAX_RESPONSE_BYTES` | 16 MiB | 1 byte–256 MiB |
| `MILK_CAPTURE_MEMORY_BYTES` | 64 MiB | At least the smaller of the request limit and 64 MiB; at most 4,294,967,295 bytes |
| `MILK_CAPTURE_QUEUE` | `64` | 1–4,096 |
| `MILK_ROUTE_POLL_SECONDS` | `30` | 1–3,600 |
| `MILK_CANDIDATE_HEADER_TIMEOUT_SECONDS` | `30` | 1–300 |
| `MILK_CANDIDATE_FIRST_BYTE_TIMEOUT_SECONDS` | `120` | 1–600 |

The `*_BYTES` variables are integer byte counts.

## Signed routes

Only production profiles use signed routes. Mechanics profiles use their
configured scope/protocol upstream, or the shared default when none is set.
Missing, invalid, or expired production routes use the shared default.

`ops/publish-route.py` requires Python 3, OpenSSL, the AWS CLI, and the S3
storage variables above. `--candidate-bps` is a basis-point value from 0 to
10,000: `100` means 1% and `10000` means 100%. A zero route must omit all
candidate URL and artifact arguments. After publishing a higher revision,
redeploy the production key entry with that exact `route_revision`.

Keep the private signing key outside Milk Parlor, Milk Man, CI, and HTTP
requests.

Use a saved Milk Man proposal instead of retyping its model and URLs:

```bash
python3 ops/publish-route.py \
  --proposal-file "$PROPOSAL_FILE" --proposal-sha256 "$PROPOSAL_SHA256" \
  --signing-key "$SIGNING_KEY" --scope-id "$SCOPE_ID" \
  --revision "$REVISION" --candidate-bps 100 --expires-at "$EXPIRES_AT" \
  --output-dir ./prepared-route
```

`PROPOSAL_SHA256` is the saved reference's SHA-256 of the whole file, not the
`proposal_sha256` field inside it. The command checks that file and scope,
then signs its model/URL bindings with your chosen traffic share and expiry.
Do not combine a proposal with manual candidate URL or artifact arguments.

`--output-dir` creates a new private directory containing `route.json` and
`current.json`; it makes no cloud calls and needs no AWS CLI or storage keys.
These files prove preparation, not a running model or an active route. Run
without `--output-dir` to sign and publish through S3. Deploy the matching
candidate URL, credential and artifact environment settings separately, then
set the production key's exact `route_revision`. A mechanics proposal remains
mechanics evidence; signing it does not establish model quality.

## Deploy

For the pinned Cloudflare Worker and container workflow, see
[deploy/cloudflare/README.md](deploy/cloudflare/README.md).

Report security issues privately as described in [SECURITY.md](SECURITY.md).
