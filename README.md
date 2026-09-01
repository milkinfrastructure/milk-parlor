# Milk Parlor

Milk Parlor is the small Rust gateway for Milk. It authenticates operator-issued keys, forwards OpenAI-compatible Chat Completions and Responses requests, streams the upstream response, and writes a compressed two-sided exchange to object storage after a complete response. Capture failure never changes the customer response.

## Run

```bash
export MILK_UPSTREAM_BASE_URL=https://api.openai.com
export MILK_UPSTREAM_API_KEY=...
export MILK_STORE_KIND=local
export MILK_STORE_ROOT="$PWD/data"

KEY='replace-with-an-operator-key'
DIGEST="$(printf %s "$KEY" | shasum -a 256 | cut -d' ' -f1)"
export MILK_KEYS_JSON="{\"$DIGEST\":{\"scope_id\":\"11111111-1111-4111-8111-111111111111\",\"profile\":\"mechanics\"}}"

cargo run
```

Use the key with the official SDK:

```bash
curl http://127.0.0.1:8080/v1/responses \
  -H "Authorization: Bearer $KEY" \
  -H 'Content-Type: application/json' \
  -d '{"model":"gpt-5.6-luna","input":"Reply with milk."}'
```

Complete captures are stored at:

```text
milk/v2/scopes/<scope_uuid>/c/<exchange_uuidv7>.json.zst
```

`GET /healthz` is public. `GET /status` is a small status reader; its `GET /api/status` request uses the same operator key and reads `milk/v2/scopes/<scope_uuid>/status/current.json`.

## Configuration

Required:

- `MILK_KEYS_JSON`: object mapping lowercase SHA-256 key digests to `scope_id` and `profile` (`production` or `mechanics`).
- `MILK_UPSTREAM_BASE_URL`: OpenAI-compatible HTTP(S) base URL.
- `MILK_UPSTREAM_API_KEY`: upstream bearer key.

Optional:

- `MILK_LISTEN` (`0.0.0.0:8080`)
- `MILK_STORE_KIND` (`local`; set `s3` for S3-compatible storage)
- `MILK_STORE_ROOT` (`./data`)
- `MILK_MAX_REQUEST_BYTES` (8 MiB)
- `MILK_MAX_RESPONSE_BYTES` (16 MiB)
- `MILK_CAPTURE_MEMORY_BYTES` (64 MiB across active and queued captures)
- `MILK_CAPTURE_QUEUE` (64)

For `MILK_STORE_KIND=s3`, set `MILK_STORE_ENDPOINT`, `MILK_STORE_REGION`, `MILK_STORE_BUCKET`, `MILK_STORE_ACCESS_KEY_ID`, and `MILK_STORE_SECRET_ACCESS_KEY`. `MILK_STORE_SESSION_TOKEN` is optional; `MILK_STORE_PATH_STYLE` defaults to `true` and `MILK_STORE_TIMEOUT_SECONDS` defaults to `30`. The S3 writer uses signed create-only puts and never overwrites a capture.
