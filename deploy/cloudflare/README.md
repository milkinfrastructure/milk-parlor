# Cloudflare deployment

The Worker sends requests to `lite` containers at `parlor.milkinfrastructure.com`. Build the Linux AMD64 scratch image locally, push it to Cloudflare Registry, then pin the returned digest in `wrangler.jsonc`:

```bash
npm ci
docker buildx build --platform linux/amd64 --tag milk-parlor:main --load ../..
npx wrangler containers push milk-parlor:main
```

Run these commands from `deploy/cloudflare`. The Cloudflare session must permit container image writes for account `d8a5175f959d3dbd4084db9fcab1c44c`. Milk Parlor does not use GitHub Actions.

For the first deployment, create a private JSON file outside the repository containing every required Worker secret:

```json
{
  "MILK_KEYS_JSON": "<operator-key-map; each production binding includes its exact route_revision>",
  "MILK_PARLOR_INSTANCE": "<new parlor-lowercase-generation for each cutover>",
  "MILK_BASELINE_CHAT_BASE_URL": "<native-chat-provider-prefix-before-v1>",
  "MILK_BASELINE_CHAT_API_KEY": "<chat-key>",
  "MILK_BASELINE_RESPONSES_BASE_URL": "<native-responses-provider-prefix-before-v1>",
  "MILK_BASELINE_RESPONSES_API_KEY": "<responses-key>",
  "MILK_ROUTE_VERIFY_KEY": "<standard-base64-ed25519-public-key>",
  "MILK_STORE_ENDPOINT": "<s3-compatible-endpoint>",
  "MILK_STORE_BUCKET": "<bucket>",
  "MILK_STORE_ACCESS_KEY_ID": "<access-key-id>",
  "MILK_STORE_SECRET_ACCESS_KEY": "<secret-access-key>"
}
```

Add `MILK_CANDIDATE_A_ARTIFACT_SHA256` only with at least one complete native pair: `MILK_CANDIDATE_A_CHAT_BASE_URL` and `MILK_CANDIDATE_A_CHAT_API_KEY`, or `MILK_CANDIDATE_A_RESPONSES_BASE_URL` and `MILK_CANDIDATE_A_RESPONSES_API_KEY`. Add `MILK_STORE_SESSION_TOKEN` only when the S3-compatible backend requires it. Do not commit this file. The Worker supplies the fixed container settings: `MILK_LISTEN=0.0.0.0:8080`, `MILK_STORE_KIND=s3`, `MILK_STORE_REGION=auto`, `MILK_STORE_PATH_STYLE=true`, `MILK_STORE_TIMEOUT_SECONDS=30`, and `MILK_ROUTE_POLL_SECONDS=30`.

For updates, `--secrets-file` changes only the supplied secrets; omitted secrets remain saved. The value of `MILK_KEYS_JSON` is replaced as a whole, so preserve every existing binding when adding a key. Keep a private recoverable copy of that map and the credential source; Cloudflare does not reveal saved secret values. Change `MILK_PARLOR_INSTANCE` to start a generation with the updated environment.

Validate without uploading, then atomically upload the Worker and its secrets as one version:

```bash
secrets_file=/absolute/path/to/milk-parlor.secrets.json
chmod 600 "$secrets_file"
npm run check
npm run deploy -- --secrets-file "$secrets_file" --strict
```
