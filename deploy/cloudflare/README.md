# Cloudflare deployment

The Worker sends every request to one `lite` container at `parlor.milkinfrastructure.com`. The image workflow publishes the cached Linux AMD64 scratch image to GHCR. On `main`, it copies that release into Cloudflare Registry with pinned Wrangler. Configure the single GitHub Actions secret `CLOUDFLARE_API_TOKEN`; it must permit container image writes for account `d8a5175f959d3dbd4084db9fcab1c44c`.

Create a temporary JSON file outside the repository containing every required Worker secret:

```json
{
  "MILK_KEYS_JSON": "<operator-key-map>",
  "MILK_UPSTREAM_BASE_URL": "<openai-compatible-origin>",
  "MILK_UPSTREAM_API_KEY": "<upstream-key>",
  "MILK_STORE_ENDPOINT": "<s3-compatible-endpoint>",
  "MILK_STORE_BUCKET": "<bucket>",
  "MILK_STORE_ACCESS_KEY_ID": "<access-key-id>",
  "MILK_STORE_SECRET_ACCESS_KEY": "<secret-access-key>"
}
```

Add `MILK_STORE_SESSION_TOKEN` only when the S3-compatible backend requires it. Do not commit this file. The Worker supplies the fixed container settings: `MILK_LISTEN=0.0.0.0:8080`, `MILK_STORE_KIND=s3`, `MILK_STORE_REGION=auto`, `MILK_STORE_PATH_STYLE=true`, and `MILK_STORE_TIMEOUT_SECONDS=30`.

Validate without uploading, then atomically upload the Worker and its secrets as one version:

```bash
secrets_file=/absolute/path/to/milk-parlor.secrets.json
chmod 600 "$secrets_file"
trap 'rm -f "$secrets_file"' EXIT
npm ci
npm run check
npm run deploy -- --secrets-file "$secrets_file" --strict
```
