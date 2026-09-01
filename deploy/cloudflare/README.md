# Cloudflare deployment

The Worker sends every request to one `lite` container at `parlor.milkinfrastructure.com`. The image workflow publishes the same multi-architecture scratch image to GHCR and the public Docker Hub repository Cloudflare can pull. Configure the GitHub Actions secrets `DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN` first.

Set these Worker secrets before the first deployment:

```bash
for name in \
  MILK_KEYS_JSON \
  MILK_UPSTREAM_BASE_URL \
  MILK_UPSTREAM_API_KEY \
  MILK_STORE_ENDPOINT \
  MILK_STORE_BUCKET \
  MILK_STORE_ACCESS_KEY_ID \
  MILK_STORE_SECRET_ACCESS_KEY
do
  npx wrangler secret put "$name"
done
```

Set `MILK_STORE_SESSION_TOKEN` the same way only when the S3-compatible backend requires it. The Worker supplies the fixed container settings: `MILK_LISTEN=0.0.0.0:8080`, `MILK_STORE_KIND=s3`, `MILK_STORE_REGION=auto`, `MILK_STORE_PATH_STYLE=true`, and `MILK_STORE_TIMEOUT_SECONDS=30`.

Validate without deploying, then deploy:

```bash
npm ci
npm run check
npm run deploy
```
