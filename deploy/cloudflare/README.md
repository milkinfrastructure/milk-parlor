# Cloudflare deployment

The Worker sends every request to one `lite` container at `parlor.milkinfrastructure.com`. The image workflow publishes the multi-architecture scratch image to GHCR. On `main`, it loads the Linux AMD64 release and copies it into Cloudflare Registry with pinned Wrangler. Configure the single GitHub Actions secret `CLOUDFLARE_API_TOKEN`; it must permit container image writes for account `d8a5175f959d3dbd4084db9fcab1c44c`.

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
