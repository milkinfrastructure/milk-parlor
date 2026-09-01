import { Container, getContainer } from "@cloudflare/containers";
import { env } from "cloudflare:workers";

function containerEnvironment(bindings) {
  return {
    MILK_LISTEN: "0.0.0.0:8080",
    MILK_STORE_KIND: "s3",
    MILK_STORE_REGION: "auto",
    MILK_STORE_PATH_STYLE: "true",
    MILK_STORE_TIMEOUT_SECONDS: "30",
    MILK_KEYS_JSON: bindings.MILK_KEYS_JSON,
    MILK_UPSTREAM_BASE_URL: bindings.MILK_UPSTREAM_BASE_URL,
    MILK_UPSTREAM_API_KEY: bindings.MILK_UPSTREAM_API_KEY,
    MILK_STORE_ENDPOINT: bindings.MILK_STORE_ENDPOINT,
    MILK_STORE_BUCKET: bindings.MILK_STORE_BUCKET,
    MILK_STORE_ACCESS_KEY_ID: bindings.MILK_STORE_ACCESS_KEY_ID,
    MILK_STORE_SECRET_ACCESS_KEY: bindings.MILK_STORE_SECRET_ACCESS_KEY,
    ...(bindings.MILK_STORE_SESSION_TOKEN === undefined
      ? {}
      : { MILK_STORE_SESSION_TOKEN: bindings.MILK_STORE_SESSION_TOKEN }),
  };
}

export class MilkParlor extends Container {
  defaultPort = 8080;
  sleepAfter = "10m";
  envVars = containerEnvironment(env);
}

export default {
  fetch(request) {
    return getContainer(env.MILK_PARLOR, "parlor").fetch(request);
  },
};
