import { Container, getContainer } from "@cloudflare/containers";
import { env } from "cloudflare:workers";

function containerEnvironment(bindings) {
  const candidate = [
    bindings.MILK_CANDIDATE_A_BASE_URL,
    bindings.MILK_CANDIDATE_A_API_KEY,
    bindings.MILK_CANDIDATE_A_ARTIFACT_SHA256,
  ];
  const configured = candidate.filter((value) => value !== undefined).length;
  if (configured !== 0 && configured !== candidate.length) {
    throw new Error("candidate URL, key, and artifact digest must be set together");
  }
  return {
    MILK_LISTEN: "0.0.0.0:8080",
    MILK_STORE_KIND: "s3",
    MILK_STORE_REGION: "auto",
    MILK_STORE_PATH_STYLE: "true",
    MILK_STORE_TIMEOUT_SECONDS: "30",
    MILK_KEYS_JSON: bindings.MILK_KEYS_JSON,
    MILK_BASELINE_BASE_URL: bindings.MILK_BASELINE_BASE_URL,
    MILK_BASELINE_API_KEY: bindings.MILK_BASELINE_API_KEY,
    MILK_ROUTE_VERIFY_KEY: bindings.MILK_ROUTE_VERIFY_KEY,
    MILK_ROUTE_POLL_SECONDS: "30",
    MILK_STORE_ENDPOINT: bindings.MILK_STORE_ENDPOINT,
    MILK_STORE_BUCKET: bindings.MILK_STORE_BUCKET,
    MILK_STORE_ACCESS_KEY_ID: bindings.MILK_STORE_ACCESS_KEY_ID,
    MILK_STORE_SECRET_ACCESS_KEY: bindings.MILK_STORE_SECRET_ACCESS_KEY,
    ...(configured === 0
      ? {}
      : {
          MILK_CANDIDATE_A_BASE_URL: candidate[0],
          MILK_CANDIDATE_A_API_KEY: candidate[1],
          MILK_CANDIDATE_A_ARTIFACT_SHA256: candidate[2],
        }),
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
