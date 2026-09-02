import { Container, getContainer } from "@cloudflare/containers";
import { env } from "cloudflare:workers";

function containerEnvironment(bindings) {
  const candidateChat = [
    bindings.MILK_CANDIDATE_A_CHAT_BASE_URL,
    bindings.MILK_CANDIDATE_A_CHAT_API_KEY,
  ];
  const candidateResponses = [
    bindings.MILK_CANDIDATE_A_RESPONSES_BASE_URL,
    bindings.MILK_CANDIDATE_A_RESPONSES_API_KEY,
  ];
  for (const pair of [candidateChat, candidateResponses]) {
    const configured = pair.filter((value) => value !== undefined).length;
    if (configured !== 0 && configured !== pair.length) {
      throw new Error("each candidate protocol URL and key must be set together");
    }
  }
  const candidateConfigured = [...candidateChat, ...candidateResponses].some(
    (value) => value !== undefined,
  );
  if (candidateConfigured !== (bindings.MILK_CANDIDATE_A_ARTIFACT_SHA256 !== undefined)) {
    throw new Error("candidate protocol bindings and artifact digest must be set together");
  }
  return {
    MILK_LISTEN: "0.0.0.0:8080",
    MILK_STORE_KIND: "s3",
    MILK_STORE_REGION: "auto",
    MILK_STORE_PATH_STYLE: "true",
    MILK_STORE_TIMEOUT_SECONDS: "30",
    MILK_KEYS_JSON: bindings.MILK_KEYS_JSON,
    MILK_BASELINE_CHAT_BASE_URL: bindings.MILK_BASELINE_CHAT_BASE_URL,
    MILK_BASELINE_CHAT_API_KEY: bindings.MILK_BASELINE_CHAT_API_KEY,
    MILK_BASELINE_RESPONSES_BASE_URL: bindings.MILK_BASELINE_RESPONSES_BASE_URL,
    MILK_BASELINE_RESPONSES_API_KEY: bindings.MILK_BASELINE_RESPONSES_API_KEY,
    MILK_ROUTE_VERIFY_KEY: bindings.MILK_ROUTE_VERIFY_KEY,
    MILK_ROUTE_POLL_SECONDS: "30",
    MILK_STORE_ENDPOINT: bindings.MILK_STORE_ENDPOINT,
    MILK_STORE_BUCKET: bindings.MILK_STORE_BUCKET,
    MILK_STORE_ACCESS_KEY_ID: bindings.MILK_STORE_ACCESS_KEY_ID,
    MILK_STORE_SECRET_ACCESS_KEY: bindings.MILK_STORE_SECRET_ACCESS_KEY,
    ...(!candidateConfigured
      ? {}
      : {
          MILK_CANDIDATE_A_ARTIFACT_SHA256:
            bindings.MILK_CANDIDATE_A_ARTIFACT_SHA256,
          ...(candidateChat[0] === undefined
            ? {}
            : {
                MILK_CANDIDATE_A_CHAT_BASE_URL: candidateChat[0],
                MILK_CANDIDATE_A_CHAT_API_KEY: candidateChat[1],
              }),
          ...(candidateResponses[0] === undefined
            ? {}
            : {
                MILK_CANDIDATE_A_RESPONSES_BASE_URL: candidateResponses[0],
                MILK_CANDIDATE_A_RESPONSES_API_KEY: candidateResponses[1],
              }),
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
    const instance = env.MILK_PARLOR_INSTANCE;
    if (typeof instance !== "string" || !/^parlor-[a-z0-9-]{1,48}$/.test(instance)) {
      throw new Error("MILK_PARLOR_INSTANCE is invalid");
    }
    return getContainer(env.MILK_PARLOR, instance).fetch(request);
  },
};
