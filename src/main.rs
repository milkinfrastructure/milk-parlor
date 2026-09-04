use anyhow::{Context, Result, anyhow, bail};
use axum::{
    Json, Router,
    body::{Body, to_bytes},
    extract::{Request, State},
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header},
    response::{Html, IntoResponse, Response},
    routing::{get, post},
};
use base64::Engine;
use bytes::Bytes;
use futures_util::Stream;
use reqwest::redirect::Policy;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, HashMap},
    env,
    io::Cursor,
    net::SocketAddr,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::{Context as TaskContext, Poll},
    time::{Duration, Instant},
};
use subtle::ConstantTimeEq;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};
use url::Url;
use uuid::Uuid;

mod route;
mod store;
use route::{Protocol, RouteManager, Target, parse_verifying_key};
use store::{MAX_GET_BYTES, Store};

const EXCHANGE_SCHEMA: &str = "milk.exchange.v2";
const DEFAULT_LISTEN: &str = "0.0.0.0:8080";
const DEFAULT_MAX_REQUEST_BYTES: usize = 8 * 1024 * 1024;
const DEFAULT_MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_CAPTURE_MEMORY_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_CAPTURE_QUEUE: usize = 64;

#[derive(Clone)]
struct AppState {
    client: reqwest::Client,
    baseline: ProtocolBindings,
    candidate_a: Option<CandidateBinding>,
    keys: Arc<Vec<OperatorKey>>,
    routes: RouteManager,
    store: Store,
    capture_tx: mpsc::Sender<CaptureJob>,
    capture_memory: Arc<Semaphore>,
    max_request_bytes: usize,
    max_response_bytes: usize,
    candidate_header_timeout: Duration,
    candidate_first_byte_timeout: Duration,
    counters: Arc<Counters>,
}

#[derive(Clone)]
struct UpstreamBinding {
    base_url: Url,
    api_key: Arc<str>,
}

#[derive(Clone)]
struct ProtocolBindings {
    chat_completions: UpstreamBinding,
    responses: UpstreamBinding,
}

impl ProtocolBindings {
    fn get(&self, protocol: Protocol) -> &UpstreamBinding {
        match protocol {
            Protocol::ChatCompletions => &self.chat_completions,
            Protocol::Responses => &self.responses,
        }
    }
}

#[derive(Clone)]
struct CandidateProtocolBinding {
    upstream: UpstreamBinding,
    binding_sha256: Arc<str>,
}

#[derive(Clone)]
struct CandidateBinding {
    artifact_sha256: Arc<str>,
    chat_completions: Option<CandidateProtocolBinding>,
    responses: Option<CandidateProtocolBinding>,
}

impl CandidateBinding {
    fn get(&self, protocol: Protocol) -> Option<&CandidateProtocolBinding> {
        match protocol {
            Protocol::ChatCompletions => self.chat_completions.as_ref(),
            Protocol::Responses => self.responses.as_ref(),
        }
    }
}

#[derive(Serialize)]
struct CandidateIdentity<'a> {
    artifact_sha256: &'a str,
    base_url: &'a str,
    protocol: Protocol,
}

#[derive(Clone)]
struct OperatorKey {
    digest: [u8; 32],
    scope_id: Uuid,
    profile: Profile,
    route_revision: Option<u64>,
}

#[derive(Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Profile {
    Production,
    Mechanics,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KeyBinding {
    scope_id: Uuid,
    profile: Profile,
    route_revision: Option<u64>,
}

#[derive(Default)]
struct Counters {
    observed: AtomicU64,
    completed: AtomicU64,
    enqueued: AtomicU64,
    persisted: AtomicU64,
    dropped: AtomicU64,
    oversized: AtomicU64,
    interrupted: AtomicU64,
    storage_failed: AtomicU64,
    writer_alive: AtomicBool,
}

struct CaptureJob {
    scope_id: Uuid,
    profile: Profile,
    trajectory_id: Option<Uuid>,
    exchange_id: Uuid,
    started_at: String,
    completed_at: String,
    endpoint: &'static str,
    streaming: bool,
    request_method: String,
    request_path: String,
    request_headers: BTreeMap<String, String>,
    request: Bytes,
    response_status: u16,
    response_headers: BTreeMap<String, String>,
    response: Vec<u8>,
    route_id: Option<Uuid>,
    route_target: &'static str,
    candidate_artifact_sha256: Option<String>,
    candidate_binding_sha256: Option<String>,
    fallback_reason: Option<String>,
    ttft_ms: Option<u64>,
    total_ms: u64,
    _memory: Vec<OwnedSemaphorePermit>,
}

struct CaptureSeed {
    scope_id: Uuid,
    profile: Profile,
    trajectory_id: Option<Uuid>,
    exchange_id: Uuid,
    started_at: String,
    endpoint: &'static str,
    streaming: bool,
    request_method: String,
    request_path: String,
    request_headers: BTreeMap<String, String>,
    request: Bytes,
    response_status: u16,
    response_headers: BTreeMap<String, String>,
    route_id: Option<Uuid>,
    route_target: &'static str,
    candidate_artifact_sha256: Option<String>,
    candidate_binding_sha256: Option<String>,
    fallback_reason: Option<String>,
}

struct ResponseRecorder {
    seed: Option<CaptureSeed>,
    response: Vec<u8>,
    memory: Vec<OwnedSemaphorePermit>,
    capture_tx: mpsc::Sender<CaptureJob>,
    capture_memory: Arc<Semaphore>,
    counters: Arc<Counters>,
    started: Instant,
    first_byte: Option<Duration>,
    max_response_bytes: usize,
    exchange_id: Uuid,
    expected_response_bytes: Option<usize>,
    observed_response_bytes: usize,
    stream_terminal_seen: bool,
    stream_terminal_tail: Vec<u8>,
    finished: bool,
}

struct UpstreamBody {
    first: Option<Bytes>,
    inner: Pin<Box<dyn Stream<Item = reqwest::Result<Bytes>> + Send>>,
    recorder: ResponseRecorder,
}

impl Stream for UpstreamBody {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
    ) -> Poll<Option<Self::Item>> {
        if let Some(bytes) = self.first.take() {
            self.recorder.observe(&bytes);
            return Poll::Ready(Some(Ok(bytes)));
        }
        match self.inner.as_mut().poll_next(context) {
            Poll::Ready(Some(Ok(bytes))) => {
                self.recorder.observe(&bytes);
                Poll::Ready(Some(Ok(bytes)))
            }
            Poll::Ready(Some(Err(error))) => {
                eprintln!(
                    "upstream stream failed for {}: {error}",
                    self.recorder.exchange_id
                );
                self.recorder.finish(true);
                Poll::Ready(Some(Err(std::io::Error::other("upstream stream failed"))))
            }
            Poll::Ready(None) => {
                self.recorder.finish(false);
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for UpstreamBody {
    fn drop(&mut self) {
        let interrupted = !self.recorder.response_complete();
        self.recorder.finish(interrupted);
    }
}

impl ResponseRecorder {
    fn observe(&mut self, bytes: &Bytes) {
        if self.first_byte.is_none() && !bytes.is_empty() {
            self.first_byte = Some(self.started.elapsed());
        }
        self.observed_response_bytes = self.observed_response_bytes.saturating_add(bytes.len());
        self.observe_stream_terminal(bytes);
        if self.seed.is_none() || bytes.is_empty() {
            return;
        }
        let next = self.response.len().checked_add(bytes.len());
        if next.is_none_or(|length| length > self.max_response_bytes) {
            self.disable_capture(true);
            return;
        }
        let Ok(count) = u32::try_from(bytes.len()) else {
            self.disable_capture(true);
            return;
        };
        match Arc::clone(&self.capture_memory).try_acquire_many_owned(count) {
            Ok(permit) => {
                self.memory.push(permit);
                self.response.extend_from_slice(bytes);
            }
            Err(_) => self.disable_capture(false),
        }
    }

    fn observe_stream_terminal(&mut self, bytes: &[u8]) {
        if self.stream_terminal_seen || bytes.is_empty() {
            return;
        }
        const MARKERS: [&[u8]; 4] = [
            b"data: [DONE]",
            b"event: response.completed",
            b"event: response.failed",
            b"event: response.incomplete",
        ];
        let tail_limit = MARKERS
            .iter()
            .map(|marker| marker.len() - 1)
            .max()
            .unwrap_or(0);
        let mut scan = Vec::with_capacity(self.stream_terminal_tail.len() + bytes.len());
        scan.extend_from_slice(&self.stream_terminal_tail);
        scan.extend_from_slice(bytes);
        if MARKERS
            .iter()
            .any(|marker| scan.windows(marker.len()).any(|window| window == *marker))
        {
            self.stream_terminal_seen = true;
            self.stream_terminal_tail.clear();
            return;
        }
        let start = scan.len().saturating_sub(tail_limit);
        self.stream_terminal_tail.clear();
        self.stream_terminal_tail.extend_from_slice(&scan[start..]);
    }

    fn response_complete(&self) -> bool {
        self.expected_response_bytes
            .is_some_and(|expected| expected == self.observed_response_bytes)
            || self.stream_terminal_seen
    }

    fn disable_capture(&mut self, oversized: bool) {
        self.seed = None;
        self.response.clear();
        self.memory.clear();
        if oversized {
            self.counters.oversized.fetch_add(1, Ordering::Relaxed);
        } else {
            self.counters.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn finish(&mut self, interrupted: bool) {
        if self.finished {
            return;
        }
        self.finished = true;
        if interrupted {
            self.counters.interrupted.fetch_add(1, Ordering::Relaxed);
            self.seed = None;
            self.response.clear();
            self.memory.clear();
            return;
        }
        self.counters.completed.fetch_add(1, Ordering::Relaxed);
        let Some(seed) = self.seed.take() else {
            return;
        };
        let job = CaptureJob {
            scope_id: seed.scope_id,
            profile: seed.profile,
            trajectory_id: seed.trajectory_id,
            exchange_id: seed.exchange_id,
            started_at: seed.started_at,
            completed_at: utc_now(),
            endpoint: seed.endpoint,
            streaming: seed.streaming,
            request_method: seed.request_method,
            request_path: seed.request_path,
            request_headers: seed.request_headers,
            request: seed.request,
            response_status: seed.response_status,
            response_headers: seed.response_headers,
            response: std::mem::take(&mut self.response),
            route_id: seed.route_id,
            route_target: seed.route_target,
            candidate_artifact_sha256: seed.candidate_artifact_sha256,
            candidate_binding_sha256: seed.candidate_binding_sha256,
            fallback_reason: seed.fallback_reason,
            ttft_ms: self.first_byte.map(duration_ms),
            total_ms: duration_ms(self.started.elapsed()),
            _memory: std::mem::take(&mut self.memory),
        };
        match self.capture_tx.try_send(job) {
            Ok(()) => {
                self.counters.enqueued.fetch_add(1, Ordering::Relaxed);
            }
            Err(_) => {
                self.counters.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

#[derive(Serialize)]
struct ExchangeEnvelope {
    schema_version: &'static str,
    scope_id: Uuid,
    profile: Profile,
    #[serde(skip_serializing_if = "Option::is_none")]
    trajectory_id: Option<Uuid>,
    exchange_id: Uuid,
    started_at: String,
    completed_at: String,
    endpoint: &'static str,
    streaming: bool,
    request: RequestCapture,
    response: ResponseCapture,
    route: RouteCapture,
    timing: TimingCapture,
    complete: bool,
}

#[derive(Serialize)]
struct RequestCapture {
    method: String,
    path: String,
    headers: BTreeMap<String, String>,
    body_base64: String,
    byte_len: usize,
    sha256: String,
}

#[derive(Serialize)]
struct ResponseCapture {
    status: u16,
    headers: BTreeMap<String, String>,
    body_base64: String,
    byte_len: usize,
    sha256: String,
}

#[derive(Serialize)]
struct RouteCapture {
    route_id: Option<Uuid>,
    target: &'static str,
    candidate_artifact_sha256: Option<String>,
    candidate_binding_sha256: Option<String>,
    fallback_reason: Option<String>,
}

#[derive(Serialize)]
struct TimingCapture {
    ttft_ms: Option<u64>,
    total_ms: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    let listen: SocketAddr = env::var("MILK_LISTEN")
        .unwrap_or_else(|_| DEFAULT_LISTEN.to_owned())
        .parse()
        .context("MILK_LISTEN must be a socket address")?;
    let baseline = ProtocolBindings {
        chat_completions: required_upstream(
            "MILK_BASELINE_CHAT_BASE_URL",
            "MILK_BASELINE_CHAT_API_KEY",
        )?,
        responses: required_upstream(
            "MILK_BASELINE_RESPONSES_BASE_URL",
            "MILK_BASELINE_RESPONSES_API_KEY",
        )?,
    };
    let candidate_a = optional_candidate_binding()?;
    let keys = Arc::new(parse_keys(&required_env("MILK_KEYS_JSON")?)?);
    let max_request_bytes = env_usize(
        "MILK_MAX_REQUEST_BYTES",
        DEFAULT_MAX_REQUEST_BYTES,
        1,
        64 * 1024 * 1024,
    )?;
    let max_response_bytes = env_usize(
        "MILK_MAX_RESPONSE_BYTES",
        DEFAULT_MAX_RESPONSE_BYTES,
        1,
        256 * 1024 * 1024,
    )?;
    let capture_memory_bytes = env_usize(
        "MILK_CAPTURE_MEMORY_BYTES",
        DEFAULT_CAPTURE_MEMORY_BYTES,
        max_request_bytes.min(DEFAULT_CAPTURE_MEMORY_BYTES),
        u32::MAX as usize,
    )?;
    let capture_queue = env_usize("MILK_CAPTURE_QUEUE", DEFAULT_CAPTURE_QUEUE, 1, 4096)?;

    let counters = Arc::new(Counters::default());
    counters.writer_alive.store(true, Ordering::Release);
    let store = Store::from_environment().await?;
    let routes = RouteManager::new(
        store.clone(),
        parse_verifying_key(&required_env("MILK_ROUTE_VERIFY_KEY")?)?,
        Duration::from_secs(env_usize("MILK_ROUTE_POLL_SECONDS", 30, 1, 3600)? as u64),
        route_revisions(&keys),
    );
    let (capture_tx, capture_rx) = mpsc::channel(capture_queue);
    tokio::spawn(capture_writer(
        capture_rx,
        store.clone(),
        Arc::clone(&counters),
    ));

    let client = reqwest::Client::builder()
        .redirect(Policy::none())
        .connect_timeout(Duration::from_secs(10))
        .pool_idle_timeout(Duration::from_secs(90))
        .build()?;
    let state = AppState {
        client,
        baseline,
        candidate_a,
        keys,
        routes,
        store,
        capture_tx,
        capture_memory: Arc::new(Semaphore::new(capture_memory_bytes)),
        max_request_bytes,
        max_response_bytes,
        candidate_header_timeout: Duration::from_secs(env_usize(
            "MILK_CANDIDATE_HEADER_TIMEOUT_SECONDS",
            30,
            1,
            300,
        )? as u64),
        candidate_first_byte_timeout: Duration::from_secs(env_usize(
            "MILK_CANDIDATE_FIRST_BYTE_TIMEOUT_SECONDS",
            120,
            1,
            600,
        )? as u64),
        counters,
    };
    let app = Router::new()
        .route("/", get(status_page))
        .route("/status", get(status_page))
        .route("/healthz", get(health))
        .route("/api/status", get(api_status))
        .route("/v1/chat/completions", post(proxy))
        .route("/v1/responses", post(proxy))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(listen).await?;
    eprintln!("milk-parlor listening on {listen}");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown())
        .await?;
    Ok(())
}

async fn shutdown() {
    let _ = tokio::signal::ctrl_c().await;
}

async fn status_page() -> Html<&'static str> {
    Html(STATUS_HTML)
}

async fn health(State(state): State<AppState>) -> impl IntoResponse {
    let counters = &state.counters;
    let candidate = state.candidate_a.as_ref();
    Json(serde_json::json!({
        "status": "ok",
        "protocols": {
            "chat_completions": true,
            "responses": true,
            "candidate_chat_completions": candidate.is_some_and(|value| value.chat_completions.is_some()),
            "candidate_responses": candidate.is_some_and(|value| value.responses.is_some())
        },
        "capture": {
            "writer_alive": counters.writer_alive.load(Ordering::Acquire),
            "observed": counters.observed.load(Ordering::Relaxed),
            "completed": counters.completed.load(Ordering::Relaxed),
            "enqueued": counters.enqueued.load(Ordering::Relaxed),
            "persisted": counters.persisted.load(Ordering::Relaxed),
            "dropped": counters.dropped.load(Ordering::Relaxed),
            "oversized": counters.oversized.load(Ordering::Relaxed),
            "interrupted": counters.interrupted.load(Ordering::Relaxed),
            "storage_failed": counters.storage_failed.load(Ordering::Relaxed)
        }
    }))
}

async fn api_status(State(state): State<AppState>, headers: HeaderMap) -> Response {
    let Some(key) = authenticate(&headers, &state.keys) else {
        return unauthorized();
    };
    let object_key = format!("milk/v2/scopes/{}/status/current.json", key.scope_id);
    match state.store.get(&object_key).await {
        Ok(Some(value)) if value.len() <= MAX_GET_BYTES => {
            if serde_json::from_slice::<serde_json::Value>(&value).is_err() {
                return gateway_error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "invalid_status",
                    "The stored status is invalid.",
                );
            }
            let mut response = Response::new(Body::from(value));
            response.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            );
            response
                .headers_mut()
                .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
            response
        }
        Ok(None) => (
            StatusCode::OK,
            [(header::CACHE_CONTROL, "no-store")],
            Json(serde_json::json!({
                "schema_version": "milk.status.v2",
                "scope_id": key.scope_id,
                "profile": key.profile,
                "state": "waiting",
                "next_action": "capture"
            })),
        )
            .into_response(),
        Ok(Some(_)) => gateway_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "invalid_status",
            "The stored status is too large.",
        ),
        Err(error) => {
            eprintln!("status read failed for {}: {error:#}", key.scope_id);
            gateway_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "status_unavailable",
                "Status is temporarily unavailable.",
            )
        }
    }
}

async fn proxy(State(state): State<AppState>, request: Request) -> Response {
    let Some(operator) = authenticate(request.headers(), &state.keys).cloned() else {
        return unauthorized();
    };
    state.counters.observed.fetch_add(1, Ordering::Relaxed);

    let started = Instant::now();
    let started_at = utc_now();
    let exchange_id = Uuid::now_v7();
    let protocol = match request.uri().path() {
        "/v1/chat/completions" => Protocol::ChatCompletions,
        "/v1/responses" => Protocol::Responses,
        _ => {
            return gateway_error(
                StatusCode::NOT_FOUND,
                "unsupported_endpoint",
                "The requested inference endpoint is not supported.",
            );
        }
    };
    let endpoint = protocol.as_str();
    let method = request.method().clone();
    let path_and_query = request
        .uri()
        .path_and_query()
        .map_or_else(|| request.uri().path().to_owned(), ToString::to_string);
    let request_path = request.uri().path().to_owned();
    let trajectory_id = match trajectory_id(request.headers()) {
        Ok(value) => value,
        Err(error) => {
            eprintln!("request header rejection: {error:#}");
            return gateway_error(
                StatusCode::BAD_REQUEST,
                "invalid_headers",
                "The request headers are invalid.",
            );
        }
    };
    let request_headers = safe_request_headers(request.headers());
    let forwarded_headers = match upstream_request_headers(request.headers()) {
        Ok(headers) => headers,
        Err(error) => {
            eprintln!("request header rejection: {error:#}");
            return gateway_error(
                StatusCode::BAD_REQUEST,
                "invalid_headers",
                "The request headers are invalid.",
            );
        }
    };
    let request_body = match to_bytes(request.into_body(), state.max_request_bytes).await {
        Ok(body) => body,
        Err(_) => {
            state.counters.oversized.fetch_add(1, Ordering::Relaxed);
            return gateway_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "request_too_large",
                "The request exceeds the gateway memory bound.",
            );
        }
    };
    let streaming = serde_json::from_slice::<serde_json::Value>(&request_body)
        .ok()
        .and_then(|value| value.get("stream").and_then(serde_json::Value::as_bool))
        .unwrap_or(false);

    let route = state.routes.choose(
        operator.scope_id,
        operator.profile == Profile::Production,
        exchange_id,
        protocol,
    );
    let mut route_target = route.target;
    let mut fallback_reason = None;
    let mut first_chunk = None;
    let mut expected_response_bytes = None;
    let upstream = if route.target == Target::CandidateA {
        let configured_candidate = state.candidate_a.as_ref();
        let candidate = configured_candidate
            .and_then(|candidate| candidate.get(protocol).map(|binding| (candidate, binding)))
            .filter(|(candidate, binding)| {
                route.candidate_artifact_sha256.as_deref()
                    == Some(candidate.artifact_sha256.as_ref())
                    && route.candidate_binding_sha256.as_deref()
                        == Some(binding.binding_sha256.as_ref())
            });
        let attempt = match (state.candidate_a.as_ref(), candidate) {
            (None, _) => Err((
                "candidate_unconfigured".to_owned(),
                anyhow!("candidate binding is not configured"),
            )),
            (Some(_), None) => Err((
                "candidate_identity_mismatch".to_owned(),
                anyhow!("signed candidate protocol binding does not match the configured binding"),
            )),
            (_, Some((_, candidate))) => {
                send_candidate(
                    &state,
                    candidate,
                    &method,
                    &path_and_query,
                    &forwarded_headers,
                    &request_body,
                )
                .await
            }
        };
        match attempt {
            Ok((response, chunk, length)) => {
                first_chunk = Some(chunk);
                expected_response_bytes = length;
                response
            }
            Err((reason, error)) => {
                eprintln!("candidate request failed for {exchange_id}: {error:#}");
                fallback_reason = Some(reason);
                route_target = Target::Baseline;
                match send_upstream(
                    &state,
                    state.baseline.get(protocol),
                    &method,
                    &path_and_query,
                    &forwarded_headers,
                    &request_body,
                )
                .await
                {
                    Ok(response) => response,
                    Err(error) => return upstream_failure(&state, exchange_id, error),
                }
            }
        }
    } else {
        match send_upstream(
            &state,
            state.baseline.get(protocol),
            &method,
            &path_and_query,
            &forwarded_headers,
            &request_body,
        )
        .await
        {
            Ok(response) => response,
            Err(error) => return upstream_failure(&state, exchange_id, error),
        }
    };

    let status = upstream.status();
    let expected_response_bytes = if first_chunk.is_some() {
        expected_response_bytes
    } else {
        upstream
            .content_length()
            .and_then(|length| usize::try_from(length).ok())
    };
    let response_headers_capture = safe_response_headers(upstream.headers());
    let response_headers = match downstream_response_headers(upstream.headers()) {
        Ok(headers) => headers,
        Err(error) => {
            state.counters.interrupted.fetch_add(1, Ordering::Relaxed);
            eprintln!("upstream headers rejected for {exchange_id}: {error:#}");
            return gateway_error(
                StatusCode::BAD_GATEWAY,
                "invalid_upstream_headers",
                "The upstream returned invalid headers.",
            );
        }
    };

    let mut memory = Vec::new();
    let capture_request = match u32::try_from(request_body.len()) {
        Ok(bytes) => match Arc::clone(&state.capture_memory).try_acquire_many_owned(bytes) {
            Ok(permit) => {
                memory.push(permit);
                Some(request_body)
            }
            Err(_) => {
                state.counters.dropped.fetch_add(1, Ordering::Relaxed);
                None
            }
        },
        Err(_) => None,
    };

    let seed = capture_request.map(|request| CaptureSeed {
        scope_id: operator.scope_id,
        profile: operator.profile,
        trajectory_id,
        exchange_id,
        started_at,
        endpoint,
        streaming,
        request_method: method.to_string(),
        request_path,
        request_headers,
        request,
        response_status: status.as_u16(),
        response_headers: response_headers_capture,
        route_id: route.route_id,
        route_target: route_target.as_str(),
        candidate_artifact_sha256: route.candidate_artifact_sha256,
        candidate_binding_sha256: route.candidate_binding_sha256,
        fallback_reason,
    });
    let first_byte = first_chunk.as_ref().map(|_| started.elapsed());
    let body = UpstreamBody {
        first: first_chunk,
        inner: Box::pin(upstream.bytes_stream()),
        recorder: ResponseRecorder {
            seed,
            response: Vec::new(),
            memory,
            capture_tx: state.capture_tx.clone(),
            capture_memory: Arc::clone(&state.capture_memory),
            counters: Arc::clone(&state.counters),
            started,
            first_byte,
            max_response_bytes: state.max_response_bytes,
            exchange_id,
            expected_response_bytes,
            observed_response_bytes: 0,
            stream_terminal_seen: false,
            stream_terminal_tail: Vec::new(),
            finished: false,
        },
    };
    let mut response = Response::new(Body::from_stream(body));
    *response.status_mut() = status;
    *response.headers_mut() = response_headers;
    response
}

async fn capture_writer(
    mut receiver: mpsc::Receiver<CaptureJob>,
    store: Store,
    counters: Arc<Counters>,
) {
    while let Some(job) = receiver.recv().await {
        let scope_id = job.scope_id;
        let exchange_id = job.exchange_id;
        let key = format!("milk/v2/scopes/{scope_id}/c/{exchange_id}.json.zst");
        let encoded = tokio::task::spawn_blocking(move || encode_capture(job)).await;
        let result = match encoded {
            Ok(Ok(value)) => store.create(&key, &value).await,
            Ok(Err(error)) => Err(error),
            Err(error) => Err(anyhow!(error).context("capture encoder panicked")),
        };
        match result {
            Ok(()) => {
                counters.persisted.fetch_add(1, Ordering::Relaxed);
            }
            Err(error) => {
                counters.storage_failed.fetch_add(1, Ordering::Relaxed);
                eprintln!("capture persistence failed for {key}: {error:#}");
            }
        }
    }
    counters.writer_alive.store(false, Ordering::Release);
}

fn encode_capture(job: CaptureJob) -> Result<Vec<u8>> {
    let envelope = ExchangeEnvelope {
        schema_version: EXCHANGE_SCHEMA,
        scope_id: job.scope_id,
        profile: job.profile,
        trajectory_id: job.trajectory_id,
        exchange_id: job.exchange_id,
        started_at: job.started_at,
        completed_at: job.completed_at,
        endpoint: job.endpoint,
        streaming: job.streaming,
        request: RequestCapture {
            method: job.request_method,
            path: job.request_path,
            headers: job.request_headers,
            body_base64: base64::engine::general_purpose::STANDARD.encode(&job.request),
            byte_len: job.request.len(),
            sha256: sha256_hex(&job.request),
        },
        response: ResponseCapture {
            status: job.response_status,
            headers: job.response_headers,
            body_base64: base64::engine::general_purpose::STANDARD.encode(&job.response),
            byte_len: job.response.len(),
            sha256: sha256_hex(&job.response),
        },
        route: RouteCapture {
            route_id: job.route_id,
            target: job.route_target,
            candidate_artifact_sha256: job.candidate_artifact_sha256,
            candidate_binding_sha256: job.candidate_binding_sha256,
            fallback_reason: job.fallback_reason,
        },
        timing: TimingCapture {
            ttft_ms: job.ttft_ms,
            total_ms: job.total_ms,
        },
        complete: true,
    };
    let json = serde_json::to_vec(&envelope)?;
    zstd::encode_all(Cursor::new(json), 3).context("compress capture")
}

fn parse_keys(raw: &str) -> Result<Vec<OperatorKey>> {
    let configured: BTreeMap<String, KeyBinding> =
        serde_json::from_str(raw).context("MILK_KEYS_JSON is invalid")?;
    if configured.is_empty() || configured.len() > 4096 {
        bail!("MILK_KEYS_JSON must contain 1..=4096 keys");
    }
    let keys: Vec<OperatorKey> = configured
        .into_iter()
        .map(|(digest, binding)| {
            if binding.scope_id.is_nil() {
                bail!("MILK_KEYS_JSON contains a nil scope_id");
            }
            match (binding.profile, binding.route_revision) {
                (Profile::Production, Some(1..)) | (Profile::Mechanics, None) => {}
                (Profile::Production, _) => {
                    bail!("each production scope needs a nonzero route_revision")
                }
                (Profile::Mechanics, Some(_)) => {
                    bail!("mechanics scopes must omit route_revision")
                }
            }
            Ok(OperatorKey {
                digest: decode_sha256(&digest)?,
                scope_id: binding.scope_id,
                profile: binding.profile,
                route_revision: binding.route_revision,
            })
        })
        .collect::<Result<_>>()?;
    let mut scopes = BTreeMap::new();
    for key in &keys {
        if scopes
            .insert(key.scope_id, (key.profile, key.route_revision))
            .is_some_and(|binding| binding != (key.profile, key.route_revision))
        {
            bail!("MILK_KEYS_JSON assigns one scope conflicting bindings");
        }
    }
    Ok(keys)
}

fn route_revisions(keys: &[OperatorKey]) -> HashMap<Uuid, u64> {
    keys.iter()
        .filter_map(|key| key.route_revision.map(|revision| (key.scope_id, revision)))
        .collect()
}

fn authenticate<'a>(headers: &HeaderMap, keys: &'a [OperatorKey]) -> Option<&'a OperatorKey> {
    let mut values = headers.get_all(header::AUTHORIZATION).iter();
    let raw = values.next()?.to_str().ok()?;
    if values.next().is_some() {
        return None;
    }
    let (scheme, token) = raw.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") || !(16..=4096).contains(&token.len()) {
        return None;
    }
    let actual: [u8; 32] = Sha256::digest(token.as_bytes()).into();
    let mut matched = None;
    for key in keys {
        if key.digest.ct_eq(&actual).unwrap_u8() == 1 {
            matched = Some(key);
        }
    }
    matched
}

fn trajectory_id(headers: &HeaderMap) -> Result<Option<Uuid>> {
    let mut values = headers.get_all("x-milk-trajectory-id").iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        bail!("x-milk-trajectory-id must appear once");
    }
    let value = value
        .to_str()
        .context("x-milk-trajectory-id must be ASCII")?;
    let value = Uuid::parse_str(value).context("x-milk-trajectory-id must be a UUID")?;
    if value.is_nil() {
        bail!("x-milk-trajectory-id must be nonzero");
    }
    Ok(Some(value))
}

async fn send_upstream(
    state: &AppState,
    binding: &UpstreamBinding,
    method: &axum::http::Method,
    path_and_query: &str,
    headers: &reqwest::header::HeaderMap,
    body: &Bytes,
) -> Result<reqwest::Response> {
    let url = upstream_url(&binding.base_url, path_and_query)?;
    state
        .client
        .request(method.clone(), url)
        .headers(headers.clone())
        .bearer_auth(binding.api_key.as_ref())
        .body(body.clone())
        .send()
        .await
        .context("upstream request failed")
}

async fn send_candidate(
    state: &AppState,
    candidate: &CandidateProtocolBinding,
    method: &axum::http::Method,
    path_and_query: &str,
    headers: &reqwest::header::HeaderMap,
    body: &Bytes,
) -> std::result::Result<(reqwest::Response, Bytes, Option<usize>), (String, anyhow::Error)> {
    let mut response = match tokio::time::timeout(
        state.candidate_header_timeout,
        send_upstream(
            state,
            &candidate.upstream,
            method,
            path_and_query,
            headers,
            body,
        ),
    )
    .await
    {
        Ok(Ok(response)) => response,
        Ok(Err(error)) => return Err(("candidate_unavailable".to_owned(), error)),
        Err(_) => {
            return Err((
                "candidate_header_timeout".to_owned(),
                anyhow!("candidate response header timeout"),
            ));
        }
    };
    if retry_candidate_status(response.status()) {
        let status = response.status().as_u16();
        return Err((
            format!("candidate_status_{status}"),
            anyhow!("candidate returned retryable status {status}"),
        ));
    }
    if let Err(error) = downstream_response_headers(response.headers()) {
        return Err(("candidate_invalid_headers".to_owned(), error));
    }
    let content_length = response
        .content_length()
        .and_then(|length| usize::try_from(length).ok());
    match tokio::time::timeout(
        state.candidate_first_byte_timeout,
        first_response_chunk(&mut response),
    )
    .await
    {
        Ok(Ok(chunk)) => Ok((response, chunk, content_length)),
        Ok(Err(error)) => Err(("candidate_body_unavailable".to_owned(), error)),
        Err(_) => Err((
            "candidate_first_byte_timeout".to_owned(),
            anyhow!("candidate first response byte timeout"),
        )),
    }
}

async fn first_response_chunk(response: &mut reqwest::Response) -> Result<Bytes> {
    loop {
        match response.chunk().await.context("candidate body failed")? {
            Some(chunk) if !chunk.is_empty() => return Ok(chunk),
            Some(_) => {}
            None => bail!("candidate ended before its first response byte"),
        }
    }
}

fn retry_candidate_status(status: reqwest::StatusCode) -> bool {
    status == reqwest::StatusCode::TOO_MANY_REQUESTS || status.is_server_error()
}

fn upstream_failure(state: &AppState, exchange_id: Uuid, error: anyhow::Error) -> Response {
    state.counters.interrupted.fetch_add(1, Ordering::Relaxed);
    eprintln!("upstream request failed for {exchange_id}: {error:#}");
    gateway_error(
        StatusCode::BAD_GATEWAY,
        "upstream_unavailable",
        "The upstream request failed.",
    )
}

fn upstream_request_headers(headers: &HeaderMap) -> Result<reqwest::header::HeaderMap> {
    let mut forwarded = reqwest::header::HeaderMap::new();
    for (name, value) in headers {
        let lower = name.as_str().to_ascii_lowercase();
        if is_hop_by_hop(&lower)
            || matches!(
                lower.as_str(),
                "authorization"
                    | "host"
                    | "content-length"
                    | "openai-organization"
                    | "openai-project"
            )
            || lower.starts_with("x-milk-")
            || !allowed_request_header(&lower)
        {
            continue;
        }
        forwarded.append(
            reqwest::header::HeaderName::from_bytes(name.as_str().as_bytes())?,
            reqwest::header::HeaderValue::from_bytes(value.as_bytes())?,
        );
    }
    forwarded.insert(
        reqwest::header::ACCEPT_ENCODING,
        reqwest::header::HeaderValue::from_static("identity"),
    );
    Ok(forwarded)
}

fn downstream_response_headers(headers: &reqwest::header::HeaderMap) -> Result<HeaderMap> {
    let mut forwarded = HeaderMap::new();
    for (name, value) in headers {
        let lower = name.as_str().to_ascii_lowercase();
        if is_hop_by_hop(&lower) || lower.starts_with("x-milk-") || !allowed_response_header(&lower)
        {
            continue;
        }
        forwarded.append(
            HeaderName::from_bytes(name.as_str().as_bytes())?,
            HeaderValue::from_bytes(value.as_bytes())?,
        );
    }
    Ok(forwarded)
}

fn allowed_request_header(name: &str) -> bool {
    matches!(
        name,
        "accept"
            | "content-encoding"
            | "content-type"
            | "openai-beta"
            | "user-agent"
            | "x-client-request-id"
    ) || name.starts_with("x-stainless-")
}

fn allowed_response_header(name: &str) -> bool {
    matches!(
        name,
        "cache-control"
            | "content-encoding"
            | "content-length"
            | "content-type"
            | "etag"
            | "retry-after"
            | "vary"
            | "www-authenticate"
            | "x-request-id"
            | "x-should-retry"
    ) || name.starts_with("openai-")
        || name.starts_with("x-openai-")
        || name.starts_with("x-ratelimit-")
}

fn is_hop_by_hop(name: &str) -> bool {
    matches!(
        name,
        "connection"
            | "keep-alive"
            | "proxy-authenticate"
            | "proxy-authorization"
            | "proxy-connection"
            | "te"
            | "trailer"
            | "transfer-encoding"
            | "upgrade"
    )
}

fn safe_request_headers(headers: &HeaderMap) -> BTreeMap<String, String> {
    safe_headers(
        headers,
        &["content-type", "content-encoding", "accept", "openai-beta"],
    )
}

fn safe_response_headers(headers: &reqwest::header::HeaderMap) -> BTreeMap<String, String> {
    let mut selected = BTreeMap::new();
    for name in [
        "content-type",
        "content-encoding",
        "x-request-id",
        "openai-request-id",
    ] {
        if let Some(value) = headers.get(name).and_then(|value| value.to_str().ok()) {
            selected.insert(name.to_owned(), value.to_owned());
        }
    }
    selected
}

fn safe_headers(headers: &HeaderMap, names: &[&str]) -> BTreeMap<String, String> {
    let mut selected = BTreeMap::new();
    for &name in names {
        if let Some(value) = headers.get(name).and_then(|value| value.to_str().ok()) {
            selected.insert(name.to_owned(), value.to_owned());
        }
    }
    selected
}

fn parse_upstream(name: &str, raw: &str) -> Result<Url> {
    let url = Url::parse(raw).with_context(|| format!("{name} is not a URL"))?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || url.query().is_some()
        || url.fragment().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        bail!("{name} must be an HTTP(S) base URL without credentials, query, or fragment");
    }
    if url.path().trim_end_matches('/').ends_with("/v1") {
        bail!("{name} must stop before /v1 because Parlor appends the client endpoint path");
    }
    Ok(url)
}

fn required_upstream(base_name: &str, key_name: &str) -> Result<UpstreamBinding> {
    Ok(UpstreamBinding {
        base_url: parse_upstream(base_name, &required_env(base_name)?)?,
        api_key: required_env(key_name)?.into(),
    })
}

fn optional_upstream(base_name: &str, key_name: &str) -> Result<Option<UpstreamBinding>> {
    match (optional_env(base_name)?, optional_env(key_name)?) {
        (None, None) => Ok(None),
        (Some(base_url), Some(api_key)) => Ok(Some(UpstreamBinding {
            base_url: parse_upstream(base_name, &base_url)?,
            api_key: api_key.into(),
        })),
        _ => bail!("{base_name} and {key_name} must be set together"),
    }
}

fn candidate_protocol_binding(
    protocol: Protocol,
    upstream: Option<UpstreamBinding>,
    artifact_sha256: &str,
) -> Result<Option<CandidateProtocolBinding>> {
    upstream
        .map(|upstream| {
            let base_url = upstream.base_url.as_str().trim_end_matches('/');
            let binding_sha256 = sha256_hex(&serde_json::to_vec(&CandidateIdentity {
                artifact_sha256,
                base_url,
                protocol,
            })?);
            Ok(CandidateProtocolBinding {
                upstream,
                binding_sha256: binding_sha256.into(),
            })
        })
        .transpose()
}

fn optional_candidate_binding() -> Result<Option<CandidateBinding>> {
    let artifact_sha256 = optional_env("MILK_CANDIDATE_A_ARTIFACT_SHA256")?;
    let chat = optional_upstream(
        "MILK_CANDIDATE_A_CHAT_BASE_URL",
        "MILK_CANDIDATE_A_CHAT_API_KEY",
    )?;
    let responses = optional_upstream(
        "MILK_CANDIDATE_A_RESPONSES_BASE_URL",
        "MILK_CANDIDATE_A_RESPONSES_API_KEY",
    )?;
    match artifact_sha256 {
        None if chat.is_none() && responses.is_none() => Ok(None),
        None => bail!("candidate protocol bindings require MILK_CANDIDATE_A_ARTIFACT_SHA256"),
        Some(artifact_sha256) if chat.is_some() || responses.is_some() => {
            require_sha256("MILK_CANDIDATE_A_ARTIFACT_SHA256", &artifact_sha256)?;
            Ok(Some(CandidateBinding {
                chat_completions: candidate_protocol_binding(
                    Protocol::ChatCompletions,
                    chat,
                    &artifact_sha256,
                )?,
                responses: candidate_protocol_binding(
                    Protocol::Responses,
                    responses,
                    &artifact_sha256,
                )?,
                artifact_sha256: artifact_sha256.into(),
            }))
        }
        Some(_) => bail!("a candidate artifact requires at least one complete protocol binding"),
    }
}

fn upstream_url(base: &Url, path_and_query: &str) -> Result<Url> {
    let raw = format!("{}{}", base.as_str().trim_end_matches('/'), path_and_query);
    let url = Url::parse(&raw)?;
    if url.host_str() != base.host_str() || url.scheme() != base.scheme() {
        bail!("upstream request escaped configured authority");
    }
    Ok(url)
}

fn required_env(name: &str) -> Result<String> {
    let value = env::var(name).with_context(|| format!("{name} is required"))?;
    if value.is_empty() {
        bail!("{name} must not be empty");
    }
    Ok(value)
}

fn optional_env(name: &str) -> Result<Option<String>> {
    match env::var(name) {
        Ok(value) if !value.is_empty() => Ok(Some(value)),
        Ok(_) => bail!("{name} must not be empty"),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(error).with_context(|| format!("read {name}")),
    }
}

fn require_sha256(name: &str, value: &str) -> Result<()> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        bail!("{name} must be 64 lowercase hexadecimal characters");
    }
    Ok(())
}

fn env_usize(name: &str, default: usize, minimum: usize, maximum: usize) -> Result<usize> {
    let value = match env::var(name) {
        Ok(value) => value
            .parse::<usize>()
            .with_context(|| format!("{name} must be an integer"))?,
        Err(env::VarError::NotPresent) => default,
        Err(error) => return Err(error).with_context(|| format!("read {name}")),
    };
    if !(minimum..=maximum).contains(&value) {
        bail!("{name} must be in {minimum}..={maximum}");
    }
    Ok(value)
}

fn decode_sha256(value: &str) -> Result<[u8; 32]> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        bail!("API-key digest must be 64 lowercase hexadecimal characters");
    }
    let mut output = [0_u8; 32];
    for (index, byte) in output.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)?;
    }
    Ok(output)
}

fn sha256_hex(value: &[u8]) -> String {
    let digest = Sha256::digest(value);
    let mut output = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write;
        let _ = write!(output, "{byte:02x}");
    }
    output
}

fn utc_now() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .expect("UTC timestamp is representable")
}

fn duration_ms(duration: Duration) -> u64 {
    duration.as_millis().min(u64::MAX as u128) as u64
}

fn unauthorized() -> Response {
    let mut response = gateway_error(
        StatusCode::UNAUTHORIZED,
        "unauthorized",
        "A valid Milk bearer key is required.",
    );
    response
        .headers_mut()
        .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
    response
}

fn gateway_error(status: StatusCode, code: &'static str, message: &'static str) -> Response {
    (
        status,
        [(header::CACHE_CONTROL, "no-store")],
        Json(serde_json::json!({
            "error": {"type": "gateway_error", "code": code, "message": message}
        })),
    )
        .into_response()
}

const STATUS_HTML: &str = r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Milk Parlor</title>
<style>
:root{color-scheme:dark;--ink:#f3efe4;--dim:#8f918b;--line:#383a37;--panel:#181a18;--pink:#ff6b9e;--mint:#7ce6c2;--gold:#ffd166;--red:#ff6b6b}*{box-sizing:border-box}body{margin:0;background:#0c0d0c;color:var(--ink);font:14px/1.45 ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace}main{width:min(1120px,calc(100% - 32px));margin:0 auto;padding:48px 0 72px}header{display:flex;align-items:center;gap:16px;margin-bottom:38px}.mark{width:48px;height:48px;image-rendering:pixelated}.brand{font-size:12px;letter-spacing:.16em;text-transform:uppercase;color:var(--dim)}h1,h2,h3,p{margin:0}h1{font-size:22px;letter-spacing:-.04em}.cursor{display:inline-block;width:8px;height:18px;background:var(--pink);vertical-align:-3px;animation:blink 1s steps(1) infinite}@keyframes blink{50%{opacity:0}}.auth{display:grid;grid-template-columns:1fr auto;gap:8px;max-width:620px;margin-top:18px}input,button{font:inherit;border:1px solid var(--line);border-radius:0;padding:12px 14px}input{min-width:0;background:var(--panel);color:var(--ink);outline:none}input:focus{border-color:var(--pink)}button{background:var(--ink);color:#0c0d0c;cursor:pointer;font-weight:700}button:hover{background:var(--pink)}.error{min-height:22px;margin-top:10px;color:var(--red)}[hidden]{display:none!important}.scope{display:flex;justify-content:space-between;gap:20px;align-items:flex-end;border-bottom:1px solid var(--line);padding-bottom:18px}.eyebrow{color:var(--dim);font-size:11px;letter-spacing:.13em;text-transform:uppercase}.scope h2{font-size:clamp(15px,2vw,22px);overflow-wrap:anywhere}.badge{border:1px solid var(--line);padding:5px 8px;color:var(--mint);text-transform:uppercase;font-size:11px}.meter{margin:28px 0 34px}.meter-line{display:flex;justify-content:space-between;margin-bottom:9px}.meter-line strong{font-size:18px}.track{height:8px;background:var(--panel);border:1px solid var(--line)}.fill{height:100%;width:0;background:var(--pink);transition:width .35s ease}.rail{display:grid;grid-template-columns:repeat(9,minmax(0,1fr));border:1px solid var(--line);margin-bottom:28px}.stage{min-height:126px;padding:12px;border-right:1px solid var(--line);position:relative}.stage:last-child{border:0}.stage b{color:var(--dim);font-size:10px}.stage h3{font-size:12px;margin:16px 0 7px}.stage p{font-size:10px;color:var(--dim);overflow-wrap:anywhere}.stage[data-state=done]{background:#14231e}.stage[data-state=done] b,.stage[data-state=done] h3{color:var(--mint)}.stage[data-state=next]{box-shadow:inset 0 -3px var(--gold)}.grid{display:grid;grid-template-columns:repeat(4,1fr);border:1px solid var(--line)}.metric{padding:16px;border-right:1px solid var(--line);border-bottom:1px solid var(--line);min-height:112px}.metric:nth-child(4n){border-right:0}.metric:nth-last-child(-n+4){border-bottom:0}.metric span{display:block;color:var(--dim);font-size:10px;text-transform:uppercase;letter-spacing:.1em}.metric strong{display:block;margin:10px 0 6px;font-size:22px}.metric small{color:var(--dim)}.lists{display:grid;grid-template-columns:repeat(3,1fr);gap:1px;background:var(--line);border:1px solid var(--line);margin-top:28px}.list{background:var(--panel);padding:16px}.list h3{font-size:11px;color:var(--dim);text-transform:uppercase;letter-spacing:.1em;margin-bottom:12px}.list p{white-space:pre-line}details{margin-top:28px;border-top:1px solid var(--line);padding-top:14px;color:var(--dim)}summary{cursor:pointer}pre{white-space:pre-wrap;overflow-wrap:anywhere;color:var(--ink);font-size:11px}.foot{margin-top:28px;color:var(--dim);font-size:11px}@media(max-width:850px){.rail{grid-template-columns:repeat(3,1fr)}.stage{border-bottom:1px solid var(--line)}.grid{grid-template-columns:repeat(2,1fr)}.metric:nth-child(2n){border-right:0}.metric:nth-last-child(-n+4){border-bottom:1px solid var(--line)}.metric:nth-last-child(-n+2){border-bottom:0}.lists{grid-template-columns:1fr}}@media(max-width:520px){main{padding-top:28px}.auth{grid-template-columns:1fr}.scope{align-items:flex-start;flex-direction:column}.rail{grid-template-columns:1fr 1fr}.grid{grid-template-columns:1fr}.metric{border-right:0!important;border-bottom:1px solid var(--line)!important}.metric:last-child{border-bottom:0!important}}@media(prefers-reduced-motion:reduce){.cursor{animation:none}.fill{transition:none}}
</style>
</head>
<body>
<main>
<header><img class="mark" alt="" width="48" height="48" src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACAAAAAgCAYAAABzenr0AAAAAXNSR0IArs4c6QAAAERlWElmTU0AKgAAAAgAAYdpAAQAAAABAAAAGgAAAAAAA6ABAAMAAAABAAEAAKACAAQAAAABAAAAIKADAAQAAAABAAAAIAAAAACshmLzAAAD9UlEQVRYCcVXXWgcVRT+7uzf7Cb7l90W8mPR2rTdJqWRllBtKlEjYmmEQh6s9KH4UNC+5EVEn8TXQu1DEZ9ERFDBglQfBBHB9KGIigoubdpi2qYN6ZLNZnY3uzM7c8czs9mdSfcnd9tIDty95557fr4558zcuwyASWPLSNqyyGuBvZ0AYC2UHyeFwgBOxvfi4sBEUwjf5GZxMfNHfe+6moVm8vq6HSMMwHJixrqgTx9b50+ay2DqM2AqtrsuH7n+Oe5qSn3djukMAOOoxDik7dsgJRMAYzAO7oRxNAUYOoz0TQQv/NwuXsNeRwAsa1PJw6DBcys2AG8ohOj+PeCFAsxwEBr+RwBM8kBK9ICvKDCzywCnOkcjCMoyQMOUZALQGQln4FzfOPjkQXgHn7YjmBUdZj4Pj+S8ySwsI3DiWcxc0nDs1iWky0sbovGQxgfttE7F9+Hd3sPY/eYk+BtjgFR9GZlHAgsG4enuRojKYBPJ/EdSkAMynkpnoVJfXCtn27mHA7+FWkpOYGJ4BOXRftDjttByif1eyGdewfgzoxgMxF0bzVnhEnBFgfp3uu7FP7QHzGMl8PFICIBqmphXywCNXskLK6xx975dDmalP5m0UVS4CUU3bJ6RjQgJAfiTq3itRAGJrgT7MEAg9PnqWopFgeF99t5cUcX5aws2f7ZUseeNfoQAuJ28p2URXGudt31hjK5tXp5fxmyBstQhdQxgxnCC7KJMFLRVDC+v4q9cEfcFn9qNUaCtgcES8P49t1mV/7iiYDqfwSc3Fx8puOVFCMB2HXiVPnyidFQtIsarzbiRjRAAt5Mzi8A71H+RNv4P6CWEN/s47tdMXJhjmFoy0U3Bv04yKA99BgZ1DWeLGRuv0ylu+I28cBP2UBlOP3De7T46dT46NIYX4tvAbt3Gt3IMOlrdmRoD1yTCAGoGtfmHNIFJz0CKhuB/cS9OljpokpoTmoV6QI/JUJ6js0CAPAO9CEyMgYW7BLQB4Qxw2YuV55+oOw3/ugCpTHVZI19qF90V6PDp8HwQBmCV1/Q5CSsOJcDo2++LRNA1MgQp0g34fTU8wrPjsYXJgl5AmS6evt/m12kYVBa9JwgjEaL7IT25K3j50x9xg07OB5XVdTbNFlbbOq3dTINkl3eewJGufnAKWpymS4mL/NEoel8atyX8Tga54x8izzW8PvcdrharB5NLvYEVAmBZ2f8LdrwMHgnYTopvHQYCXtQAVK6koX75C7SZf3Bo9gv8q+YagjUTCAOIe2QM+KjORF89OYlkaod9H5C8Psh0RTfuLeH81e/xU+EOfl9dRJk7DdoscE0mDKBmsNnzhk242QEf9rflAP4DLPkuGVgeVLEAAAAASUVORK5CYII="><div><div class="brand">milkinfrastructure.com</div><h1>Milk Parlor <span class="cursor" aria-hidden="true"></span></h1></div></header>
<section id="login"><p>Read the object-memory loop for one operator key.</p><form class="auth" id="f"><input id="k" type="password" autocomplete="off" placeholder="Milk operator key" aria-label="Milk operator key" required><button>open parlor</button></form><p class="error" id="error" role="alert"></p></section>
<section id="app" hidden>
<div class="scope"><div><div class="eyebrow">scope</div><h2 id="scope"></h2></div><span class="badge" id="profile"></span></div>
<div class="meter"><div class="meter-line"><strong id="count"></strong><span id="threshold"></span></div><div class="track"><div class="fill" id="fill"></div></div></div>
<section class="rail" id="rail">
<article class="stage" data-key="traffic"><b>01</b><h3>traffic</h3><p></p></article><article class="stage" data-key="summary"><b>02</b><h3>summary</h3><p></p></article><article class="stage" data-key="readiness"><b>03</b><h3>ready</h3><p></p></article><article class="stage" data-key="eval"><b>04</b><h3>evals</h3><p></p></article><article class="stage" data-key="dataset"><b>05</b><h3>dataset</h3><p></p></article><article class="stage" data-key="training"><b>06</b><h3>student</h3><p></p></article><article class="stage" data-key="evaluation"><b>07</b><h3>winner</h3><p></p></article><article class="stage" data-key="candidate"><b>08</b><h3>candidate</h3><p></p></article><article class="stage" data-key="proposal"><b>09</b><h3>route proposal</h3><p></p></article>
</section>
<section id="metrics" hidden><div class="grid"><div class="metric"><span>parsed</span><strong id="parse"></strong><small>95% confidence</small></div><div class="metric"><span>successful</span><strong id="success"></strong><small>captured responses</small></div><div class="metric"><span>p95 latency</span><strong id="latency"></strong><small>end to end</small></div><div class="metric"><span>p50 throughput</span><strong id="throughput"></strong><small>tokens / second bucket</small></div><div class="metric"><span>unique content</span><strong id="unique"></strong><small>deduplicated</small></div><div class="metric"><span>max concurrency</span><strong id="concurrency"></strong><small>observed</small></div><div class="metric"><span>classified</span><strong id="classified"></strong><small>semantic sample</small></div><div class="metric"><span>abstained</span><strong id="abstained"></strong><small>classifier uncertainty</small></div></div><div class="lists"><div class="list"><h3>traffic</h3><p id="traffic"></p></div><div class="list"><h3>intent</h3><p id="intent"></p></div><div class="list"><h3>outcome</h3><p id="outcome"></p></div></div></section>
<details><summary>raw status</summary><pre id="raw"></pre></details><p class="foot" id="foot"></p>
</section>
</main>
<script>
const $=id=>document.getElementById(id),pct=n=>(n/100).toFixed(1)+'%',pairs=o=>Object.entries(o||{}).sort((a,b)=>b[1]-a[1]).slice(0,4).map(([k,v])=>k+'  '+v).join('\n')||'waiting',short=v=>typeof v==='string'?v.slice(0,8):'waiting';
function stage(key,done,note,next){const e=document.querySelector('[data-key='+key+']');e.dataset.state=done?'done':next?'next':'wait';e.querySelector('p').textContent=note}
function render(d){$('login').hidden=true;$('app').hidden=false;$('scope').textContent=d.scope_id;$('profile').textContent=d.profile;const next=d.next_summary_threshold,g=d.eval_generation;if(g){$('count').textContent=g.completed_case_count+' / '+g.target_case_count+' evals';$('threshold').textContent=d.capture_count+' exchanges captured';$('fill').style.width=Math.min(100,g.completed_case_count/g.target_case_count*100)+'%'}else{$('count').textContent=d.capture_count+' complete exchanges';$('threshold').textContent=next?'next traffic checkpoint '+next:'traffic checkpoint current';$('fill').style.width=(next?Math.min(100,d.capture_count/next*100):100)+'%'}const steps=[['traffic',d.capture_count>0,d.capture_count+' captured'],['summary',!!d.summary,d.summary?'checkpoint '+short(d.summary.uuid):'waiting for '+next],['readiness',!!d.readiness,d.readiness?(d.readiness.ready?'ready':'not ready'):'not evaluated'],['eval',!!d.eval&&!g,g?g.completed_case_count+' / '+g.target_case_count+' cases':d.eval?d.eval.case_count+' cases':'waiting'],['dataset',!!d.dataset,d.dataset?'revision '+short(d.dataset.uuid):'waiting'],['training',!!d.training,d.training?'model '+short(d.training.uuid):'waiting'],['evaluation',!!d.evaluation,d.evaluation?d.evaluation.winner_branch+' selected':'waiting'],['candidate',!!d.candidate,d.candidate?'endpoint '+short(d.candidate.uuid):'zero'],['proposal',!!d.proposal,d.proposal?'awaiting operator signature':d.next_action]];let marked=false;for(const [key,done,note] of steps){const isNext=!done&&!marked;stage(key,done,note,isNext);if(isNext)marked=true}const m=d.summary_metrics;$('metrics').hidden=!m;if(m){const q=m.quality,s=m.series,c=m.counters,sem=m.semantic;$('parse').textContent=pct(q.parse_basis_points);$('success').textContent=pct(q.success_basis_points);$('latency').textContent=s.total_ms.p95+' ms';$('throughput').textContent=(s.tps_milli.p50/1000).toFixed(1);$('unique').textContent=c.unique_contents;$('concurrency').textContent=c.max_concurrency;$('classified').textContent=sem.classified;$('abstained').textContent=pct(sem.abstain_basis_points);$('traffic').textContent=pairs(m.distributions.model)+'\n\n'+pairs(m.distributions.status);$('intent').textContent=pairs(sem.operation)+'\n\n'+pairs(sem.domain);$('outcome').textContent=pairs(sem.outcome)+'\n\n'+pairs(sem.sentiment)}$('raw').textContent=JSON.stringify(d,null,2);$('foot').textContent='next action: '+d.next_action+' · key held only for this browser session'}
async function load(key){$('error').textContent='';try{const r=await fetch('/api/status',{headers:{authorization:'Bearer '+key}});if(!r.ok)throw Error(r.status===401?'That key was not accepted.':'Status is unavailable.');const d=await r.json();sessionStorage.setItem('milk-key',key);render(d)}catch(e){$('error').textContent=e.message}}
$('f').onsubmit=e=>{e.preventDefault();load($('k').value)};const saved=sessionStorage.getItem('milk-key');if(saved)load(saved);
</script>
</body>
</html>"#;
