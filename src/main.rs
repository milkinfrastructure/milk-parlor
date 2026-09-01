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
    collections::BTreeMap,
    env,
    io::{Cursor, ErrorKind},
    net::SocketAddr,
    path::{Component, Path, PathBuf},
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
use tokio::{
    fs,
    io::AsyncWriteExt,
    sync::{OwnedSemaphorePermit, Semaphore, mpsc},
};
use url::Url;
use uuid::Uuid;

const EXCHANGE_SCHEMA: &str = "milk.exchange.v2";
const DEFAULT_LISTEN: &str = "0.0.0.0:8080";
const DEFAULT_MAX_REQUEST_BYTES: usize = 8 * 1024 * 1024;
const DEFAULT_MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
const DEFAULT_CAPTURE_MEMORY_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_CAPTURE_QUEUE: usize = 64;
const MAX_STATUS_BYTES: usize = 1024 * 1024;

#[derive(Clone)]
struct AppState {
    client: reqwest::Client,
    upstream: Url,
    upstream_api_key: Arc<str>,
    keys: Arc<Vec<OperatorKey>>,
    store: Store,
    capture_tx: mpsc::Sender<CaptureJob>,
    capture_memory: Arc<Semaphore>,
    max_request_bytes: usize,
    max_response_bytes: usize,
    counters: Arc<Counters>,
}

#[derive(Clone)]
struct OperatorKey {
    digest: [u8; 32],
    scope_id: Uuid,
    profile: Profile,
}

#[derive(Clone, Copy, Deserialize, Serialize)]
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

#[derive(Clone)]
enum Store {
    Local(Arc<LocalStore>),
}

struct LocalStore {
    root: PathBuf,
}

impl Store {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        match self {
            Self::Local(store) => store.get(key).await,
        }
    }

    async fn create(&self, key: &str, value: &[u8]) -> Result<()> {
        match self {
            Self::Local(store) => store.create(key, value).await,
        }
    }
}

impl LocalStore {
    fn path(&self, key: &str) -> Result<PathBuf> {
        let path = Path::new(key);
        if path.is_absolute()
            || path
                .components()
                .any(|part| !matches!(part, Component::Normal(_)))
        {
            bail!("invalid object key");
        }
        Ok(self.root.join(path))
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        match fs::read(self.path(key)?).await {
            Ok(value) => Ok(Some(value)),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error).context("read local object"),
        }
    }

    async fn create(&self, key: &str, value: &[u8]) -> Result<()> {
        let path = self.path(key)?;
        let parent = path.parent().context("object key has no parent")?;
        fs::create_dir_all(parent).await?;
        let temporary = parent.join(format!(".{}.tmp", Uuid::now_v7()));
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .await?;
        if let Err(error) = async {
            file.write_all(value).await?;
            file.sync_data().await?;
            fs::hard_link(&temporary, &path).await?;
            Result::<(), std::io::Error>::Ok(())
        }
        .await
        {
            let _ = fs::remove_file(&temporary).await;
            return Err(error).context("create local object");
        }
        fs::remove_file(&temporary).await?;
        Ok(())
    }
}

struct CaptureJob {
    scope_id: Uuid,
    profile: Profile,
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
    ttft_ms: Option<u64>,
    total_ms: u64,
    _memory: Vec<OwnedSemaphorePermit>,
}

struct CaptureSeed {
    scope_id: Uuid,
    profile: Profile,
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
    inner: Pin<Box<dyn Stream<Item = reqwest::Result<Bytes>> + Send>>,
    recorder: ResponseRecorder,
}

impl Stream for UpstreamBody {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        context: &mut TaskContext<'_>,
    ) -> Poll<Option<Self::Item>> {
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
    route_id: Option<String>,
    target: &'static str,
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
    let upstream = parse_upstream(&required_env("MILK_UPSTREAM_BASE_URL")?)?;
    let upstream_api_key: Arc<str> = required_env("MILK_UPSTREAM_API_KEY")?.into();
    let keys = Arc::new(parse_keys(&required_env("MILK_KEYS_JSON")?)?);
    let store_root =
        PathBuf::from(env::var("MILK_STORE_ROOT").unwrap_or_else(|_| "./data".to_owned()));
    fs::create_dir_all(&store_root).await?;
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
    let store = Store::Local(Arc::new(LocalStore { root: store_root }));
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
        upstream,
        upstream_api_key,
        keys,
        store,
        capture_tx,
        capture_memory: Arc::new(Semaphore::new(capture_memory_bytes)),
        max_request_bytes,
        max_response_bytes,
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
    Json(serde_json::json!({
        "status": "ok",
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
        Ok(Some(value)) if value.len() <= MAX_STATUS_BYTES => {
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
    let endpoint = match request.uri().path() {
        "/v1/chat/completions" => "chat_completions",
        "/v1/responses" => "responses",
        _ => "other",
    };
    let method = request.method().clone();
    let path_and_query = request
        .uri()
        .path_and_query()
        .map_or_else(|| request.uri().path().to_owned(), ToString::to_string);
    let request_path = request.uri().path().to_owned();
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

    let upstream_url = match upstream_url(&state.upstream, &path_and_query) {
        Ok(url) => url,
        Err(error) => {
            eprintln!("upstream URL rejected: {error:#}");
            return gateway_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "invalid_upstream",
                "The upstream is misconfigured.",
            );
        }
    };
    let upstream = match state
        .client
        .request(method.clone(), upstream_url)
        .headers(forwarded_headers)
        .bearer_auth(state.upstream_api_key.as_ref())
        .body(request_body.clone())
        .send()
        .await
    {
        Ok(response) => response,
        Err(error) => {
            state.counters.interrupted.fetch_add(1, Ordering::Relaxed);
            eprintln!("upstream request failed for {exchange_id}: {error}");
            return gateway_error(
                StatusCode::BAD_GATEWAY,
                "upstream_unavailable",
                "The upstream request failed.",
            );
        }
    };

    let status = upstream.status();
    let expected_response_bytes = upstream
        .content_length()
        .and_then(|length| usize::try_from(length).ok());
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
    });
    let body = UpstreamBody {
        inner: Box::pin(upstream.bytes_stream()),
        recorder: ResponseRecorder {
            seed,
            response: Vec::new(),
            memory,
            capture_tx: state.capture_tx.clone(),
            capture_memory: Arc::clone(&state.capture_memory),
            counters: Arc::clone(&state.counters),
            started,
            first_byte: None,
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
            route_id: None,
            target: "baseline",
            fallback_reason: None,
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
    configured
        .into_iter()
        .map(|(digest, binding)| {
            if binding.scope_id.is_nil() {
                bail!("MILK_KEYS_JSON contains a nil scope_id");
            }
            Ok(OperatorKey {
                digest: decode_sha256(&digest)?,
                scope_id: binding.scope_id,
                profile: binding.profile,
            })
        })
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

fn parse_upstream(raw: &str) -> Result<Url> {
    let url = Url::parse(raw).context("MILK_UPSTREAM_BASE_URL is not a URL")?;
    if !matches!(url.scheme(), "http" | "https")
        || url.host_str().is_none()
        || url.query().is_some()
        || url.fragment().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        bail!(
            "MILK_UPSTREAM_BASE_URL must be an HTTP(S) base URL without credentials, query, or fragment"
        );
    }
    Ok(url)
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
body{font:16px/1.5 system-ui,sans-serif;max-width:760px;margin:8vh auto;padding:0 20px;color:#171717;background:#faf8f2}form{display:flex;gap:8px}input,button{font:inherit;padding:10px 12px}input{flex:1}button{background:#171717;color:white;border:0}pre{white-space:pre-wrap;background:white;border:1px solid #ddd;padding:16px;min-height:120px}</style>
</head>
<body>
<h1>Milk Parlor</h1>
<p>Enter an operator key to read this scope's current Milk state.</p>
<form id="f"><input id="k" type="password" autocomplete="off" placeholder="Milk operator key" required><button>Read status</button></form>
<pre id="o">Waiting for a key.</pre>
<script>
f.onsubmit=async e=>{e.preventDefault();o.textContent='Loading…';try{const r=await fetch('/api/status',{headers:{authorization:'Bearer '+k.value}});const j=await r.json();o.textContent=JSON.stringify(j,null,2)}catch(e){o.textContent=String(e)}};
</script>
</body>
</html>"#;
