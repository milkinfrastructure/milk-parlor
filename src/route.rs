use anyhow::{Context, Result, bail};
use base64::Engine;
use ed25519_dalek::{Signature, Verifier, VerifyingKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use time::{OffsetDateTime, UtcOffset, format_description::well_known::Rfc3339};
use uuid::Uuid;

use crate::store::Store;

const ROUTE_SCHEMA: &str = "milk.route.v2";
const POINTER_SCHEMA: &str = "milk.route-pointer.v2";

#[derive(Clone)]
pub(crate) struct RouteManager {
    store: Store,
    verifying_key: VerifyingKey,
    poll_every: Duration,
    expected_revisions: Arc<HashMap<Uuid, u64>>,
    scopes: Arc<Mutex<HashMap<Uuid, ScopeState>>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Target {
    Baseline,
    CandidateA,
}

impl Target {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Baseline => "baseline",
            Self::CandidateA => "candidate-a",
        }
    }
}

pub(crate) struct RouteChoice {
    pub(crate) route_id: Option<Uuid>,
    pub(crate) target: Target,
    pub(crate) candidate_artifact_sha256: Option<String>,
    pub(crate) candidate_binding_sha256: Option<String>,
}

#[derive(Default)]
struct ScopeState {
    last_poll: Option<Instant>,
    refreshing: bool,
    route: Option<VerifiedRoute>,
}

#[derive(Clone)]
struct VerifiedRoute {
    route_id: Uuid,
    revision: u64,
    route_sha256: String,
    valid_from: OffsetDateTime,
    expires_at: OffsetDateTime,
    candidate: Option<Binding>,
    candidate_artifact_sha256: Option<String>,
    candidate_binding_sha256: Option<String>,
    candidate_basis_points: u16,
}

#[derive(Clone, Copy, Deserialize, Serialize)]
enum Binding {
    #[serde(rename = "baseline")]
    Baseline,
    #[serde(rename = "candidate-a")]
    CandidateA,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SignedRoute {
    schema_version: String,
    scope_id: Uuid,
    route_id: Uuid,
    revision: u64,
    valid_from: String,
    expires_at: String,
    baseline: Binding,
    candidate: Option<Binding>,
    candidate_artifact_sha256: Option<String>,
    candidate_binding_sha256: Option<String>,
    candidate_basis_points: u16,
    signature: String,
}

#[derive(Serialize)]
struct UnsignedRoute<'a> {
    schema_version: &'a str,
    scope_id: Uuid,
    route_id: Uuid,
    revision: u64,
    valid_from: &'a str,
    expires_at: &'a str,
    baseline: Binding,
    candidate: Option<Binding>,
    candidate_artifact_sha256: Option<&'a str>,
    candidate_binding_sha256: Option<&'a str>,
    candidate_basis_points: u16,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct SignedPointer {
    schema_version: String,
    scope_id: Uuid,
    route_id: Uuid,
    revision: u64,
    route_sha256: String,
    published_at: String,
    signature: String,
}

#[derive(Serialize)]
struct UnsignedPointer<'a> {
    schema_version: &'a str,
    scope_id: Uuid,
    route_id: Uuid,
    revision: u64,
    route_sha256: &'a str,
    published_at: &'a str,
}

impl RouteManager {
    pub(crate) fn new(
        store: Store,
        verifying_key: VerifyingKey,
        poll_every: Duration,
        expected_revisions: HashMap<Uuid, u64>,
    ) -> Self {
        Self {
            store,
            verifying_key,
            poll_every,
            expected_revisions: Arc::new(expected_revisions),
            scopes: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(crate) fn choose(
        &self,
        scope_id: Uuid,
        production: bool,
        exchange_id: Uuid,
    ) -> RouteChoice {
        if !production {
            return RouteChoice {
                route_id: None,
                target: Target::Baseline,
                candidate_artifact_sha256: None,
                candidate_binding_sha256: None,
            };
        }
        let (route, refresh) = {
            let mut scopes = self.scopes.lock().expect("route cache lock poisoned");
            let state = scopes.entry(scope_id).or_default();
            let due = state
                .last_poll
                .is_none_or(|last_poll| last_poll.elapsed() >= self.poll_every);
            let refresh = due && !state.refreshing;
            if refresh {
                state.last_poll = Some(Instant::now());
                state.refreshing = true;
            }
            (state.route.clone(), refresh)
        };
        if refresh {
            let manager = self.clone();
            tokio::spawn(async move { manager.refresh(scope_id).await });
        }
        let now = OffsetDateTime::now_utc();
        let Some(route) = route.filter(|route| route.valid_from <= now && now < route.expires_at)
        else {
            return RouteChoice {
                route_id: None,
                target: Target::Baseline,
                candidate_artifact_sha256: None,
                candidate_binding_sha256: None,
            };
        };
        let target = if matches!(route.candidate, Some(Binding::CandidateA))
            && assigned_to_candidate(route.route_id, exchange_id, route.candidate_basis_points)
        {
            Target::CandidateA
        } else {
            Target::Baseline
        };
        RouteChoice {
            route_id: Some(route.route_id),
            target,
            candidate_artifact_sha256: route.candidate_artifact_sha256,
            candidate_binding_sha256: route.candidate_binding_sha256,
        }
    }

    async fn refresh(&self, scope_id: Uuid) {
        let current = self
            .scopes
            .lock()
            .expect("route cache lock poisoned")
            .get(&scope_id)
            .and_then(|state| state.route.clone());
        let result = self.poll(scope_id, current.as_ref()).await;
        let mut scopes = self.scopes.lock().expect("route cache lock poisoned");
        let state = scopes.entry(scope_id).or_default();
        state.refreshing = false;
        match result {
            Ok(Some(route)) => state.route = Some(route),
            Ok(None) => {}
            Err(error) => eprintln!("route poll failed for {scope_id}: {error:#}"),
        }
    }

    async fn poll(
        &self,
        scope_id: Uuid,
        current: Option<&VerifiedRoute>,
    ) -> Result<Option<VerifiedRoute>> {
        let prefix = format!("milk/v2/scopes/{scope_id}/r");
        let Some(pointer_bytes) = self.store.get(&format!("{prefix}/current.json")).await? else {
            return Ok(None);
        };
        let pointer = parse_pointer(&pointer_bytes, &self.verifying_key, scope_id)?;
        let expected_revision = self
            .expected_revisions
            .get(&scope_id)
            .context("production scope has no trusted route revision")?;
        if pointer.revision != *expected_revision {
            bail!(
                "route revision {} does not equal trusted revision {}",
                pointer.revision,
                expected_revision
            );
        }
        if let Some(current) = current {
            if pointer.revision < current.revision
                || (pointer.revision == current.revision && pointer.route_id != current.route_id)
            {
                bail!("route pointer is stale or conflicts at its revision");
            }
            if pointer.revision == current.revision && pointer.route_id == current.route_id {
                if pointer.route_sha256 != current.route_sha256 {
                    bail!("route pointer conflicts at its revision");
                }
                return Ok(None);
            }
        }
        let route_key = format!("{prefix}/{}.json", pointer.route_id);
        let route_bytes = self
            .store
            .get(&route_key)
            .await?
            .context("route pointer refers to a missing object")?;
        if sha256_hex(&route_bytes) != pointer.route_sha256 {
            bail!("route object digest does not match pointer");
        }
        let route = parse_route(&route_bytes, &self.verifying_key, scope_id)?;
        if route.route_id != pointer.route_id || route.revision != pointer.revision {
            bail!("route object identity does not match pointer");
        }
        let now = OffsetDateTime::now_utc();
        if route.valid_from > now || route.expires_at <= now {
            bail!("route object is not currently valid");
        }
        Ok(Some(route))
    }
}

pub(crate) fn parse_verifying_key(raw: &str) -> Result<VerifyingKey> {
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(raw)
        .context("MILK_ROUTE_VERIFY_KEY must be standard base64")?;
    let bytes: [u8; 32] = decoded
        .try_into()
        .map_err(|_| anyhow::anyhow!("MILK_ROUTE_VERIFY_KEY must encode 32 bytes"))?;
    VerifyingKey::from_bytes(&bytes).context("MILK_ROUTE_VERIFY_KEY is invalid")
}

fn parse_pointer(
    bytes: &[u8],
    verifying_key: &VerifyingKey,
    scope_id: Uuid,
) -> Result<SignedPointer> {
    let pointer: SignedPointer = serde_json::from_slice(bytes).context("invalid route pointer")?;
    require_canonical(bytes, &pointer, "route pointer")?;
    if pointer.schema_version != POINTER_SCHEMA
        || pointer.scope_id != scope_id
        || pointer.scope_id.is_nil()
        || pointer.route_id.is_nil()
        || pointer.revision == 0
        || !is_sha256(&pointer.route_sha256)
    {
        bail!("route pointer fields are invalid");
    }
    let _ = canonical_time(&pointer.published_at)?;
    let unsigned = UnsignedPointer {
        schema_version: &pointer.schema_version,
        scope_id: pointer.scope_id,
        route_id: pointer.route_id,
        revision: pointer.revision,
        route_sha256: &pointer.route_sha256,
        published_at: &pointer.published_at,
    };
    verify_signature(
        verifying_key,
        &serde_json::to_vec(&unsigned)?,
        &pointer.signature,
    )?;
    Ok(pointer)
}

fn parse_route(
    bytes: &[u8],
    verifying_key: &VerifyingKey,
    scope_id: Uuid,
) -> Result<VerifiedRoute> {
    let route: SignedRoute = serde_json::from_slice(bytes).context("invalid route object")?;
    require_canonical(bytes, &route, "route object")?;
    if route.schema_version != ROUTE_SCHEMA
        || route.scope_id != scope_id
        || route.scope_id.is_nil()
        || route.route_id.is_nil()
        || route.revision == 0
        || !matches!(route.baseline, Binding::Baseline)
    {
        bail!("route object fields are invalid");
    }
    match (
        route.candidate,
        route.candidate_artifact_sha256.as_deref(),
        route.candidate_binding_sha256.as_deref(),
        route.candidate_basis_points,
    ) {
        (None, None, None, 0) => {}
        (Some(Binding::CandidateA), Some(artifact), Some(binding), 1..=10_000)
            if is_sha256(artifact) && is_sha256(binding) => {}
        _ => bail!("route candidate and basis points are inconsistent"),
    }
    let valid_from = canonical_time(&route.valid_from)?;
    let expires_at = canonical_time(&route.expires_at)?;
    if valid_from >= expires_at {
        bail!("route validity interval is empty");
    }
    let unsigned = UnsignedRoute {
        schema_version: &route.schema_version,
        scope_id: route.scope_id,
        route_id: route.route_id,
        revision: route.revision,
        valid_from: &route.valid_from,
        expires_at: &route.expires_at,
        baseline: route.baseline,
        candidate: route.candidate,
        candidate_artifact_sha256: route.candidate_artifact_sha256.as_deref(),
        candidate_binding_sha256: route.candidate_binding_sha256.as_deref(),
        candidate_basis_points: route.candidate_basis_points,
    };
    verify_signature(
        verifying_key,
        &serde_json::to_vec(&unsigned)?,
        &route.signature,
    )?;
    Ok(VerifiedRoute {
        route_id: route.route_id,
        revision: route.revision,
        route_sha256: sha256_hex(bytes),
        valid_from,
        expires_at,
        candidate: route.candidate,
        candidate_artifact_sha256: route.candidate_artifact_sha256,
        candidate_binding_sha256: route.candidate_binding_sha256,
        candidate_basis_points: route.candidate_basis_points,
    })
}

fn require_canonical<T: Serialize>(bytes: &[u8], parsed: &T, name: &str) -> Result<()> {
    if serde_json::to_vec(parsed)? != bytes {
        bail!("{name} is not canonical JSON");
    }
    Ok(())
}

fn canonical_time(raw: &str) -> Result<OffsetDateTime> {
    let value = OffsetDateTime::parse(raw, &Rfc3339).context("route timestamp is invalid")?;
    if value.offset() != UtcOffset::UTC || value.format(&Rfc3339)? != raw {
        bail!("route timestamp is not canonical UTC RFC3339");
    }
    Ok(value)
}

fn verify_signature(key: &VerifyingKey, message: &[u8], encoded: &str) -> Result<()> {
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .context("route signature is not standard base64")?;
    let signature = Signature::from_slice(&decoded).context("route signature must be 64 bytes")?;
    key.verify(message, &signature)
        .context("route signature verification failed")
}

fn assigned_to_candidate(route_id: Uuid, exchange_id: Uuid, basis_points: u16) -> bool {
    let mut hasher = Sha256::new();
    hasher.update(route_id.as_bytes());
    hasher.update(exchange_id.as_bytes());
    let digest = hasher.finalize();
    let value = u64::from_be_bytes(digest[..8].try_into().expect("SHA-256 has eight bytes"));
    let bucket = ((u128::from(value) * 10_000) >> 64) as u16;
    bucket < basis_points
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
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
