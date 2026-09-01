use anyhow::{Context, Result, bail};
use futures_util::StreamExt;
use hmac::{Hmac, Mac};
use reqwest::{Method, StatusCode, redirect::Policy};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    env,
    io::ErrorKind,
    path::{Component, Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use time::OffsetDateTime;
use tokio::{fs, io::AsyncWriteExt};
use url::Url;
use uuid::Uuid;

pub(crate) const MAX_GET_BYTES: usize = 1024 * 1024;

#[derive(Clone)]
pub(crate) enum Store {
    Local(Arc<LocalStore>),
    S3(Arc<S3Store>),
}

pub(crate) struct LocalStore {
    root: PathBuf,
}

pub(crate) struct S3Store {
    client: reqwest::Client,
    endpoint: Url,
    region: String,
    bucket: String,
    path_style: bool,
    access_key_id: Arc<str>,
    secret_access_key: Arc<str>,
    session_token: Option<Arc<str>>,
    timeout: Duration,
}

impl Store {
    pub(crate) async fn from_environment() -> Result<Self> {
        match env::var("MILK_STORE_KIND") {
            Ok(value) if value == "s3" => Ok(Self::S3(Arc::new(S3Store::from_environment()?))),
            Ok(value) if value == "local" => {
                let root = PathBuf::from(
                    env::var("MILK_STORE_ROOT").unwrap_or_else(|_| "./data".to_owned()),
                );
                fs::create_dir_all(&root).await?;
                Ok(Self::Local(Arc::new(LocalStore { root })))
            }
            Err(env::VarError::NotPresent) => {
                let root = PathBuf::from(
                    env::var("MILK_STORE_ROOT").unwrap_or_else(|_| "./data".to_owned()),
                );
                fs::create_dir_all(&root).await?;
                Ok(Self::Local(Arc::new(LocalStore { root })))
            }
            Ok(value) => bail!("MILK_STORE_KIND must be local or s3, not {value}"),
            Err(error) => Err(error).context("read MILK_STORE_KIND"),
        }
    }

    pub(crate) async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        match self {
            Self::Local(store) => store.get(key).await,
            Self::S3(store) => store.get(key).await,
        }
    }

    pub(crate) async fn create(&self, key: &str, value: &[u8]) -> Result<()> {
        match self {
            Self::Local(store) => store.create(key, value).await,
            Self::S3(store) => store.create(key, value).await,
        }
    }
}

impl LocalStore {
    fn path(&self, key: &str) -> Result<PathBuf> {
        validate_key(key)?;
        Ok(self.root.join(key))
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let path = self.path(key)?;
        let metadata = match fs::metadata(&path).await {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error).context("inspect local object"),
        };
        if metadata.len() > MAX_GET_BYTES as u64 {
            bail!("local object exceeds read bound");
        }
        fs::read(path).await.map(Some).context("read local object")
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

impl S3Store {
    fn from_environment() -> Result<Self> {
        let endpoint = Url::parse(&required("MILK_STORE_ENDPOINT")?)
            .context("MILK_STORE_ENDPOINT is not a URL")?;
        if !matches!(endpoint.scheme(), "http" | "https")
            || endpoint.host_str().is_none()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || !matches!(endpoint.path(), "" | "/")
        {
            bail!("MILK_STORE_ENDPOINT must be an HTTP(S) origin without credentials");
        }
        let region = bounded_identity("MILK_STORE_REGION", &required("MILK_STORE_REGION")?)?;
        let bucket = bounded_identity("MILK_STORE_BUCKET", &required("MILK_STORE_BUCKET")?)?;
        let access_key_id = secret("MILK_STORE_ACCESS_KEY_ID")?;
        let secret_access_key = secret("MILK_STORE_SECRET_ACCESS_KEY")?;
        let session_token = env::var("MILK_STORE_SESSION_TOKEN")
            .ok()
            .filter(|value| !value.is_empty())
            .map(|value| validate_secret("MILK_STORE_SESSION_TOKEN", value))
            .transpose()?;
        let path_style = env_bool("MILK_STORE_PATH_STYLE", true)?;
        let timeout_seconds = env_u64("MILK_STORE_TIMEOUT_SECONDS", 30, 1, 120)?;
        let client = reqwest::Client::builder()
            .redirect(Policy::none())
            .connect_timeout(Duration::from_secs(timeout_seconds))
            .build()?;
        Ok(Self {
            client,
            endpoint,
            region,
            bucket,
            path_style,
            access_key_id,
            secret_access_key,
            session_token,
            timeout: Duration::from_secs(timeout_seconds),
        })
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let request = self.signed_request(Method::GET, key, &[], &[])?;
        let response = request.send().await.context("S3 GET failed")?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if response.status() != StatusCode::OK {
            bail!("S3 GET returned {}", response.status());
        }
        if response
            .content_length()
            .is_some_and(|length| length > MAX_GET_BYTES as u64)
        {
            bail!("S3 object exceeds read bound");
        }
        let mut value = Vec::with_capacity(
            response
                .content_length()
                .unwrap_or_default()
                .min(MAX_GET_BYTES as u64) as usize,
        );
        let mut body = response.bytes_stream();
        while let Some(chunk) = body.next().await {
            let chunk = chunk.context("read S3 object")?;
            if value
                .len()
                .checked_add(chunk.len())
                .is_none_or(|length| length > MAX_GET_BYTES)
            {
                bail!("S3 object exceeds read bound");
            }
            value.extend_from_slice(&chunk);
        }
        Ok(Some(value))
    }

    async fn create(&self, key: &str, value: &[u8]) -> Result<()> {
        let response = self
            .signed_request(
                Method::PUT,
                key,
                value,
                &[("content-type", "application/zstd"), ("if-none-match", "*")],
            )?
            .send()
            .await
            .context("S3 create failed")?;
        if matches!(response.status(), StatusCode::OK | StatusCode::CREATED) {
            return Ok(());
        }
        if response.status() == StatusCode::PRECONDITION_FAILED {
            bail!("S3 object already exists");
        }
        bail!("S3 create returned {}", response.status())
    }

    fn signed_request(
        &self,
        method: Method,
        key: &str,
        body: &[u8],
        extra_headers: &[(&str, &str)],
    ) -> Result<reqwest::RequestBuilder> {
        validate_key(key)?;
        let (url, host, canonical_uri) = self.object_url(key)?;
        let now = OffsetDateTime::now_utc();
        let date = format!(
            "{:04}{:02}{:02}",
            now.year(),
            u8::from(now.month()),
            now.day()
        );
        let amz_date = format!(
            "{date}T{:02}{:02}{:02}Z",
            now.hour(),
            now.minute(),
            now.second()
        );
        let payload_sha256 = sha256_hex(body);
        let mut signed = BTreeMap::from([
            ("host".to_owned(), host),
            ("x-amz-content-sha256".to_owned(), payload_sha256.clone()),
            ("x-amz-date".to_owned(), amz_date.clone()),
        ]);
        if let Some(token) = &self.session_token {
            signed.insert("x-amz-security-token".to_owned(), token.to_string());
        }
        for (name, value) in extra_headers {
            let name = name.to_ascii_lowercase();
            let value = value.split_whitespace().collect::<Vec<_>>().join(" ");
            if name.is_empty() || value.is_empty() || signed.insert(name, value).is_some() {
                bail!("invalid S3 signed header");
            }
        }
        let canonical_headers = signed
            .iter()
            .map(|(name, value)| format!("{name}:{value}\n"))
            .collect::<String>();
        let signed_headers = signed.keys().cloned().collect::<Vec<_>>().join(";");
        let canonical_request = format!(
            "{}\n{canonical_uri}\n\n{canonical_headers}\n{signed_headers}\n{payload_sha256}",
            method.as_str()
        );
        let scope = format!("{date}/{}/s3/aws4_request", self.region);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
            sha256_hex(canonical_request.as_bytes())
        );
        let date_key = hmac_sha256(
            format!("AWS4{}", self.secret_access_key).as_bytes(),
            date.as_bytes(),
        );
        let region_key = hmac_sha256(&date_key, self.region.as_bytes());
        let service_key = hmac_sha256(&region_key, b"s3");
        let signing_key = hmac_sha256(&service_key, b"aws4_request");
        let signature = hex(&hmac_sha256(&signing_key, string_to_sign.as_bytes()));
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope},SignedHeaders={signed_headers},Signature={signature}",
            self.access_key_id
        );

        let mut request = self
            .client
            .request(method, url)
            .timeout(self.timeout)
            .header("authorization", authorization)
            .body(body.to_vec());
        for (name, value) in signed {
            if name != "host" {
                request = request.header(name, value);
            }
        }
        Ok(request)
    }

    fn object_url(&self, key: &str) -> Result<(Url, String, String)> {
        let endpoint_host = self
            .endpoint
            .host_str()
            .context("S3 endpoint has no host")?;
        let endpoint_port = self.endpoint.port();
        let (host_name, canonical_uri) = if self.path_style {
            (
                endpoint_host.to_owned(),
                format!("/{}/{}", aws_encode(&self.bucket), aws_encode_path(key)),
            )
        } else {
            (
                format!("{}.{}", self.bucket, endpoint_host),
                format!("/{}", aws_encode_path(key)),
            )
        };
        let host =
            endpoint_port.map_or_else(|| host_name.clone(), |port| format!("{host_name}:{port}"));
        let url = Url::parse(&format!(
            "{}://{host}{canonical_uri}",
            self.endpoint.scheme()
        ))?;
        Ok((url, host, canonical_uri))
    }
}

fn validate_key(key: &str) -> Result<()> {
    let path = Path::new(key);
    if key.is_empty()
        || path.is_absolute()
        || path
            .components()
            .any(|part| !matches!(part, Component::Normal(_)))
    {
        bail!("invalid object key");
    }
    Ok(())
}

fn required(name: &str) -> Result<String> {
    let value = env::var(name).with_context(|| format!("{name} is required"))?;
    if value.is_empty() {
        bail!("{name} must not be empty");
    }
    Ok(value)
}

fn bounded_identity(name: &str, value: &str) -> Result<String> {
    if value.len() > 255
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        bail!("{name} is invalid");
    }
    Ok(value.to_owned())
}

fn secret(name: &str) -> Result<Arc<str>> {
    validate_secret(name, required(name)?)
}

fn validate_secret(name: &str, value: String) -> Result<Arc<str>> {
    if value.len() > 4096 || value.chars().any(char::is_whitespace) {
        bail!("{name} is invalid");
    }
    Ok(value.into())
}

fn env_bool(name: &str, default: bool) -> Result<bool> {
    match env::var(name) {
        Ok(value) if value.eq_ignore_ascii_case("true") || value == "1" => Ok(true),
        Ok(value) if value.eq_ignore_ascii_case("false") || value == "0" => Ok(false),
        Ok(_) => bail!("{name} must be true or false"),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error).with_context(|| format!("read {name}")),
    }
}

fn env_u64(name: &str, default: u64, minimum: u64, maximum: u64) -> Result<u64> {
    let value = match env::var(name) {
        Ok(value) => value
            .parse::<u64>()
            .with_context(|| format!("{name} must be an integer"))?,
        Err(env::VarError::NotPresent) => default,
        Err(error) => return Err(error).with_context(|| format!("read {name}")),
    };
    if !(minimum..=maximum).contains(&value) {
        bail!("{name} must be in {minimum}..={maximum}");
    }
    Ok(value)
}

fn aws_encode(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            encoded.push(byte as char);
        } else {
            encoded.push('%');
            encoded.push(char::from(b"0123456789ABCDEF"[(byte >> 4) as usize]));
            encoded.push(char::from(b"0123456789ABCDEF"[(byte & 15) as usize]));
        }
    }
    encoded
}

fn aws_encode_path(value: &str) -> String {
    value
        .split('/')
        .map(aws_encode)
        .collect::<Vec<_>>()
        .join("/")
}

fn hmac_sha256(key: &[u8], value: &[u8]) -> [u8; 32] {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(value);
    mac.finalize().into_bytes().into()
}

fn sha256_hex(value: &[u8]) -> String {
    hex(&Sha256::digest(value))
}

fn hex(value: &[u8]) -> String {
    let mut output = String::with_capacity(value.len() * 2);
    for byte in value {
        use std::fmt::Write;
        let _ = write!(output, "{byte:02x}");
    }
    output
}
