use crate::client::build_simple_client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::Mutex;
use wreq::Client;

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct FlareSolverrRequest {
    cmd: String,
    url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    session: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cookies: Option<Vec<Cookie>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    post_data: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_timeout: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Cookie {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Deserialize)]
struct FlareSolverrResponse {
    status: String,
    message: String,
    solution: Option<Solution>,
}

#[derive(Debug, Deserialize)]
struct Solution {
    #[allow(dead_code)]
    url: String,
    #[allow(dead_code)]
    status: u16,
    cookies: Vec<Cookie>,
    #[serde(rename = "userAgent")]
    user_agent: String,
    response: Option<String>,
}

pub struct FlareSolverrClient {
    client: Client,
    base_url: String,
    session_id: Option<String>,
}

// ── Session Pool ──────────────────────────────────────────────────────

/// A single session slot in the pool
struct SessionSlot {
    session_id: String,
    cookies: Vec<Cookie>,
    user_agent: Option<String>,
}

/// Pool of FlareSolverr sessions for parallel request handling
struct FlareSolverrPool {
    /// HTTP client to talk to FlareSolverr API (shared, stateless)
    api_client: Client,
    /// FlareSolverr base URL
    base_url: String,
    /// Session slots, each behind its own Mutex
    slots: Vec<Mutex<SessionSlot>>,
}

/// Global pool
static FLARESOLVERR_POOL: OnceLock<FlareSolverrPool> = OnceLock::new();

/// Round-robin counter for distributing requests across sessions
static POOL_COUNTER: AtomicUsize = AtomicUsize::new(0);

/// When true, skip direct requests entirely and go straight to FlareSolverr
static FLARESOLVERR_ACTIVE: AtomicBool = AtomicBool::new(false);

/// Get the configured number of sessions (from env var, default 3)
pub fn get_session_count() -> usize {
    std::env::var("FLARESOLVERR_SESSIONS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3)
        .max(1) // at least 1
}

/// Initialize the global pool after a successful FlareSolverr login.
/// Creates additional sessions that share the same cookies.
pub async fn init_session_pool(
    flare_client: FlareSolverrClient,
    cookies: Vec<Cookie>,
    domain: &str,
) {
    let session_count = get_session_count();
    let base_url = flare_client.base_url.clone();
    let api_client = flare_client.client;
    let primary_session_id = flare_client.session_id.unwrap_or_else(|| "ygege_1".into());

    let mut slots = Vec::with_capacity(session_count);

    // First slot: the primary session from login (already solved CF)
    slots.push(Mutex::new(SessionSlot {
        session_id: primary_session_id.clone(),
        cookies: cookies.clone(),
        user_agent: None,
    }));

    let warmup_url = format!("https://{}/", domain);

    // Create additional sessions and warm them up
    for i in 1..session_count {
        let session_id = format!("ygege_pool_{}", i);

        // Destroy any existing session (ignore errors)
        let destroy_req = serde_json::json!({
            "cmd": "sessions.destroy",
            "session": &session_id,
        });
        let _ = api_client
            .post(&format!("{}/v1", base_url))
            .json(&destroy_req)
            .send()
            .await;

        // Create new session
        let create_req = serde_json::json!({
            "cmd": "sessions.create",
            "session": &session_id,
        });
        match api_client
            .post(&format!("{}/v1", base_url))
            .json(&create_req)
            .send()
            .await
        {
            Ok(resp) => {
                if let Ok(flare_resp) = resp.json::<FlareSolverrResponse>().await {
                    if flare_resp.status == "ok" {
                        info!("Created FlareSolverr pool session {}/{}: {}", i + 1, session_count, session_id);
                    } else {
                        warn!("Failed to create pool session {}: {}", session_id, flare_resp.message);
                    }
                }
            }
            Err(e) => {
                warn!("Failed to create pool session {}: {}", session_id, e);
            }
        }

        // Warm up: navigate to the site so CF challenge is solved before real requests
        info!("Warming up session {}/{}: solving CF challenge...", i + 1, session_count);
        let warmup_req = FlareSolverrRequest {
            cmd: "request.get".to_string(),
            url: warmup_url.clone(),
            session: Some(session_id.clone()),
            cookies: Some(cookies.clone()),
            post_data: None,
            max_timeout: Some(120000), // give warmup more time
        };
        match api_client
            .post(&format!("{}/v1", base_url))
            .json(&warmup_req)
            .send()
            .await
        {
            Ok(resp) => {
                if let Ok(flare_resp) = resp.json::<FlareSolverrResponse>().await {
                    if flare_resp.status == "ok" {
                        info!("Session {}/{} warmed up successfully", i + 1, session_count);
                    } else {
                        warn!("Session {}/{} warmup failed: {}", i + 1, session_count, flare_resp.message);
                    }
                }
            }
            Err(e) => {
                warn!("Session {}/{} warmup error: {}", i + 1, session_count, e);
            }
        }

        slots.push(Mutex::new(SessionSlot {
            session_id,
            cookies: cookies.clone(),
            user_agent: None,
        }));
    }

    info!("FlareSolverr pool initialized with {} sessions", slots.len());

    let _ = FLARESOLVERR_POOL.set(FlareSolverrPool {
        api_client,
        base_url,
        slots,
    });

    FLARESOLVERR_ACTIVE.store(true, Ordering::SeqCst);
}

/// Check if FlareSolverr mode is active
pub fn is_flaresolverr_active() -> bool {
    FLARESOLVERR_ACTIVE.load(Ordering::SeqCst)
}

/// Pick the next session slot (round-robin)
fn next_slot_index() -> usize {
    let pool = FLARESOLVERR_POOL.get().expect("Pool not initialized");
    POOL_COUNTER.fetch_add(1, Ordering::Relaxed) % pool.slots.len()
}

// ── Public API ────────────────────────────────────────────────────────

/// Download binary data using a direct HTTP client with FlareSolverr cookies.
pub async fn fetch_binary(url: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let pool = FLARESOLVERR_POOL.get().ok_or("FlareSolverr pool not initialized")?;
    let slot_idx = next_slot_index();
    let slot = pool.slots[slot_idx].lock().await;

    // Build a client with Linux Chrome emulation to match FlareSolverr's browser
    let emulation = wreq_util::EmulationOption::builder()
        .emulation(wreq_util::Emulation::Chrome133)
        .emulation_os(wreq_util::EmulationOS::Linux)
        .build();

    let client = wreq::Client::builder()
        .emulation(emulation)
        .cookie_store(true)
        .build()
        .map_err(|e| -> Box<dyn std::error::Error> { format!("{}", e).into() })?;

    let parsed_url = wreq::Url::parse(url)?;
    let domain = parsed_url.domain().unwrap_or("");
    let base_url = wreq::Url::parse(&format!("https://{}/", domain))?;

    for cookie in &slot.cookies {
        let wreq_cookie = wreq::cookie::CookieBuilder::new(&cookie.name, &cookie.value)
            .domain(domain)
            .path("/")
            .http_only(true)
            .secure(true)
            .build();
        client.set_cookie(&base_url, wreq_cookie);
    }

    let mut request = client.get(url);
    if let Some(ref ua) = slot.user_agent {
        request = request.header("User-Agent", ua.as_str());
    }

    drop(slot); // Release lock before network call

    debug!("Downloading binary via direct client (Chrome133/Linux): {}", url);
    let response = request.send().await?;
    let status = response.status();

    if status.is_success() {
        let bytes = response.bytes().await?;
        debug!("Binary download completed: {} bytes", bytes.len());
        return Ok(bytes.to_vec());
    }

    warn!("Direct binary download failed with status {}", status);

    // Fallback: try via FlareSolverr (may corrupt binary data)
    warn!("Attempting FlareSolverr fallback for binary download (may corrupt data)");
    let result = fetch_via_pool(url).await?;
    warn!(
        "FlareSolverr returned {} bytes for binary download. Content preview: {}",
        result.body.len(),
        &result.body[..result.body.len().min(200)]
    );
    Ok(result.body.into_bytes())
}

/// Fetch a page via the FlareSolverr pool (round-robin session selection).
async fn fetch_via_pool(url: &str) -> Result<FetchResult, Box<dyn std::error::Error>> {
    let pool = FLARESOLVERR_POOL.get().ok_or("FlareSolverr pool not initialized")?;
    let slot_idx = next_slot_index();
    let mut slot = pool.slots[slot_idx].lock().await;

    debug!("Fetching via FlareSolverr (session {}/{}): {}", slot_idx + 1, pool.slots.len(), url);
    let start = std::time::Instant::now();

    let request = FlareSolverrRequest {
        cmd: "request.get".to_string(),
        url: url.to_string(),
        session: Some(slot.session_id.clone()),
        cookies: Some(slot.cookies.clone()),
        post_data: None,
        max_timeout: Some(60000),
    };

    let response = pool.api_client
        .post(&format!("{}/v1", pool.base_url))
        .json(&request)
        .send()
        .await?;

    let flare_response: FlareSolverrResponse = response.json().await?;

    if flare_response.status != "ok" {
        return Err(format!("FlareSolverr error: {}", flare_response.message).into());
    }

    let solution = flare_response.solution.ok_or("No solution in response")?;

    // Update cookies
    for new_cookie in &solution.cookies {
        if let Some(existing) = slot.cookies.iter_mut().find(|c| c.name == new_cookie.name) {
            existing.value = new_cookie.value.clone();
        } else {
            slot.cookies.push(new_cookie.clone());
        }
    }

    // Capture User-Agent
    if slot.user_agent.is_none() && !solution.user_agent.is_empty() {
        debug!("Captured FlareSolverr User-Agent: {}", solution.user_agent);
        slot.user_agent = Some(solution.user_agent.clone());
    }

    debug!("FlareSolverr fetch completed in {:?} (session {})", start.elapsed(), slot_idx + 1);

    Ok(FetchResult {
        body: solution.response.unwrap_or_default(),
        status_code: 200,
        used_flaresolverr: true,
    })
}

/// POST a form via the FlareSolverr pool.
async fn post_via_pool(
    url: &str,
    form_data: &HashMap<String, String>,
) -> Result<FetchResult, Box<dyn std::error::Error>> {
    let pool = FLARESOLVERR_POOL.get().ok_or("FlareSolverr pool not initialized")?;
    let slot_idx = next_slot_index();
    let mut slot = pool.slots[slot_idx].lock().await;

    debug!("POSTing via FlareSolverr (session {}): {}", slot_idx + 1, url);
    let start = std::time::Instant::now();

    let post_data = form_data
        .iter()
        .map(|(k, v)| format!("{}={}", urlencoding::encode(k), urlencoding::encode(v)))
        .collect::<Vec<_>>()
        .join("&");

    let request = FlareSolverrRequest {
        cmd: "request.post".to_string(),
        url: url.to_string(),
        session: Some(slot.session_id.clone()),
        cookies: Some(slot.cookies.clone()),
        post_data: Some(post_data),
        max_timeout: Some(60000),
    };

    let response = pool.api_client
        .post(&format!("{}/v1", pool.base_url))
        .json(&request)
        .send()
        .await?;

    let flare_response: FlareSolverrResponse = response.json().await?;

    if flare_response.status != "ok" {
        return Err(format!("FlareSolverr POST error: {}", flare_response.message).into());
    }

    let solution = flare_response.solution.ok_or("No solution in POST response")?;

    // Update cookies
    for new_cookie in &solution.cookies {
        if let Some(existing) = slot.cookies.iter_mut().find(|c| c.name == new_cookie.name) {
            existing.value = new_cookie.value.clone();
        } else {
            slot.cookies.push(new_cookie.clone());
        }
    }

    debug!("FlareSolverr POST completed in {:?} (session {})", start.elapsed(), slot_idx + 1);

    Ok(FetchResult {
        body: solution.response.unwrap_or_default(),
        status_code: 200,
        used_flaresolverr: true,
    })
}

/// Fetch a page: if FlareSolverr mode is active, go directly through the pool.
/// Otherwise try direct GET first, fall back to FlareSolverr if 403.
pub async fn fetch_page(
    client: &Client,
    url: &str,
) -> Result<FetchResult, Box<dyn std::error::Error>> {
    if is_flaresolverr_active() {
        return fetch_via_pool(url).await;
    }

    let response = client.get(url).send().await?;
    let status = response.status();

    if status.is_success() {
        let body = response.text().await?;
        return Ok(FetchResult {
            body,
            status_code: status.as_u16(),
            used_flaresolverr: false,
        });
    }

    if status == 403 {
        match fetch_via_pool(url).await {
            Ok(result) => return Ok(result),
            Err(e) => {
                warn!("FlareSolverr fallback failed: {}", e);
            }
        }
    }

    if status == 307 || status == 302 {
        return Ok(FetchResult {
            body: String::new(),
            status_code: status.as_u16(),
            used_flaresolverr: false,
        });
    }

    Err(format!("Request failed with status: {}", status).into())
}

/// POST a form: if FlareSolverr mode is active, go directly through the pool.
pub async fn post_page(
    client: &Client,
    url: &str,
    form_body: &str,
    content_type: &str,
) -> Result<FetchResult, Box<dyn std::error::Error>> {
    if is_flaresolverr_active() {
        let mut form_data = HashMap::new();
        for pair in form_body.split('&') {
            let mut parts = pair.splitn(2, '=');
            if let (Some(key), Some(value)) = (parts.next(), parts.next()) {
                form_data.insert(key.to_string(), value.to_string());
            }
        }
        return post_via_pool(url, &form_data).await;
    }

    let response = client
        .post(url)
        .body(form_body.to_string())
        .header("Content-Type", content_type)
        .send()
        .await?;
    let status = response.status();
    let body = response.text().await?;

    Ok(FetchResult {
        body,
        status_code: status.as_u16(),
        used_flaresolverr: false,
    })
}

pub struct FetchResult {
    pub body: String,
    pub status_code: u16,
    #[allow(dead_code)]
    pub used_flaresolverr: bool,
}

// ── FlareSolverrClient (used during login only) ───────────────────────

impl FlareSolverrClient {
    pub fn new(base_url: String) -> Result<Self, Box<dyn std::error::Error>> {
        let client = build_simple_client().map_err(|e| -> Box<dyn std::error::Error> {
            format!("{}", e).into()
        })?;
        Ok(Self {
            client,
            base_url,
            session_id: None,
        })
    }

    /// Create a persistent session in FlareSolverr to reuse browser state
    pub async fn create_session(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let session_id = format!("ygege_{}", std::process::id());

        // Try to destroy any existing session first (ignore errors)
        let destroy_req = serde_json::json!({
            "cmd": "sessions.destroy",
            "session": &session_id,
        });
        let _ = self
            .client
            .post(&format!("{}/v1", self.base_url))
            .json(&destroy_req)
            .send()
            .await;

        // Create new session
        let create_req = serde_json::json!({
            "cmd": "sessions.create",
            "session": &session_id,
        });
        let response = self
            .client
            .post(&format!("{}/v1", self.base_url))
            .json(&create_req)
            .send()
            .await?;

        let resp: FlareSolverrResponse = response.json().await?;
        if resp.status == "ok" {
            debug!("Created FlareSolverr session: {}", session_id);
            self.session_id = Some(session_id);
        } else {
            warn!("Failed to create FlareSolverr session: {}, proceeding without session", resp.message);
        }

        Ok(())
    }

    pub async fn get_with_cookies(
        &mut self,
        url: &str,
        cookies: Option<&[Cookie]>,
    ) -> Result<(String, Vec<Cookie>, String), Box<dyn std::error::Error>> {
        let request = FlareSolverrRequest {
            cmd: "request.get".to_string(),
            url: url.to_string(),
            session: self.session_id.clone(),
            cookies: cookies.map(|c| c.to_vec()),
            post_data: None,
            max_timeout: Some(60000),
        };

        let response = self
            .client
            .post(&format!("{}/v1", self.base_url))
            .json(&request)
            .send()
            .await?;

        let flare_response: FlareSolverrResponse = response.json().await?;

        if flare_response.status != "ok" {
            return Err(format!("FlareSolverr error: {}", flare_response.message).into());
        }

        let solution = flare_response.solution.ok_or("No solution in response")?;

        let body = solution.response.unwrap_or_default();
        let user_agent = solution.user_agent;
        Ok((body, solution.cookies, user_agent))
    }

    pub async fn post_form(
        &mut self,
        url: &str,
        form_data: &HashMap<String, String>,
        cookies: Option<&[Cookie]>,
    ) -> Result<Vec<Cookie>, Box<dyn std::error::Error>> {
        let post_data = form_data
            .iter()
            .map(|(k, v)| format!("{}={}", urlencoding::encode(k), urlencoding::encode(v)))
            .collect::<Vec<_>>()
            .join("&");

        let request = FlareSolverrRequest {
            cmd: "request.post".to_string(),
            url: url.to_string(),
            session: self.session_id.clone(),
            cookies: cookies.map(|c| c.to_vec()),
            post_data: Some(post_data),
            max_timeout: Some(60000),
        };

        let response = self
            .client
            .post(&format!("{}/v1", self.base_url))
            .json(&request)
            .send()
            .await?;

        let flare_response: FlareSolverrResponse = response.json().await?;

        if flare_response.status != "ok" {
            return Err(format!("FlareSolverr error: {}", flare_response.message).into());
        }

        let solution = flare_response.solution.ok_or("No solution in response")?;
        Ok(solution.cookies)
    }
}
