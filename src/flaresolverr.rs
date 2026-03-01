use crate::client::build_simple_client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
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
    #[allow(dead_code)]
    #[serde(rename = "userAgent")]
    user_agent: String,
    response: Option<String>,
}

pub struct FlareSolverrClient {
    client: Client,
    base_url: String,
    session_id: Option<String>,
}

/// Global FlareSolverr client + cookies, set after successful FlareSolverr login
static FLARESOLVERR_STATE: OnceLock<Mutex<Option<FlareSolverrState>>> = OnceLock::new();

/// When true, skip direct requests entirely and go straight to FlareSolverr
static FLARESOLVERR_ACTIVE: AtomicBool = AtomicBool::new(false);

struct FlareSolverrState {
    client: FlareSolverrClient,
    cookies: Vec<Cookie>,
    user_agent: Option<String>,
}

/// Initialize the global FlareSolverr state after a successful login
pub fn set_global_flaresolverr(flare_client: FlareSolverrClient, cookies: Vec<Cookie>) {
    let state = FLARESOLVERR_STATE.get_or_init(|| Mutex::new(None));
    if let Ok(mut guard) = state.try_lock() {
        *guard = Some(FlareSolverrState {
            client: flare_client,
            cookies,
            user_agent: None,
        });
    }
    // Mark FlareSolverr as active — all subsequent fetch_page() calls skip direct requests
    FLARESOLVERR_ACTIVE.store(true, Ordering::SeqCst);
}

/// Check if FlareSolverr mode is active
pub fn is_flaresolverr_active() -> bool {
    FLARESOLVERR_ACTIVE.load(Ordering::SeqCst)
}

/// Download binary data (e.g., .torrent files) using a direct HTTP request
/// with the FlareSolverr cookies. FlareSolverr's browser can't handle binary
/// downloads since it converts responses to text strings.
pub async fn fetch_binary(url: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let state = FLARESOLVERR_STATE.get_or_init(|| Mutex::new(None));
    let guard = state.lock().await;

    if let Some(ref fs_state) = *guard {
        // Build a fresh simple client with all FlareSolverr cookies
        let client = build_simple_client().map_err(|e| -> Box<dyn std::error::Error> {
            format!("{}", e).into()
        })?;

        let parsed_url = wreq::Url::parse(url)?;
        let domain = parsed_url.domain().unwrap_or("");

        // Apply all cookies from the FlareSolverr session
        let base_url = wreq::Url::parse(&format!("https://{}/", domain))?;
        for cookie in &fs_state.cookies {
            let wreq_cookie = wreq::cookie::CookieBuilder::new(&cookie.name, &cookie.value)
                .domain(domain)
                .path("/")
                .http_only(true)
                .secure(true)
                .build();
            client.set_cookie(&base_url, wreq_cookie);
        }

        // Use FlareSolverr's exact User-Agent — CF ties cf_clearance to the UA
        let mut request = client.get(url);
        if let Some(ref ua) = fs_state.user_agent {
            debug!("Using FlareSolverr User-Agent: {}", ua);
            request = request.header("User-Agent", ua.as_str());
        }

        debug!("Downloading binary via direct client with FlareSolverr cookies: {}", url);
        let response = request.send().await?;
        let status = response.status();

        if status.is_success() {
            let bytes = response.bytes().await?;
            debug!("Binary download completed: {} bytes", bytes.len());
            return Ok(bytes.to_vec());
        }

        warn!("Direct binary download failed with status {}, falling back to FlareSolverr text response", status);
    }

    // Fallback: use FlareSolverr (will likely corrupt binary, but better than nothing)
    drop(guard);
    let result = fetch_via_flaresolverr(url).await?;
    Ok(result.body.into_bytes())
}

/// Fetch a page via FlareSolverr (used when FlareSolverr mode is active).
async fn fetch_via_flaresolverr(url: &str) -> Result<FetchResult, Box<dyn std::error::Error>> {
    let state = FLARESOLVERR_STATE.get_or_init(|| Mutex::new(None));
    let mut guard = state.lock().await;

    if let Some(ref mut fs_state) = *guard {
        debug!("Fetching via FlareSolverr: {}", url);
        let start = std::time::Instant::now();
        let (body, new_cookies, user_agent) = fs_state
            .client
            .get_with_cookies(url, Some(&fs_state.cookies))
            .await?;

        // Update stored cookies with any new ones
        for new_cookie in &new_cookies {
            if let Some(existing) = fs_state
                .cookies
                .iter_mut()
                .find(|c| c.name == new_cookie.name)
            {
                existing.value = new_cookie.value.clone();
            } else {
                fs_state.cookies.push(new_cookie.clone());
            }
        }

        // Capture the User-Agent from FlareSolverr's browser on first response
        if fs_state.user_agent.is_none() && !user_agent.is_empty() {
            debug!("Captured FlareSolverr User-Agent: {}", user_agent);
            fs_state.user_agent = Some(user_agent);
        }

        debug!(
            "FlareSolverr fetch completed in {:?}",
            start.elapsed()
        );

        return Ok(FetchResult {
            body,
            status_code: 200,
            used_flaresolverr: true,
        });
    }

    Err("FlareSolverr state not initialized".into())
}

/// Fetch a page: if FlareSolverr mode is active, go directly through it.
/// Otherwise try direct GET first, fall back to FlareSolverr if 403.
pub async fn fetch_page(
    client: &Client,
    url: &str,
) -> Result<FetchResult, Box<dyn std::error::Error>> {
    // If FlareSolverr mode is active, skip direct request entirely
    if is_flaresolverr_active() {
        return fetch_via_flaresolverr(url).await;
    }

    // Try direct request
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

    // If 403 (Cloudflare challenge), try FlareSolverr
    if status == 403 {
        match fetch_via_flaresolverr(url).await {
            Ok(result) => return Ok(result),
            Err(e) => {
                warn!("FlareSolverr fallback failed: {}", e);
            }
        }
    }

    // Check for session expiry (307/302)
    if status == 307 || status == 302 {
        return Ok(FetchResult {
            body: String::new(),
            status_code: status.as_u16(),
            used_flaresolverr: false,
        });
    }

    Err(format!("Request failed with status: {}", status).into())
}

/// POST a form via FlareSolverr (used when FlareSolverr mode is active).
async fn post_via_flaresolverr(
    url: &str,
    form_data: &std::collections::HashMap<String, String>,
) -> Result<FetchResult, Box<dyn std::error::Error>> {
    let state = FLARESOLVERR_STATE.get_or_init(|| Mutex::new(None));
    let mut guard = state.lock().await;

    if let Some(ref mut fs_state) = *guard {
        debug!("POSTing via FlareSolverr: {}", url);
        let start = std::time::Instant::now();

        // Use post_form which returns cookies; we also need the response body
        // So we'll build the request manually
        let post_data = form_data
            .iter()
            .map(|(k, v)| format!("{}={}", urlencoding::encode(k), urlencoding::encode(v)))
            .collect::<Vec<_>>()
            .join("&");

        let request = FlareSolverrRequest {
            cmd: "request.post".to_string(),
            url: url.to_string(),
            session: fs_state.client.session_id.clone(),
            cookies: Some(fs_state.cookies.clone()),
            post_data: Some(post_data),
            max_timeout: Some(60000),
        };

        let response = fs_state
            .client
            .client
            .post(&format!("{}/v1", fs_state.client.base_url))
            .json(&request)
            .send()
            .await?;

        let flare_response: FlareSolverrResponse = response.json().await?;

        if flare_response.status != "ok" {
            return Err(format!("FlareSolverr POST error: {}", flare_response.message).into());
        }

        let solution = flare_response.solution.ok_or("No solution in POST response")?;

        // Update stored cookies
        for new_cookie in &solution.cookies {
            if let Some(existing) = fs_state
                .cookies
                .iter_mut()
                .find(|c| c.name == new_cookie.name)
            {
                existing.value = new_cookie.value.clone();
            } else {
                fs_state.cookies.push(new_cookie.clone());
            }
        }

        debug!("FlareSolverr POST completed in {:?}", start.elapsed());

        let body = solution.response.unwrap_or_default();
        return Ok(FetchResult {
            body,
            status_code: 200,
            used_flaresolverr: true,
        });
    }

    Err("FlareSolverr state not initialized".into())
}

/// POST a form: if FlareSolverr mode is active, go directly through it.
/// Otherwise use the direct client.
pub async fn post_page(
    client: &Client,
    url: &str,
    form_body: &str,
    content_type: &str,
) -> Result<FetchResult, Box<dyn std::error::Error>> {
    // If FlareSolverr mode is active, proxy through FlareSolverr
    if is_flaresolverr_active() {
        // Parse the form body into a HashMap for FlareSolverr
        let mut form_data = std::collections::HashMap::new();
        for pair in form_body.split('&') {
            let mut parts = pair.splitn(2, '=');
            if let (Some(key), Some(value)) = (parts.next(), parts.next()) {
                form_data.insert(key.to_string(), value.to_string());
            }
        }
        return post_via_flaresolverr(url, &form_data).await;
    }

    // Direct POST
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
