use crate::client::build_simple_client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::OnceLock;
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

struct FlareSolverrState {
    client: FlareSolverrClient,
    cookies: Vec<Cookie>,
}

/// Initialize the global FlareSolverr state after a successful login
pub fn set_global_flaresolverr(flare_client: FlareSolverrClient, cookies: Vec<Cookie>) {
    let state = FLARESOLVERR_STATE.get_or_init(|| Mutex::new(None));
    // We can't await here (sync fn), so use try_lock
    if let Ok(mut guard) = state.try_lock() {
        *guard = Some(FlareSolverrState {
            client: flare_client,
            cookies,
        });
    }
}

/// Fetch a page: try direct GET first, fall back to FlareSolverr if 403.
/// Returns the response body as a string.
pub async fn fetch_page(
    client: &Client,
    url: &str,
) -> Result<FetchResult, Box<dyn std::error::Error>> {
    // Try direct request first
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
        let state = FLARESOLVERR_STATE.get_or_init(|| Mutex::new(None));
        let mut guard = state.lock().await;

        if let Some(ref mut fs_state) = *guard {
            debug!("Got 403, retrying via FlareSolverr: {}", url);
            let (body, new_cookies) = fs_state
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

            return Ok(FetchResult {
                body,
                status_code: 200,
                used_flaresolverr: true,
            });
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

    pub async fn get_with_cookies(
        &mut self,
        url: &str,
        cookies: Option<&[Cookie]>,
    ) -> Result<(String, Vec<Cookie>), Box<dyn std::error::Error>> {
        let request = FlareSolverrRequest {
            cmd: "request.get".to_string(),
            url: url.to_string(),
            session: self.session_id.clone(),
            cookies: cookies.map(|c| c.to_vec()),
            post_data: None,
            max_timeout: Some(120000),
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
        Ok((body, solution.cookies))
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
            max_timeout: Some(120000),
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
