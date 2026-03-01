use crate::DOMAIN;
use crate::config::Config;
use crate::rest::client_extractor::MaybeCustomClient;
use actix_web::{HttpRequest, HttpResponse, get, web};
use serde_json::Value;
use tokio::time::{Duration, sleep};

#[get("/torrent/{id:[0-9]+}")]
pub async fn download_torrent(
    data: MaybeCustomClient,
    config: web::Data<Config>,
    req_data: HttpRequest,
) -> Result<HttpResponse, Box<dyn std::error::Error>> {
    let id = req_data.match_info().get("id").unwrap();
    let id = id.parse::<usize>()?;

    let domain_lock = DOMAIN.lock()?;
    let cloned_guard = domain_lock.clone();
    let domain = cloned_guard.as_str();
    drop(domain_lock);

    // Request token
    let url = format!("https://{}/engine/start_download_timer", domain);
    let form_body = format!("torrent_id={}", id);

    debug!("Request download token {} {}", url, form_body);

    let result = crate::flaresolverr::post_page(
        &data.client,
        &url,
        &form_body,
        "application/x-www-form-urlencoded; charset=UTF-8",
    )
    .await?;

    if result.status_code != 200 {
        return Err(format!("Failed to get token: {}", result.status_code).into());
    }

    // FlareSolverr wraps JSON API responses in HTML (browser rendering).
    // Try raw JSON first, then extract JSON object from within HTML.
    let response_body = &result.body;
    let body: Value = match serde_json::from_str(response_body) {
        Ok(v) => v,
        Err(_) => {
            debug!("Response is not raw JSON, extracting JSON from HTML wrapper");
            // Extract JSON by finding first { and last }
            let json_str = if let (Some(start), Some(end)) = (response_body.find('{'), response_body.rfind('}')) {
                &response_body[start..=end]
            } else {
                response_body.as_str()
            };
            serde_json::from_str(json_str).map_err(|e| {
                error!("Failed to parse token response: {}. Body: {}", e, &response_body[..response_body.len().min(500)]);
                format!("Failed to parse token response: {}", e)
            })?
        }
    };
    debug!("Response {}", body);

    let token = body
        .get("token")
        .and_then(|h| h.as_str())
        .ok_or("Token not found in start_download_timer response")?;

    if !config.turbo_enabled.unwrap_or(false) {
        debug!("Wait 30 secs...");
        sleep(Duration::from_secs(30)).await;
        debug!("Wait is over");
    }

    // Request signed torrent file
    let url = format!(
        "https://{}/engine/download_torrent?id={}&token={}",
        domain, id, token
    );
    debug!("download URL {}", url);

    // For binary files, use fetch_binary to avoid FlareSolverr's text corruption
    let body: Vec<u8>;
    if crate::flaresolverr::is_flaresolverr_active() {
        body = crate::flaresolverr::fetch_binary(&url).await?;
        if body.is_empty() {
            return Err("Downloaded torrent file is empty".into());
        }
        debug!("Downloaded torrent file: {} bytes", body.len());
    } else {
        let result = crate::flaresolverr::fetch_page(&data.client, &url).await?;

        if result.status_code != 200 {
            if result.status_code == 302 {
                return match crate::utils::get_remaining_downloads(&data.client).await {
                    Ok(0) => {
                        error!("No remaining downloads");
                        Err("No remaining downloads".into())
                    }
                    Ok(n) => {
                        warn!(
                            "Failed to download torrent, but you have {} remaining downloads, might be caused by an insufficient ratio.",
                            n
                        );
                        Err("Failed to download torrent, but you have remaining downloads.".into())
                    }
                    Err(e) => {
                        error!("Error while checking remaining downloads: {}", e);
                        Err("Failed to download torrent and check remaining downloads.".into())
                    }
                };
            }
            return Err(format!(
                "Failed to get torrent file: {}",
                result.status_code,
            )
            .into());
        }
        body = result.body.into_bytes();
    }

    let mut response_builder = HttpResponse::Ok();
    response_builder
        .content_type("application/x-bittorrent")
        .append_header((
            "Content-Disposition",
            format!("attachment; filename=\"{}.torrent\"", id),
        ));

    if let Some(cookies) = data.cookies_header {
        response_builder.insert_header(("X-Session-Cookies", cookies));
    }

    Ok(response_builder.body(body))
}
