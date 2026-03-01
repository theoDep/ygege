use crate::client::build_client;
use crate::config::Config;
use crate::domain::get_leaked_ip;
use crate::flaresolverr::{Cookie, FlareSolverrClient, init_session_pool};
use crate::{DOMAIN, LOGIN_PAGE, LOGIN_PROCESS_PAGE};
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::sync::OnceLock;
use wreq::header::HeaderMap;
use wreq::{Client, Url};

pub static KEY: OnceLock<String> = OnceLock::new();

pub async fn login(
    config: &Config,
    use_sessions: bool,
) -> Result<Client, Box<dyn std::error::Error>> {
    let username: &str = config.username.as_str();
    let password: &str = config.password.as_str();
    debug!("Logging in with username: {}", username);

    let domain_lock = DOMAIN.lock()?;
    let cloned_guard = domain_lock.clone();
    let domain = cloned_guard.as_str();
    drop(domain_lock);

    let leaked_ip = get_leaked_ip().await?;
    let client = build_client(domain, &leaked_ip)?;

    let mut headers = HeaderMap::new();
    add_bypass_headers(&mut headers);

    let start = std::time::Instant::now();

    if use_sessions {
        // check if the session file exists
        let session_file = format!("sessions/{}.cookies", username);
        if std::path::Path::new(&session_file.clone()).exists() {
            debug!("Session file found: {}", session_file);
            // load the session from the file
            let cookies = std::fs::read_to_string(&session_file)?;
            let cookies = cookies.split(";").collect::<Vec<&str>>();
            let cookies_len = cookies.len();
            for cookie in cookies {
                let cookie = cookie.trim();
                if cookie.is_empty() {
                    continue;
                }
                let parts: Vec<&str> = cookie.split('=').collect();
                if parts.len() != 2 {
                    continue;
                }
                let name = parts[0].trim();
                let value = parts[1].trim();
                let cookie = wreq::cookie::CookieBuilder::new(name, value)
                    .domain(domain)
                    .path("/")
                    .http_only(true)
                    .secure(true)
                    .build();
                let url = Url::parse(format!("https://{domain}/").as_str())?;
                client.set_cookie(&url, cookie);
            }
            debug!("Restored {} cookies from session file", cookies_len);
        }

        // check if the session is still valid
        let session_check = client
            .get(format!("https://{domain}/"))
            .headers(headers.clone())
            .send()
            .await;

        match session_check {
            Ok(response) if response.status().is_success() => {
                let stop = std::time::Instant::now();
                debug!(
                    "Successfully resumed session in {:?}",
                    stop.duration_since(start)
                );
                return Ok(client);
            }
            Ok(response) => {
                debug!(
                    "Session is not valid, deleting session file (code {})",
                    response.status()
                );
                let session_file = format!("sessions/{}.cookies", username);
                let _ = std::fs::remove_file(&session_file);
                debug!("Session file deleted");
            }
            Err(e) => {
                warn!("Session validation failed (connection error): {}", e);
                let session_file = format!("sessions/{}.cookies", username);
                let _ = std::fs::remove_file(&session_file);
                debug!("Session file deleted due to connection error");
            }
        }
    }

    client.clear_cookies();

    // Try leaked IP method first
    match try_leaked_ip_login(&client, domain, username, password, &headers).await {
        Ok(()) => {
            let stop = std::time::Instant::now();
            debug!(
                "Logged in successfully with leaked IP in {:?}",
                stop.duration_since(start)
            );

            if use_sessions {
                save_session(username, &client).await?;
            }
            return Ok(client);
        }
        Err(e) => {
            warn!("Leaked IP login failed: {}, trying FlareSolverr", e);
        }
    }

    // Fallback to FlareSolverr if available
    if let Some(flaresolverr_url) = &config.flaresolverr_url {
        let (flare_client, cookies) =
            try_flaresolverr_login(domain, username, password, flaresolverr_url).await?;

        // Build a simple client with the FlareSolverr cookies applied
        let simple_client = crate::client::build_simple_client()
            .map_err(|e| -> Box<dyn std::error::Error> { format!("{}", e).into() })?;

        let base_url = Url::parse(format!("https://{domain}/").as_str())?;
        for cookie in &cookies {
            let wreq_cookie = wreq::cookie::CookieBuilder::new(&cookie.name, &cookie.value)
                .domain(domain)
                .path("/")
                .http_only(true)
                .secure(true)
                .build();
            simple_client.set_cookie(&base_url, wreq_cookie);
        }

        // Initialize the FlareSolverr session pool so fetch_page() can use it
        init_session_pool(flare_client, cookies, domain).await;

        let stop = std::time::Instant::now();
        debug!(
            "Logged in successfully with FlareSolverr in {:?}",
            stop.duration_since(start)
        );

        if use_sessions {
            save_session(username, &simple_client).await?;
        }
        return Ok(simple_client);
    }

    Err("Leaked IP login failed and no FlareSolverr URL configured".into())
}

async fn try_leaked_ip_login(
    client: &Client,
    domain: &str,
    username: &str,
    password: &str,
    headers: &HeaderMap,
) -> Result<(), Box<dyn std::error::Error>> {
    // inject account_created=true cookie (cookie magique)
    let cookie = wreq::cookie::CookieBuilder::new("account_created", "true")
        .domain(domain)
        .path("/")
        .http_only(true)
        .secure(true)
        .build();

    let url = Url::parse(format!("https://{domain}/").as_str())?;
    client.set_cookie(&url, cookie);

    // make a request to the login page
    let response = client
        .get(format!("https://{domain}{LOGIN_PAGE}"))
        .headers(headers.clone())
        .send()
        .await?;

    if !response.status().is_success() {
        return Err(format!("Failed to fetch login page: {}", response.status()).into());
    }

    // detect if the ygg_ cookie is set
    let cookies = response.cookies();
    let mut has_ygg_cookie = false;
    for cookie in cookies {
        if cookie.name() == "ygg_" {
            has_ygg_cookie = true;
            break;
        }
    }

    if !has_ygg_cookie {
        return Err("No ygg_ cookie found".into());
    }

    // multipart/form-data
    let payload = [("id", username), ("pass", password)];

    // post multipart on /auth/process_login
    let response = client
        .post(format!("https://{domain}{LOGIN_PROCESS_PAGE}"))
        .headers(headers.clone())
        .form(&payload)
        .send()
        .await?;

    if !response.status().is_success() {
        if response.status() == 401 {
            error!("Invalid username or password");
            return Err("Invalid username or password".into());
        }
        return Err(format!("Failed to login: {}", response.status()).into());
    }

    // get site root page for final cookies
    let response = client
        .get(format!("https://{domain}/"))
        .headers(headers.clone())
        .send()
        .await?;
    if !response.status().is_success() {
        return Err(format!("Failed to fetch site root page: {}", response.status()).into());
    }

    Ok(())
}

async fn try_flaresolverr_login(
    domain: &str,
    username: &str,
    password: &str,
    flaresolverr_url: &str,
) -> Result<(FlareSolverrClient, Vec<Cookie>), Box<dyn std::error::Error>> {
    let mut flare_client = FlareSolverrClient::new(flaresolverr_url.to_string())?;

    // Create a persistent session so the browser state (including cf_clearance) is reused
    if let Err(e) = flare_client.create_session().await {
        warn!("Failed to create FlareSolverr session: {}, proceeding without session", e);
    }

    // Step 1: Get login page with account_created cookie
    let account_cookie = Cookie {
        name: "account_created".to_string(),
        value: "true".to_string(),
    };

    let (_, mut cookies, _) = flare_client
        .get_with_cookies(
            &format!("https://{domain}{LOGIN_PAGE}"),
            Some(&[account_cookie]),
        )
        .await?;

    // Check for ygg_ cookie
    let has_ygg_cookie = cookies.iter().any(|c| c.name == "ygg_");
    if !has_ygg_cookie {
        return Err("No ygg_ cookie found via FlareSolverr".into());
    }

    // Step 2: Submit login form
    let mut form_data = HashMap::new();
    form_data.insert("id".to_string(), username.to_string());
    form_data.insert("pass".to_string(), password.to_string());

    let login_cookies = flare_client
        .post_form(
            &format!("https://{domain}{LOGIN_PROCESS_PAGE}"),
            &form_data,
            Some(&cookies),
        )
        .await?;

    cookies.extend(login_cookies);

    // Step 3: Get final cookies from root page
    let (_, final_cookies, _) = flare_client
        .get_with_cookies(&format!("https://{domain}/"), Some(&cookies))
        .await?;

    cookies.extend(final_cookies);

    Ok((flare_client, cookies))
}

async fn save_session(username: &str, client: &Client) -> Result<(), Box<dyn std::error::Error>> {
    // save the session in a file
    let mut file = File::create(format!("sessions/{}.cookies", username))?;
    let cookies_header = client
        .get_cookies(&Url::parse(
            format!("https://{}/", DOMAIN.lock()?.as_str()).as_str(),
        )?)
        .unwrap();
    let cookies_header_value = cookies_header.to_str()?;
    debug!("Cookies: {}", cookies_header_value);
    file.write_all(cookies_header_value.as_bytes())?;
    file.flush()?;

    Ok(())
}

pub fn add_bypass_headers(headers: &mut HeaderMap) {
    let own_ip_lock = crate::domain::OWN_IP.get();
    if let Some(own_ip) = own_ip_lock {
        headers.insert("CF-Connecting-IP", own_ip.parse().unwrap());
        headers.insert("X-Forwarded-For", own_ip.parse().unwrap());
    }
}
