use crate::{DOMAIN, LOGIN_PAGE};

#[allow(dead_code)]
pub fn check_session_expired(response: &wreq::Response) -> bool {
    if !response.status().is_success() {
        let code = response.status();
        debug!("Response status code: {}", code);
        if code == 307 || code == 302 {
            warn!("Session expired...");
            return true;
        }
    }

    let final_url = response.url().as_str().to_string();
    if final_url.contains(LOGIN_PAGE) {
        warn!("Session expired...");
        return true;
    }

    false
}

pub async fn get_remaining_downloads(
    client: &wreq::Client,
) -> Result<u16, Box<dyn std::error::Error>> {
    debug!("Fetching remaining downloads information");

    let domain = {
        let domain_lock = DOMAIN.lock()?;
        domain_lock.clone()
    };

    let url = format!(
        "https://{}//torrent/application/windows/316475-microsoft-toolkit-v2-6-4-activateur-office-2016---2019-windows-10",
        domain
    );
    let result = crate::flaresolverr::fetch_page(client, &url).await?;

    if result.status_code == 307 || result.status_code == 302 {
        return Err("Session expired".into());
    }

    let body = result.body;
    if body.contains("Limite atteinte") {
        return Ok(0);
    }

    let document = scraper::Html::parse_document(&body);

    let selector = scraper::Selector::parse("small[style=\"color: #888;\"]")
        .map_err(|_| "Invalid CSS selector")?;
    let small = document.select(&selector).next();

    let small = match small {
        Some(s) => s,
        None => return Ok(u16::MAX),
    };

    let strong_selector = scraper::Selector::parse("strong").map_err(|_| "Invalid CSS selector")?;
    let strong = small
        .select(&strong_selector)
        .next()
        .ok_or("Strong tag not found in remaining downloads info")?;
    let text = strong.text().collect::<String>();
    // split by /
    let parts: Vec<&str> = text.split('/').collect();
    if parts.len() != 2 {
        return Err("Invalid remaining downloads format".into());
    }

    let remaining: u16 = parts[0].trim().parse()?;
    Ok(remaining)
}
