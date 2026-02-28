use crate::config::Config;
use crate::auth::login;
use actix_web::{HttpResponse, get, web};

#[get("/auth")]
pub async fn auth(config: web::Data<Config>) -> Result<HttpResponse, Box<dyn std::error::Error>> {
    let client = login(&config, false).await;
    match client {
        Ok(client) => {
            let domain_lock = crate::DOMAIN.lock()?;
            let cloned_guard = domain_lock.clone();
            let domain = cloned_guard.as_str();
            drop(domain_lock);

            let url = wreq::Url::parse(&format!("https://{}/", domain)).unwrap();
            let cookies = client.get_cookies(&url);
            match cookies {
                Some(cookies_header) => {
                    let cookie_str = cookies_header.to_str().unwrap_or("").to_string();
                    info!("Login successful: cookies={}", cookie_str);
                    let mut response = HttpResponse::Ok();
                    response.insert_header(("X-Session-Cookies", cookie_str.clone()));
                    Ok(response.body(cookie_str))
                }
                None => Ok(HttpResponse::Ok().body("Login successful, but no cookies found")),
            }
        }
        Err(e) => {
            error!("Login failed: {}", e);
            Ok(HttpResponse::Unauthorized().body(format!("Login failed: {}", e)))
        }
    }
}
