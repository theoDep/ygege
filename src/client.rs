use crate::resolver::AsyncDNSResolverAdapter;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use wreq::Client;
use wreq_util::{Emulation, EmulationOS, EmulationOption};

fn build_emulation() -> EmulationOption {
    EmulationOption::builder()
        .emulation(Emulation::Chrome137)
        .emulation_os(EmulationOS::Windows)
        .build()
}

fn build_dns_resolver() -> Result<Arc<AsyncDNSResolverAdapter>, Box<dyn std::error::Error + Send + Sync>> {
    Ok(Arc::new(AsyncDNSResolverAdapter::new().map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
        format!("Failed to create DNS resolver: {}", e).into()
    })?))
}

pub fn build_client(domain: &str, leaked_ip: &str) -> Result<Client, Box<dyn std::error::Error>> {
    let dns = build_dns_resolver().map_err(|e| -> Box<dyn std::error::Error> {
        format!("{}", e).into()
    })?;
    Ok(Client::builder()
        .emulation(build_emulation())
        .gzip(true)
        .deflate(true)
        .brotli(true)
        .zstd(true)
        .cookie_store(true)
        .dns_resolver(dns)
        .cert_verification(false)
        .verify_hostname(false)
        .resolve(
            domain,
            SocketAddr::new(IpAddr::from_str(leaked_ip)?, 443),
        )
        .build()?)
}

pub fn build_simple_client() -> Result<Client, Box<dyn std::error::Error + Send + Sync>> {
    Ok(Client::builder()
        .emulation(build_emulation())
        .gzip(true)
        .deflate(true)
        .brotli(true)
        .zstd(true)
        .cookie_store(true)
        .dns_resolver(build_dns_resolver()?)
        .build()?)
}
