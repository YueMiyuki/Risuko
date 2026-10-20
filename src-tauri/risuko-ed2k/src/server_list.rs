use std::net::SocketAddrV4;

#[derive(Debug, Clone)]
pub struct ServerEntry {
    pub ip: String,
    pub port: u16,
    pub name: String,
}

impl ServerEntry {
    fn new(ip: &str, port: u16, name: &str) -> Self {
        Self {
            ip: ip.to_string(),
            port,
            name: name.to_string(),
        }
    }

    pub fn to_socket_addr(&self) -> Option<SocketAddrV4> {
        let ip = self.ip.parse().ok()?;
        Some(SocketAddrV4::new(ip, self.port))
    }
}

pub fn server_list(entries: &[String]) -> Vec<ServerEntry> {
    let servers: Vec<ServerEntry> = entries
        .iter()
        .filter_map(|s| {
            let (ip, port) = s.split_once(':')?;
            let _: std::net::Ipv4Addr = ip.parse().ok()?;
            Some(ServerEntry::new(ip, port.parse().ok()?, "Custom"))
        })
        .collect();
    if !servers.is_empty() {
        return servers;
    }
    vec![
        ServerEntry::new("176.123.5.89", 4725, "eMule Sunrise"),
        ServerEntry::new("45.82.80.155", 5687, "eMule Security"),
        ServerEntry::new("85.239.33.123", 4232, "!! Sharing-Devils No.2 !!"),
        ServerEntry::new("91.208.162.87", 4232, "!! Sharing-Devils No.1 !!"),
        ServerEntry::new("145.239.2.134", 4661, "GrupoTS Server"),
    ]
}
