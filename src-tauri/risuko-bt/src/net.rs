use std::future::Future;
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use tokio::net::UdpSocket;

pub(crate) fn canonical_addr(addr: SocketAddr) -> SocketAddr {
    SocketAddr::new(addr.ip().to_canonical(), addr.port())
}

pub(crate) fn ip_from_bytes(bytes: &[u8]) -> Option<IpAddr> {
    match bytes.len() {
        4 => <[u8; 4]>::try_from(bytes).ok().map(IpAddr::from),
        16 => <[u8; 16]>::try_from(bytes).ok().map(IpAddr::from),
        _ => None,
    }
}

pub(crate) fn host_prefix(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V4(ip) => IpAddr::V4(ip),
        IpAddr::V6(ip) => {
            let mut octets = ip.octets();
            octets[8..].fill(0);
            IpAddr::V6(Ipv6Addr::from(octets))
        }
    }
}

pub(crate) fn is_lan(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => ip.is_private() || ip.is_loopback() || ip.is_link_local(),
        IpAddr::V6(ip) => ip.is_loopback() || ip.is_unicast_link_local() || ip.is_unique_local(),
    }
}

pub(crate) fn is_shared_cgnat(ip: Ipv4Addr) -> bool {
    let octets = ip.octets();
    octets[0] == 100 && octets[1] & 0xc0 == 0x40
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Reach {
    Global,
    Private,
    Loopback,
}

pub(crate) fn reach(ip: IpAddr) -> Reach {
    let ip = ip.to_canonical();
    let unrouted = match ip {
        IpAddr::V4(v4) => v4.octets()[0] == 0 || is_shared_cgnat(v4),
        IpAddr::V6(v6) => v6.is_unspecified(),
    };
    if ip.is_loopback() {
        Reach::Loopback
    } else if unrouted || is_lan(ip) {
        Reach::Private
    } else {
        Reach::Global
    }
}

pub(crate) fn ipv4_mask(prefix: u8) -> u32 {
    match prefix {
        0 => 0,
        p => !0u32 << (32 - p.min(32)),
    }
}

pub(crate) fn ipv6_mask(prefix: u8) -> u128 {
    match prefix {
        0 => 0,
        p => !0u128 << (128 - p.min(128)),
    }
}

pub(crate) fn port_unavailable(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::AddrInUse | io::ErrorKind::PermissionDenied
    )
}

pub(crate) async fn bind_first_available<T, Fut>(
    label: &str,
    ports: &[u16],
    bind: impl Fn(u16) -> Fut,
) -> io::Result<T>
where
    Fut: Future<Output = io::Result<T>>,
{
    let mut last = io::Error::from(io::ErrorKind::InvalidInput);
    for (i, &port) in ports.iter().enumerate() {
        match bind(port).await {
            Ok(bound) => return Ok(bound),
            Err(e) if port_unavailable(&e) => {
                if i + 1 < ports.len() {
                    tracing::warn!("{label}/{port} unavailable ({e}); trying the next port");
                }
                last = e;
            }
            Err(e) => return Err(e),
        }
    }
    Err(last)
}

pub(crate) fn bind_udp_v6_only(port: u16) -> io::Result<UdpSocket> {
    use socket2::{Domain, Protocol, Socket, Type};
    let socket = Socket::new(Domain::IPV6, Type::DGRAM, Some(Protocol::UDP))?;
    socket.set_only_v6(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&SocketAddr::from((Ipv6Addr::UNSPECIFIED, port)).into())?;
    UdpSocket::from_std(socket.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefixes_group_ipv6_by_64_and_keep_ipv4_whole() {
        let ip = |text: &str| text.parse::<IpAddr>().unwrap();
        assert_eq!(
            host_prefix(ip("2001:db8:1:2:3:4:5:6")),
            ip("2001:db8:1:2::")
        );
        assert_eq!(host_prefix(ip("192.0.2.7")), ip("192.0.2.7"));
        assert_eq!(ipv4_mask(24), 0xffff_ff00);
        assert_eq!(ipv4_mask(0), 0);
        assert_eq!(ipv4_mask(40), u32::MAX);
        assert_eq!(ipv6_mask(64), !0u128 << 64);
    }

    #[tokio::test]
    async fn binding_moves_to_the_next_port_only_when_the_port_is_unavailable() {
        use std::io::{Error, ErrorKind};
        let attempts = std::cell::RefCell::new(Vec::new());

        let bound = bind_first_available("test", &[10, 20, 0], |port| {
            attempts.borrow_mut().push(port);
            async move {
                match port {
                    10 => Err(Error::from(ErrorKind::AddrInUse)),
                    20 => Err(Error::from(ErrorKind::PermissionDenied)),
                    _ => Ok(port),
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(bound, 0);
        assert_eq!(*attempts.borrow(), vec![10, 20, 0]);

        attempts.borrow_mut().clear();
        let bound = bind_first_available("test", &[10, 20, 0], |port| {
            attempts.borrow_mut().push(port);
            async move { Ok(port) }
        })
        .await
        .unwrap();
        assert_eq!(bound, 10);
        assert_eq!(*attempts.borrow(), vec![10]);

        for kind in [ErrorKind::AddrNotAvailable, ErrorKind::Unsupported] {
            attempts.borrow_mut().clear();
            let err = bind_first_available::<u16, _>("test", &[10, 20, 0], |port| {
                attempts.borrow_mut().push(port);
                async move { Err(Error::from(kind)) }
            })
            .await
            .unwrap_err();
            assert_eq!(err.kind(), kind);
            assert_eq!(*attempts.borrow(), vec![10], "{kind:?} must not be retried");
        }

        attempts.borrow_mut().clear();
        let err = bind_first_available::<u16, _>("test", &[10, 0], |port| {
            attempts.borrow_mut().push(port);
            async move { Err(Error::from(ErrorKind::AddrInUse)) }
        })
        .await
        .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::AddrInUse);
        assert_eq!(*attempts.borrow(), vec![10, 0]);
    }
}
