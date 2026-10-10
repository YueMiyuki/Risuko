use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use parking_lot::RwLock;
use tokio::net::UdpSocket;

pub(crate) struct UdpSender {
    udp: Arc<UdpSocket>,
    dont_fragment: Option<DontFragment>,
}

struct DontFragment {
    lock: RwLock<()>,
    v6: bool,
}

impl UdpSender {
    pub(crate) fn new(udp: Arc<UdpSocket>, exclusive: bool) -> Self {
        let dont_fragment = exclusive.then(|| DontFragment::for_socket(&udp)).flatten();
        Self { udp, dont_fragment }
    }

    pub(crate) fn can_probe(&self) -> bool {
        self.dont_fragment.is_some()
    }

    pub(crate) async fn send_to(&self, datagram: &[u8], target: SocketAddr) -> io::Result<()> {
        self.send(datagram, target, false).await
    }

    pub(crate) async fn send_probe(&self, datagram: &[u8], target: SocketAddr) -> io::Result<()> {
        self.send(datagram, target, true).await
    }

    pub(crate) fn try_send_to(&self, datagram: &[u8], target: SocketAddr) -> io::Result<()> {
        self.try_send(datagram, target, false)
    }

    async fn send(
        &self,
        datagram: &[u8],
        target: SocketAddr,
        dont_fragment: bool,
    ) -> io::Result<()> {
        loop {
            self.udp.writable().await?;
            match self.try_send(datagram, target, dont_fragment) {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
                result => return result,
            }
        }
    }

    fn try_send(&self, datagram: &[u8], target: SocketAddr, dont_fragment: bool) -> io::Result<()> {
        let send = || self.udp.try_send_to(datagram, target).map(|_| ());
        match &self.dont_fragment {
            None if dont_fragment => Err(io::Error::from(io::ErrorKind::Unsupported)),
            None => send(),
            Some(df) if !dont_fragment => {
                let _shared = df.lock.read();
                send()
            }
            Some(df) => {
                let _exclusive = df.lock.write();
                set(&self.udp, df.v6, true)?;
                let sent = send();
                let _ = set(&self.udp, df.v6, false);
                sent
            }
        }
    }
}

impl DontFragment {
    fn for_socket(udp: &UdpSocket) -> Option<Self> {
        let v6 = udp.local_addr().ok()?.is_ipv6();
        set(udp, v6, false).ok()?;
        Some(Self {
            lock: RwLock::new(()),
            v6,
        })
    }
}

pub(crate) fn is_message_too_big(error: &io::Error) -> bool {
    #[cfg(unix)]
    {
        error.raw_os_error() == Some(libc::EMSGSIZE)
    }
    #[cfg(windows)]
    {
        error.raw_os_error() == Some(windows_sys::Win32::Networking::WinSock::WSAEMSGSIZE)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = error;
        false
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn option(v6: bool, on: bool) -> (libc::c_int, libc::c_int, libc::c_int) {
    if v6 {
        let value = if on {
            libc::IPV6_PMTUDISC_DO
        } else {
            libc::IPV6_PMTUDISC_DONT
        };
        (libc::IPPROTO_IPV6, libc::IPV6_MTU_DISCOVER, value)
    } else {
        let value = if on {
            libc::IP_PMTUDISC_DO
        } else {
            libc::IP_PMTUDISC_DONT
        };
        (libc::IPPROTO_IP, libc::IP_MTU_DISCOVER, value)
    }
}

#[cfg(any(target_vendor = "apple", target_os = "freebsd"))]
fn option(v6: bool, on: bool) -> (libc::c_int, libc::c_int, libc::c_int) {
    if v6 {
        (libc::IPPROTO_IPV6, libc::IPV6_DONTFRAG, on as libc::c_int)
    } else {
        (libc::IPPROTO_IP, libc::IP_DONTFRAG, on as libc::c_int)
    }
}

#[cfg(any(
    target_os = "linux",
    target_os = "android",
    target_vendor = "apple",
    target_os = "freebsd"
))]
fn set(udp: &UdpSocket, v6: bool, on: bool) -> io::Result<()> {
    use std::os::fd::AsRawFd;
    let (level, name, value) = option(v6, on);
    // SAFETY: the fd is borrowed from `udp` and `value` outlives the call
    let rc = unsafe {
        libc::setsockopt(
            udp.as_raw_fd(),
            level,
            name,
            (&value as *const libc::c_int).cast(),
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    if rc == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(windows)]
fn set(udp: &UdpSocket, v6: bool, on: bool) -> io::Result<()> {
    use std::os::windows::io::AsRawSocket;
    use windows_sys::Win32::Networking::WinSock::{
        setsockopt, IPPROTO_IP, IPPROTO_IPV6, IPV6_DONTFRAG, IP_DONTFRAGMENT, SOCKET_ERROR,
    };
    let (level, name) = if v6 {
        (IPPROTO_IPV6, IPV6_DONTFRAG)
    } else {
        (IPPROTO_IP, IP_DONTFRAGMENT)
    };
    let value: u32 = on as u32;
    // SAFETY: the socket is borrowed from `udp` and `value` outlives the call
    let rc = unsafe {
        setsockopt(
            udp.as_raw_socket() as usize,
            level,
            name,
            (&value as *const u32).cast(),
            std::mem::size_of::<u32>() as i32,
        )
    };
    if rc == SOCKET_ERROR {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "android",
    target_vendor = "apple",
    target_os = "freebsd",
    windows
)))]
fn set(_udp: &UdpSocket, _v6: bool, _on: bool) -> io::Result<()> {
    Err(io::Error::from(io::ErrorKind::Unsupported))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn toggles_dont_fragment_around_a_send() {
        let udp = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let target = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let sender = UdpSender::new(udp, true);
        if !sender.can_probe() {
            return;
        }
        let to = target.local_addr().unwrap();
        sender.send_probe(b"probe", to).await.unwrap();
        sender.send_to(b"plain", to).await.unwrap();
        let mut buf = [0u8; 8];
        for expected in [b"probe", b"plain"] {
            let (n, _) = target.recv_from(&mut buf).await.unwrap();
            assert_eq!(&buf[..n], expected);
        }
    }
}
