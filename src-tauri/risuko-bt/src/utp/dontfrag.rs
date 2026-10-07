//! Per-datagram don't-fragment control for µTP path-MTU discovery; DF is a socket option on the shared socket, so it is toggled around each probe under a lock

use std::io;
use std::net::SocketAddr;

use tokio::net::UdpSocket;
use tokio::sync::Mutex;

pub(crate) struct DontFragment {
    lock: Mutex<()>,
    v6: bool,
}

impl DontFragment {
    /// DF control for `udp`, or `None` where it can't be toggled
    pub(crate) fn for_socket(udp: &UdpSocket) -> Option<Self> {
        let v6 = udp.local_addr().ok()?.is_ipv6();
        set(udp, v6, false).ok()?;
        Some(Self {
            lock: Mutex::new(()),
            v6,
        })
    }

    /// Send one datagram with DF set or cleared
    pub(crate) async fn send_to(
        &self,
        udp: &UdpSocket,
        datagram: &[u8],
        target: SocketAddr,
        dont_fragment: bool,
    ) -> io::Result<()> {
        let _serialized = self.lock.lock().await;
        if dont_fragment {
            set(udp, self.v6, true)?;
        }
        let sent = udp.send_to(datagram, target).await.map(|_| ());
        if dont_fragment {
            let _ = set(udp, self.v6, false);
        }
        sent
    }
}

/// The OS refused a DF datagram as larger than the known path MTU
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
    // WANT is the kernel default
    if v6 {
        let value = if on {
            libc::IPV6_PMTUDISC_DO
        } else {
            libc::IPV6_PMTUDISC_WANT
        };
        (libc::IPPROTO_IPV6, libc::IPV6_MTU_DISCOVER, value)
    } else {
        let value = if on {
            libc::IP_PMTUDISC_DO
        } else {
            libc::IP_PMTUDISC_WANT
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
        let udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let target = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let Some(df) = DontFragment::for_socket(&udp) else {
            return; // platform without DF control
        };
        df.send_to(&udp, b"probe", target.local_addr().unwrap(), true)
            .await
            .unwrap();
        let mut buf = [0u8; 8];
        let (n, _) = target.recv_from(&mut buf).await.unwrap();
        assert_eq!(&buf[..n], b"probe");
    }
}
