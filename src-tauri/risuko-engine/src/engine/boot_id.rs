use std::sync::OnceLock;

pub const UNKNOWN: &str = "unknown";

const NUMERIC_TOLERANCE_SECS: u64 = 120;

pub fn current() -> Option<&'static str> {
    static CURRENT: OnceLock<Option<String>> = OnceLock::new();
    CURRENT.get_or_init(boot_id).as_deref()
}

pub fn same_boot(stored: &str, current: Option<&str>) -> bool {
    let Some(current) = current else {
        return true;
    };
    if stored == UNKNOWN {
        return true;
    }
    match (stored.parse::<u64>(), current.parse::<u64>()) {
        (Ok(stored), Ok(current)) => stored.abs_diff(current) <= NUMERIC_TOLERANCE_SECS,
        _ => stored == current,
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
pub fn boot_id() -> Option<String> {
    let id = std::fs::read_to_string("/proc/sys/kernel/random/boot_id").ok()?;
    let id = id.trim();
    (!id.is_empty()).then(|| id.to_string())
}

#[cfg(any(target_os = "macos", target_os = "ios"))]
pub fn boot_id() -> Option<String> {
    let mut buf = [0u8; 64];
    let mut len = buf.len();
    // SAFETY: the name is NUL-terminated, buf is writable for len bytes and no new value is passed
    let status = unsafe {
        libc::sysctlbyname(
            c"kern.bootsessionuuid".as_ptr(),
            buf.as_mut_ptr().cast(),
            &mut len,
            std::ptr::null_mut(),
            0,
        )
    };
    if status != 0 {
        return None;
    }
    let text = std::str::from_utf8(buf.get(..len)?).ok()?;
    let id = text.split('\0').next()?.trim();
    (!id.is_empty()).then(|| id.to_string())
}

#[cfg(windows)]
pub fn boot_id() -> Option<String> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .ok()?
        .as_secs();
    // SAFETY: GetTickCount64 takes no arguments and only reads the system tick count
    let up = unsafe { windows_sys::Win32::System::SystemInformation::GetTickCount64() } / 1000;
    let boot = now.checked_sub(up)?;
    Some((boot - boot % 60).to_string())
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "android",
    target_os = "macos",
    target_os = "ios",
    windows
)))]
pub fn boot_id() -> Option<String> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_same_identity_is_the_same_boot() {
        let id = "0f6c2b8e-6a51-4c7f-9d3a-1e2f3a4b5c6d";
        assert!(same_boot(id, Some(id)));
        assert!(same_boot("1794000000", Some("1794000000")));
    }

    #[test]
    fn a_different_identity_is_a_different_boot() {
        assert!(!same_boot(
            "0f6c2b8e-6a51-4c7f-9d3a-1e2f3a4b5c6d",
            Some("7a1d9e04-2b3c-4d5e-8f60-718293a4b5c6")
        ));
    }

    #[test]
    fn an_unknown_identity_never_counts_as_a_restart() {
        assert!(same_boot("0f6c2b8e-6a51-4c7f-9d3a-1e2f3a4b5c6d", None));
        assert!(same_boot(UNKNOWN, None));
        assert!(same_boot(UNKNOWN, Some("1794000000")));
        assert!(same_boot(
            UNKNOWN,
            Some("0f6c2b8e-6a51-4c7f-9d3a-1e2f3a4b5c6d")
        ));
    }

    #[test]
    fn numeric_identities_allow_two_minutes_of_drift() {
        assert!(same_boot("1794000000", Some("1794000060")));
        assert!(same_boot("1794000120", Some("1794000000")));
        assert!(same_boot("1794000000", Some("1794000120")));
        assert!(!same_boot("1794000000", Some("1794000180")));
        assert!(!same_boot("1794000180", Some("1794000000")));
        assert!(!same_boot("1794000000", Some("1794086400")));
    }

    #[test]
    fn non_numeric_identities_compare_exactly() {
        assert!(!same_boot("1794000000", Some("1794000000x")));
        assert!(!same_boot("abc", Some("abd")));
        assert!(!same_boot("-60", Some("60")));
        assert!(same_boot("abc", Some("abc")));
    }

    #[test]
    fn the_boot_identity_is_stable_while_the_machine_runs() {
        let first = boot_id();
        let second = boot_id();
        if cfg!(windows) {
            assert_eq!(first.is_some(), second.is_some());
            if let (Some(first), Some(second)) = (&first, &second) {
                assert!(same_boot(first, Some(second)), "{first} {second}");
            }
        } else {
            assert_eq!(second, first);
        }
        assert_eq!(current(), current());
        if cfg!(any(target_os = "linux", target_os = "macos", windows)) {
            let id = first.expect("this platform has a boot identity");
            assert!(!id.is_empty());
            if cfg!(windows) {
                assert!(id.parse::<u64>().is_ok_and(|secs| secs > 0), "{id}");
            } else {
                assert!(id.parse::<u64>().is_err(), "{id}");
            }
            assert!(same_boot(&id, current()));
        }
    }
}
