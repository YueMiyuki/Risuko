use std::collections::HashMap;
use std::fs;
use std::path::Path;

#[derive(Default)]
pub struct NetrcEntry {
    pub login: Option<String>,
    pub password: Option<String>,
}

#[derive(Default)]
pub struct Netrc {
    pub machines: HashMap<String, NetrcEntry>,
    pub default: Option<NetrcEntry>,
}

impl Netrc {
    pub fn lookup(&self, host: &str) -> Option<&NetrcEntry> {
        self.machines
            .get(&host.to_ascii_lowercase())
            .or(self.default.as_ref())
    }

    pub fn parse(input: &str) -> Self {
        let mut out = Netrc::default();
        let cleaned = strip_macdefs(input);
        let tokens: Vec<&str> = cleaned.split_whitespace().collect();
        let mut i = 0;
        let mut current_host: Option<String> = None;
        let mut current_entry = NetrcEntry::default();
        let mut in_default = false;

        let flush = |out: &mut Netrc,
                     host: &mut Option<String>,
                     entry: &mut NetrcEntry,
                     is_default: &mut bool| {
            let taken = std::mem::take(entry);
            if *is_default {
                out.default = Some(taken);
                *is_default = false;
            } else if let Some(h) = host.take() {
                out.machines.insert(h.to_ascii_lowercase(), taken);
            }
        };

        while i < tokens.len() {
            match tokens[i] {
                "machine" => {
                    flush(
                        &mut out,
                        &mut current_host,
                        &mut current_entry,
                        &mut in_default,
                    );
                    if i + 1 < tokens.len() {
                        current_host = Some(tokens[i + 1].to_string());
                        i += 2;
                    } else {
                        break;
                    }
                }
                "default" => {
                    flush(
                        &mut out,
                        &mut current_host,
                        &mut current_entry,
                        &mut in_default,
                    );
                    in_default = true;
                    i += 1;
                }
                "login" => {
                    if i + 1 < tokens.len() {
                        current_entry.login = Some(tokens[i + 1].to_string());
                        i += 2;
                    } else {
                        break;
                    }
                }
                "password" | "passwd" => {
                    if i + 1 < tokens.len() {
                        current_entry.password = Some(tokens[i + 1].to_string());
                        i += 2;
                    } else {
                        break;
                    }
                }
                "account" => {
                    i += if i + 1 < tokens.len() { 2 } else { 1 };
                }
                "macdef" => {
                    i += if i + 1 < tokens.len() { 2 } else { 1 };
                }
                _ => {
                    i += 1;
                }
            }
        }
        flush(
            &mut out,
            &mut current_host,
            &mut current_entry,
            &mut in_default,
        );
        out
    }

    pub fn from_path(path: &Path) -> std::io::Result<Self> {
        warn_if_permissions_too_open(path);
        let s = fs::read_to_string(path)?;
        Ok(Self::parse(&s))
    }
}

#[cfg(unix)]
fn warn_if_permissions_too_open(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    if let Ok(meta) = fs::metadata(path) {
        let mode = meta.permissions().mode();
        if mode & 0o077 != 0 {
            tracing::warn!(
                "netrc file {} is group/world-accessible (mode {:o}); credentials are exposed — consider `chmod 600`",
                path.display(),
                mode & 0o777
            );
        }
    }
}

#[cfg(not(unix))]
fn warn_if_permissions_too_open(_path: &Path) {}

fn strip_macdefs(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut in_macro = false;
    for line in input.split_inclusive('\n') {
        let trimmed = line.trim_start();
        if in_macro {
            if line.trim().is_empty() {
                in_macro = false;
                out.push_str(line);
            }
            continue;
        }
        if trimmed
            .split_whitespace()
            .next()
            .is_some_and(|tok| tok == "macdef")
        {
            in_macro = true;
            continue;
        }
        out.push_str(line);
    }
    out
}

pub fn default_netrc_path() -> Option<std::path::PathBuf> {
    #[cfg(windows)]
    {
        if let Ok(home) = std::env::var("USERPROFILE") {
            let p = std::path::PathBuf::from(&home).join("_netrc");
            if p.exists() {
                return Some(p);
            }
            let p = std::path::PathBuf::from(&home).join(".netrc");
            if p.exists() {
                return Some(p);
            }
        }
        None
    }
    #[cfg(not(windows))]
    {
        std::env::var_os("HOME")
            .map(std::path::PathBuf::from)
            .map(|p| p.join(".netrc"))
            .filter(|p| p.exists())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_machine_entry() {
        let txt = "machine example.com login alice password s3cret\n";
        let n = Netrc::parse(txt);
        let e = n.lookup("example.com").expect("entry");
        assert_eq!(e.login.as_deref(), Some("alice"));
        assert_eq!(e.password.as_deref(), Some("s3cret"));
    }

    #[test]
    fn parses_multiple_entries_and_default() {
        let txt = "\
            machine a.example.com login a password 1\n\
            machine b.example.com\n\
              login b\n\
              password 2\n\
            default login guest password g\n";
        let n = Netrc::parse(txt);
        assert_eq!(
            n.lookup("a.example.com").unwrap().login.as_deref(),
            Some("a")
        );
        assert_eq!(
            n.lookup("b.example.com").unwrap().password.as_deref(),
            Some("2")
        );
        assert_eq!(
            n.lookup("c.example.com").unwrap().login.as_deref(),
            Some("guest")
        );
    }

    #[test]
    fn case_insensitive_host_match() {
        let txt = "machine Example.COM login a password p\n";
        let n = Netrc::parse(txt);
        assert!(n.lookup("example.com").is_some());
    }
}
