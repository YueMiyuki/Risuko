use std::sync::LazyLock;

use serde::Deserialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyScope {
    User,
    System,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Registry {
    user: Vec<String>,
    system: Vec<String>,
    startup_only: Vec<String>,
}

static REGISTRY: LazyLock<Registry> = LazyLock::new(|| {
    serde_json::from_str(include_str!("../../../../src/shared/configKeys.json"))
        .expect("src/shared/configKeys.json is not a valid key registry")
});

fn contains(names: &[String], key: &str) -> bool {
    names.iter().any(|name| name == key)
}

pub fn scope_of(key: &str) -> Option<KeyScope> {
    if contains(&REGISTRY.system, key) {
        Some(KeyScope::System)
    } else if contains(&REGISTRY.user, key) {
        Some(KeyScope::User)
    } else {
        None
    }
}

pub fn startup_only_keys() -> &'static [String] {
    &REGISTRY.startup_only
}

pub fn is_startup_only(key: &str) -> bool {
    contains(&REGISTRY.startup_only, key)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::defaults::{system_defaults, user_defaults};

    #[test]
    fn registry_parses() {
        assert!(!REGISTRY.user.is_empty());
        assert!(!REGISTRY.system.is_empty());
        assert!(!REGISTRY.startup_only.is_empty());
    }

    #[test]
    fn no_name_is_both_user_and_system() {
        for name in &REGISTRY.user {
            assert!(!contains(&REGISTRY.system, name), "{name} is in both lists");
        }
    }

    #[test]
    fn startup_only_names_are_system_names() {
        for name in &REGISTRY.startup_only {
            assert_eq!(scope_of(name), Some(KeyScope::System), "{name}");
        }
    }

    #[test]
    fn startup_only_keys_lists_the_registry_names() {
        let keys = startup_only_keys();
        assert_eq!(keys, REGISTRY.startup_only.as_slice());
        assert!(keys.iter().all(|key| is_startup_only(key)));
    }

    #[test]
    fn scope_of_sorts_names() {
        assert_eq!(scope_of("theme"), Some(KeyScope::User));
        assert_eq!(scope_of("ftp-user"), Some(KeyScope::System));
        assert_eq!(scope_of("no-such-setting"), None);
        assert!(is_startup_only("listen-port"));
        assert!(is_startup_only("rpc-secret"));
        assert!(!is_startup_only("split"));
        assert!(!is_startup_only("theme"));
    }

    #[test]
    fn defaults_live_in_their_registry_scope() {
        for key in system_defaults().keys() {
            if let Some(scope) = scope_of(key) {
                assert_eq!(scope, KeyScope::System, "{key}");
            }
        }
        for key in user_defaults().keys() {
            if let Some(scope) = scope_of(key) {
                assert_eq!(scope, KeyScope::User, "{key}");
            }
        }
    }
}
