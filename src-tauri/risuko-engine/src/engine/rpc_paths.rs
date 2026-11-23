use std::path::{Component, Path, PathBuf};

use same_file::Handle;
use serde_json::{Map, Value};

use super::ftp::is_pasted_private_key;
use super::options::EngineOptions;

pub const ALLOW_ANY_DIR_KEY: &str = "rpc-allow-any-dir";
const SFTP_KEY_OPTION: &str = "sftp-private-key";
pub(crate) const FILE_OPTIONS: [&str; 4] =
    ["load-cookies", "netrc-path", "cookies", SFTP_KEY_OPTION];
const FOLDER_SETTINGS: [&str; 3] = ["dir", "file-category-dirs", "task-routing-rules"];
const SWITCH_HINT: &str =
    "Switch on \"Let RPC clients use any local path\" in the advanced settings to allow it.";

pub(crate) fn allows_any_dir(options: &EngineOptions) -> bool {
    options.get_bool(ALLOW_ANY_DIR_KEY).unwrap_or(false)
}

pub(crate) fn download_roots(options: &EngineOptions) -> Vec<PathBuf> {
    absolute_paths(std::iter::once(options.dir()).chain(options.file_category_dirs().into_values()))
}

pub(crate) fn allowed_roots(options: &EngineOptions) -> Vec<PathBuf> {
    let mut roots = download_roots(options);
    roots.extend(absolute_paths(
        options
            .task_routing_rules()
            .into_iter()
            .map(|rule| rule.dir),
    ));
    roots
}

pub(crate) fn check_save_dir(dir: &str, roots: &[PathBuf]) -> Result<(), String> {
    if [dir, dir.trim()]
        .into_iter()
        .all(|candidate| dir_is_inside(candidate, roots))
    {
        Ok(())
    } else {
        Err(dir_refusal(dir))
    }
}

pub(crate) fn check_out_name(out: &str) -> Result<(), String> {
    let escapes = has_parent_step(out)
        || [out, out.trim()].into_iter().any(|name| {
            let path = Path::new(name);
            name.starts_with(['/', '\\'])
                || starts_at_home(name)
                || path.has_root()
                || matches!(path.components().next(), Some(Component::Prefix(_)))
        });
    if escapes {
        Err(out_refusal(out))
    } else {
        Ok(())
    }
}

pub(crate) fn check_task_options(
    options: &Map<String, Value>,
    roots: &[PathBuf],
) -> Result<(), String> {
    if let Some(value) = options.get("dir") {
        match value.as_str() {
            Some(dir) => check_save_dir(dir, roots)?,
            None => return Err(dir_refusal(&value.to_string())),
        }
    }
    if let Some(value) = options.get("out") {
        match value.as_str() {
            Some(out) => check_out_name(out)?,
            None => return Err(out_refusal(&value.to_string())),
        }
    }
    Ok(())
}

pub(crate) fn check_file_options(options: &Map<String, Value>) -> Result<(), String> {
    for key in FILE_OPTIONS {
        let shown = match options.get(key) {
            None => continue,
            Some(Value::String(path)) if path.is_empty() => continue,
            Some(Value::String(key_text))
                if key == SFTP_KEY_OPTION && is_pasted_private_key(key_text) =>
            {
                continue
            }
            Some(Value::String(path)) => path.clone(),
            Some(value) => value.to_string(),
        };
        let shown = if is_pasted_private_key(&shown) {
            "the given value".to_string()
        } else {
            shown
        };
        return Err(format!(
            "Reading {shown} is not allowed for RPC clients: {key} names a file on this computer. {SWITCH_HINT}"
        ));
    }
    Ok(())
}

pub(crate) fn check_global_options(
    options: &Map<String, Value>,
    allow_any_dir: bool,
) -> Result<(), String> {
    if options.contains_key(ALLOW_ANY_DIR_KEY) {
        return Err(format!(
            "{ALLOW_ANY_DIR_KEY} can only be changed in the app's settings or the settings file, not over RPC"
        ));
    }
    if allow_any_dir {
        return Ok(());
    }
    if let Some(key) = options
        .keys()
        .find(|key| FOLDER_SETTINGS.contains(&key.as_str()))
    {
        return Err(format!(
            "Changing {key} is not allowed for RPC clients: it sets the download folders. {SWITCH_HINT}"
        ));
    }
    check_file_options(options)
}

fn dir_refusal(dir: &str) -> String {
    format!(
        "Saving to {dir} is not allowed for RPC clients: it is outside the download folders. {SWITCH_HINT}"
    )
}

fn out_refusal(out: &str) -> String {
    format!(
        "Saving as {out} is not allowed for RPC clients: the name leads outside the save folder. {SWITCH_HINT}"
    )
}

fn absolute_paths(dirs: impl Iterator<Item = String>) -> Vec<PathBuf> {
    dirs.map(|dir| PathBuf::from(dir.trim()))
        .filter(|path| path.is_absolute())
        .collect()
}

fn starts_at_home(name: &str) -> bool {
    name.strip_prefix('~')
        .is_some_and(|rest| rest.is_empty() || rest.starts_with(['/', '\\']))
}

fn has_parent_step(path: &str) -> bool {
    path.split(['/', '\\']).any(|part| {
        let part = part.trim();
        part.matches('.').count() >= 2 && part.chars().all(|c| c == '.' || c.is_whitespace())
    })
}

// Win32 trims trailing dots and spaces from the last name only, and resolve() probes every ancestor as a last name
fn has_trimmed_name(path: &Path) -> bool {
    path.components().any(|part| match part {
        Component::Normal(name) => name.to_string_lossy().ends_with(['.', ' ']),
        _ => false,
    })
}

fn dir_is_inside(dir: &str, roots: &[PathBuf]) -> bool {
    let path = Path::new(dir);
    if !path.is_absolute() || has_parent_step(dir) || (cfg!(windows) && has_trimmed_name(path)) {
        return false;
    }
    let Some((existing, resolved)) = resolve(path) else {
        return false;
    };
    let roots: Vec<PathBuf> = roots
        .iter()
        .filter_map(|root| resolve(root))
        .map(|(_, root)| root)
        .collect();
    roots.iter().any(|root| resolved.starts_with(root)) || is_same_dir_or_below(&existing, &roots)
}

fn is_same_dir_or_below(existing: &Path, roots: &[PathBuf]) -> bool {
    let roots: Vec<Handle> = roots.iter().filter_map(|root| open_dir(root)).collect();
    !roots.is_empty()
        && existing
            .ancestors()
            .filter_map(open_dir)
            .any(|dir| roots.contains(&dir))
}

fn open_dir(path: &Path) -> Option<Handle> {
    if path.is_dir() {
        Handle::from_path(path).ok()
    } else {
        None
    }
}

fn resolve(path: &Path) -> Option<(PathBuf, PathBuf)> {
    let mut existing = path;
    let mut missing = Vec::new();
    while std::fs::symlink_metadata(existing).is_err() {
        missing.push(existing.file_name()?);
        existing = existing.parent()?;
    }
    let real = std::fs::canonicalize(existing).ok()?;
    let resolved = missing
        .into_iter()
        .rev()
        .fold(real.clone(), |acc, part| acc.join(part));
    Some((real, resolved))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::TempDir;

    fn text(path: &Path) -> String {
        path.to_string_lossy().into_owned()
    }

    fn download_root() -> (TempDir, PathBuf) {
        let tmp = TempDir::new().unwrap();
        let root = tmp.path().join("dl");
        std::fs::create_dir_all(root.join("sub")).unwrap();
        (tmp, root)
    }

    fn options(global: Value) -> EngineOptions {
        EngineOptions {
            global: global.as_object().cloned().unwrap(),
        }
    }

    #[test]
    fn folders_inside_or_equal_to_a_root_pass() {
        let (_tmp, root) = download_root();
        let roots = vec![root.clone()];
        assert!(check_save_dir(&text(&root), &roots).is_ok());
        assert!(check_save_dir(&text(&root.join("sub")), &roots).is_ok());
        assert!(check_save_dir(&format!("{}/", text(&root)), &roots).is_ok());
    }

    #[test]
    fn folders_that_do_not_exist_yet_are_judged_by_their_parents() {
        let (tmp, root) = download_root();
        let roots = vec![root.clone()];
        assert!(check_save_dir(&text(&root.join("new").join("deeper")), &roots).is_ok());

        let later = tmp.path().join("later");
        let roots = vec![later.clone()];
        assert!(check_save_dir(&text(&later.join("sub")), &roots).is_ok());
        assert!(check_save_dir(&text(&tmp.path().join("later-evil")), &roots).is_err());
    }

    #[test]
    fn a_sibling_with_the_same_prefix_is_outside() {
        let (tmp, root) = download_root();
        let evil = tmp.path().join("dl-evil");
        std::fs::create_dir_all(&evil).unwrap();
        let roots = vec![root];
        let error = check_save_dir(&text(&evil), &roots).unwrap_err();
        assert!(error.contains(&text(&evil)), "{error}");
        assert!(check_save_dir(&text(&evil.join("x")), &roots).is_err());
        assert!(check_save_dir(&text(tmp.path()), &roots).is_err());
    }

    #[test]
    fn relative_and_empty_folders_are_refused() {
        let (_tmp, root) = download_root();
        let roots = vec![root.clone()];
        for dir in ["dl", "dl/sub", "./sub", "", "   "] {
            assert!(check_save_dir(dir, &roots).is_err(), "{dir:?}");
        }
        assert!(check_save_dir(&format!(" {}", text(&root)), &roots).is_err());
    }

    #[test]
    fn parent_steps_are_refused_even_when_they_land_inside() {
        let (_tmp, root) = download_root();
        let roots = vec![root.clone()];
        let base = text(&root);
        for dir in [
            format!("{base}/sub/.."),
            format!("{base}/sub/../sub"),
            format!("{base}/.."),
            format!("{base}/../dl"),
            format!("{base}/.. "),
            format!("{base}/sub/..."),
            format!("{base}\\..\\x"),
        ] {
            assert!(check_save_dir(&dir, &roots).is_err(), "{dir}");
        }
    }

    #[test]
    fn whitespace_around_a_root_does_not_widen_it() {
        let (_tmp, root) = download_root();
        let roots = vec![root.clone()];
        assert!(check_save_dir(&format!("{} ", text(&root)), &roots).is_err());
        assert!(check_save_dir(&format!("{} /x", text(&root)), &roots).is_err());
    }

    #[test]
    fn names_ending_in_a_dot_or_space_are_spotted() {
        for path in ["/dl /x", "/dl.", "/dl/sub ", "/dl../x", "/dl/new./x"] {
            assert!(has_trimmed_name(Path::new(path)), "{path:?}");
        }
        for path in ["/dl/x", "/dl/.hidden", "/dl/a. b/x", "/dl/./x", "/"] {
            assert!(!has_trimmed_name(Path::new(path)), "{path:?}");
        }
    }

    #[test]
    fn roots_and_folders_compare_after_resolving() {
        let (_tmp, root) = download_root();
        let canonical = std::fs::canonicalize(&root).unwrap();
        assert!(check_save_dir(&text(&root.join("sub")), std::slice::from_ref(&canonical)).is_ok());
        assert!(check_save_dir(&text(&canonical.join("sub")), std::slice::from_ref(&root)).is_ok());
    }

    #[cfg(unix)]
    #[test]
    fn a_symlink_inside_a_root_that_points_outside_is_refused() {
        let (tmp, root) = download_root();
        let outside = tmp.path().join("outside");
        std::fs::create_dir_all(&outside).unwrap();
        std::os::unix::fs::symlink(&outside, root.join("link")).unwrap();
        std::os::unix::fs::symlink(tmp.path().join("missing"), root.join("dangling")).unwrap();
        let roots = vec![root.clone()];
        assert!(check_save_dir(&text(&root.join("link")), &roots).is_err());
        assert!(check_save_dir(&text(&root.join("link").join("new")), &roots).is_err());
        assert!(check_save_dir(&text(&root.join("dangling")), &roots).is_err());
        assert!(check_save_dir(&text(&root.join("dangling").join("new")), &roots).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn a_symlink_inside_a_root_that_stays_inside_passes() {
        let (_tmp, root) = download_root();
        std::os::unix::fs::symlink(root.join("sub"), root.join("alias")).unwrap();
        assert!(check_save_dir(&text(&root.join("alias")), std::slice::from_ref(&root)).is_ok());
    }

    fn ignores_case(tmp: &Path) -> bool {
        let probe = tmp.join("Probe");
        std::fs::create_dir(&probe).unwrap();
        same_file::is_same_file(&probe, tmp.join("probe")).unwrap_or(false)
    }

    #[test]
    fn identity_finds_the_root_among_the_existing_ancestors() {
        let (tmp, root) = download_root();
        let evil = tmp.path().join("dl-evil");
        std::fs::create_dir_all(&evil).unwrap();
        let real = std::fs::canonicalize(&root).unwrap();
        let parent = real.parent().unwrap();
        let roots = vec![real.clone()];
        assert!(is_same_dir_or_below(&real, &roots));
        assert!(is_same_dir_or_below(&real.join("sub"), &roots));
        assert!(!is_same_dir_or_below(&parent.join("dl-evil"), &roots));
        assert!(!is_same_dir_or_below(parent, &roots));

        let missing = vec![parent.join("missing")];
        assert!(!is_same_dir_or_below(parent, &missing));
        assert!(!is_same_dir_or_below(&real.join("sub"), &missing));
    }

    #[test]
    fn identity_matches_a_root_spelled_in_other_case_where_case_is_ignored() {
        let (tmp, root) = download_root();
        if !ignores_case(tmp.path()) {
            eprintln!("skipped: {} tells letter case apart", tmp.path().display());
            return;
        }
        std::fs::create_dir_all(tmp.path().join("dl-evil")).unwrap();
        let real = std::fs::canonicalize(&root).unwrap();
        let parent = real.parent().unwrap();
        let shouted = vec![parent.join("DL")];
        assert!(is_same_dir_or_below(&real, &shouted));
        assert!(is_same_dir_or_below(&real.join("sub"), &shouted));
        assert!(!is_same_dir_or_below(&parent.join("dl-evil"), &shouted));
        assert!(!is_same_dir_or_below(parent, &shouted));
    }

    #[test]
    fn the_download_folder_in_other_case_is_inside_where_case_is_ignored() {
        let (tmp, root) = download_root();
        if !ignores_case(tmp.path()) {
            eprintln!("skipped: {} tells letter case apart", tmp.path().display());
            return;
        }
        std::fs::create_dir_all(tmp.path().join("dl-evil")).unwrap();
        let shouted = tmp.path().join("DL");
        let roots = vec![root.clone()];
        assert!(check_save_dir(&text(&shouted), &roots).is_ok());
        assert!(check_save_dir(&text(&shouted.join("SUB")), &roots).is_ok());
        assert!(check_save_dir(&text(&shouted.join("new").join("deeper")), &roots).is_ok());
        assert!(check_save_dir(&text(&tmp.path().join("DL-EVIL")), &roots).is_err());

        let shouted_roots = vec![shouted];
        assert!(check_save_dir(&text(&root.join("new")), &shouted_roots).is_ok());
        assert!(check_save_dir(&text(tmp.path()), &shouted_roots).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn a_fifo_outside_the_roots_is_refused_without_opening_it() {
        let (tmp, root) = download_root();
        let fifo = tmp.path().join("fifo");
        let made = std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap();
        assert!(made.success());
        let (sender, receiver) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            let roots = vec![root];
            let refused = [text(&fifo), text(&fifo.join("x"))]
                .map(|dir| check_save_dir(&dir, &roots).is_err());
            let _ = sender.send(refused);
        });
        let refused = receiver
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("checking a fifo must not block");
        assert_eq!(refused, [true, true]);
    }

    #[test]
    fn a_root_that_does_not_exist_is_not_matched_by_identity() {
        let (tmp, _root) = download_root();
        let other = tmp.path().join("other");
        std::fs::create_dir_all(&other).unwrap();
        let roots = vec![tmp.path().join("missing")];
        assert!(check_save_dir(&text(tmp.path()), &roots).is_err());
        assert!(check_save_dir(&text(&other), &roots).is_err());
        assert!(check_save_dir(&text(&tmp.path().join("MISSING").join("x")), &roots).is_err());
    }

    #[test]
    fn out_names_stay_inside_the_save_folder() {
        for out in [
            "file.bin",
            "sub/name.bin",
            "a..b.bin",
            ".hidden",
            "",
            "~x.bin",
            "sub/~/x.bin",
            "~user/x.bin",
        ] {
            assert!(check_out_name(out).is_ok(), "{out:?}");
        }
        for out in [
            "/etc/passwd",
            " /etc/passwd",
            "../x.bin",
            "..",
            "sub/../../x.bin",
            "sub/.. ",
            "..\\x.bin",
            "\\x.bin",
            "~",
            "~/x.mp4",
            " ~/x.mp4",
            "~\\x.mp4",
            "~/.config/autostart/x.desktop",
        ] {
            let error = check_out_name(out).unwrap_err();
            assert!(error.contains("not allowed for RPC clients"), "{error}");
        }
    }

    #[cfg(windows)]
    #[test]
    fn out_names_cannot_name_a_drive() {
        for out in ["C:\\x.bin", "C:x.bin", "\\\\server\\share\\x.bin", "/x.bin"] {
            assert!(check_out_name(out).is_err(), "{out:?}");
        }
    }

    #[test]
    fn category_and_routing_folders_count_as_roots() {
        let data = if cfg!(windows) { "C:\\data" } else { "/data" };
        let dir = |name: &str| text(&Path::new(data).join(name));
        let options = options(json!({
            "dir": dir("dl"),
            "file-category-dirs": {
                "video": format!(" {} ", dir("video")),
                "music": "",
                "image": "relative/images"
            },
            "task-routing-rules": [
                {"id": "a", "label": "ISO", "pattern": "*.iso", "dir": dir("iso")},
                {"id": "b", "label": "Off", "pattern": "*.x", "dir": dir("off"), "enabled": false},
                {"id": "c", "label": "Empty", "pattern": "*.y", "dir": ""}
            ]
        }));
        let mut base = download_roots(&options);
        base.sort();
        assert_eq!(base, ["dl", "video"].map(|name| PathBuf::from(dir(name))));
        let mut all = allowed_roots(&options);
        all.sort();
        assert_eq!(
            all,
            ["dl", "iso", "off", "video"].map(|name| PathBuf::from(dir(name)))
        );
    }

    #[test]
    fn a_relative_download_folder_is_not_a_root() {
        assert!(allowed_roots(&options(json!({"dir": "."}))).is_empty());
        assert!(allowed_roots(&options(json!({}))).is_empty());
    }

    #[test]
    fn task_options_check_dir_and_out_only_when_present() {
        let (tmp, root) = download_root();
        let roots = vec![root.clone()];
        let ok = |value: Value| check_task_options(value.as_object().unwrap(), &roots);
        assert!(ok(json!({})).is_ok());
        assert!(ok(json!({"split": "4"})).is_ok());
        assert!(ok(json!({"dir": text(&root.join("sub")), "out": "a.bin"})).is_ok());

        let outside = text(&tmp.path().join("elsewhere"));
        let error = ok(json!({"dir": outside})).unwrap_err();
        assert_eq!(
            error,
            format!(
                "Saving to {outside} is not allowed for RPC clients: it is outside the download folders. Switch on \"Let RPC clients use any local path\" in the advanced settings to allow it."
            )
        );
        assert!(ok(json!({"out": "../a.bin"})).is_err());
        assert!(ok(json!({"dir": text(&root), "out": "/tmp/a.bin"})).is_err());
        for bad in [json!(null), json!(7), json!([text(&root)]), json!("")] {
            assert!(ok(json!({"dir": bad.clone()})).is_err(), "{bad}");
        }
        for bad in [json!(null), json!(7), json!(["a.bin"])] {
            assert!(ok(json!({"out": bad.clone()})).is_err(), "{bad}");
        }
    }

    #[test]
    fn file_options_pass_only_when_absent_or_empty() {
        let check = |value: Value| check_file_options(value.as_object().unwrap());
        assert!(check(json!({})).is_ok());
        assert!(check(json!({"dir": "/elsewhere", "header": ["a: b"]})).is_ok());
        for key in FILE_OPTIONS {
            assert!(check(json!({ key: "" })).is_ok(), "{key}");

            let error = check(json!({ key: "/home/me/secrets.txt" })).unwrap_err();
            assert_eq!(
                error,
                format!(
                    "Reading /home/me/secrets.txt is not allowed for RPC clients: {key} names a file on this computer. Switch on \"Let RPC clients use any local path\" in the advanced settings to allow it."
                )
            );
            for bad in [json!(" "), json!(null), json!(7), json!(["/a"]), json!({})] {
                assert!(check(json!({ key: bad.clone() })).is_err(), "{key} {bad}");
            }
        }
        let error = check(json!({"netrc-path": "", "load-cookies": 7})).unwrap_err();
        assert!(error.starts_with("Reading 7 "), "{error}");
    }

    const PASTED_KEY: &str =
        "-----BEGIN OPENSSH PRIVATE KEY-----\nAAAA\n-----END OPENSSH PRIVATE KEY-----";

    #[test]
    fn an_sftp_key_file_is_refused_but_a_pasted_key_passes_unechoed() {
        let check = |value: Value| check_file_options(value.as_object().unwrap());
        let error = check(json!({"sftp-private-key": "~/.ssh/id_ed25519"})).unwrap_err();
        assert_eq!(
            error,
            "Reading ~/.ssh/id_ed25519 is not allowed for RPC clients: sftp-private-key names a file on this computer. Switch on \"Let RPC clients use any local path\" in the advanced settings to allow it."
        );
        let global = json!({"sftp-private-key": "/home/me/.ssh/id_rsa"});
        assert!(check_global_options(global.as_object().unwrap(), false).is_err());
        assert!(check_global_options(global.as_object().unwrap(), true).is_ok());

        let pasted = json!({"sftp-private-key": PASTED_KEY});
        assert!(check(pasted.clone()).is_ok());
        assert!(check_global_options(pasted.as_object().unwrap(), false).is_ok());

        for refused in [
            json!({"sftp-private-key": [PASTED_KEY]}),
            json!({"sftp-private-key": {"key": PASTED_KEY}}),
            json!({"load-cookies": PASTED_KEY}),
        ] {
            let error = check(refused.clone()).unwrap_err();
            assert!(!error.contains("PRIVATE KEY"), "{error}");
            assert!(!error.contains("AAAA"), "{error}");
        }
    }

    #[test]
    fn global_file_options_need_the_switch() {
        let changes = json!({"max-overall-download-limit": "1", "netrc-path": "/home/me/.netrc"});
        let changes = changes.as_object().unwrap();
        let error = check_global_options(changes, false).unwrap_err();
        assert!(error.contains("netrc-path names a file"), "{error}");
        assert!(check_global_options(changes, true).is_ok());
        let cleared = json!({"load-cookies": ""});
        assert!(check_global_options(cleared.as_object().unwrap(), false).is_ok());
    }

    #[test]
    fn the_switch_is_off_unless_the_system_settings_turn_it_on() {
        let defaults = crate::config::defaults::system_defaults();
        assert_eq!(defaults.get(ALLOW_ANY_DIR_KEY), Some(&json!(false)));
        assert!(!allows_any_dir(&options(json!({}))));
        assert!(!allows_any_dir(&options(json!({ALLOW_ANY_DIR_KEY: false}))));
        assert!(allows_any_dir(&options(json!({ALLOW_ANY_DIR_KEY: true}))));
        assert!(allows_any_dir(&options(json!({ALLOW_ANY_DIR_KEY: "true"}))));

        let user = json!({"engine-overrides": {ALLOW_ANY_DIR_KEY: true}});
        let from_overrides = EngineOptions::from_config(&Map::new(), user.as_object().unwrap());
        assert!(!allows_any_dir(&from_overrides));
    }
}
