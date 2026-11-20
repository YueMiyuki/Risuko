use std::collections::HashSet;
use std::ffi::OsStr;
use std::fmt::Write as _;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, MutexGuard, OnceLock, PoisonError};

use risuko_engine::traits::{write_file_atomically, ConfigDirProvider};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tauri::{AppHandle, Manager};
use tauri_plugin_dialog::{DialogExt, MessageDialogButtons, MessageDialogKind};

use crate::bridge::TauriConfigDir;
use crate::state::AppState;

const APPROVALS_FILE: &str = "completion-script-approvals.json";
const MAX_COMMAND_CHARS: usize = 400;
const MAX_ARGS_CHARS: usize = 600;

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub(super) struct ScriptPair {
    pub(super) command: String,
    pub(super) args: String,
}

impl ScriptPair {
    pub(super) fn new(command: &str, args: &str) -> Self {
        Self {
            command: command.trim().to_string(),
            args: args.to_string(),
        }
    }

    pub(super) fn too_long(&self) -> bool {
        self.command.chars().count() > MAX_COMMAND_CHARS
            || self.args.chars().count() > MAX_ARGS_CHARS
    }

    pub(super) fn refusal(&self) -> String {
        if self.too_long() {
            format!(
                "the completion script is too long to be confirmed: the program may have at most {MAX_COMMAND_CHARS} characters and the arguments at most {MAX_ARGS_CHARS}"
            )
        } else {
            "the completion script was not allowed".to_string()
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Requester {
    User,
    App,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Answer {
    Allowed,
    Declined,
    Unavailable(Unavailable),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Unavailable {
    NoDialogProgram,
    NotShown,
}

#[derive(Serialize, Deserialize)]
struct ApprovalsFile {
    approved: Vec<ScriptPair>,
}

pub(super) struct ApprovalStore {
    file: PathBuf,
    approved: Mutex<HashSet<ScriptPair>>,
    declined: Mutex<HashSet<ScriptPair>>,
    asking: tokio::sync::Mutex<()>,
    warned_unconfirmed: AtomicBool,
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl ApprovalStore {
    pub(super) fn open(file: PathBuf) -> Self {
        let approved = load_approvals(&file);
        Self {
            file,
            approved: Mutex::new(approved),
            declined: Mutex::default(),
            asking: tokio::sync::Mutex::new(()),
            warned_unconfirmed: AtomicBool::new(false),
        }
    }

    fn is_approved(&self, pair: &ScriptPair) -> bool {
        lock(&self.approved).contains(pair)
    }

    fn declined_earlier(&self, pair: &ScriptPair, who: Requester) -> bool {
        who == Requester::App && lock(&self.declined).contains(pair)
    }

    pub(super) async fn ensure_approved<F, Fut>(
        &self,
        pair: &ScriptPair,
        who: Requester,
        ask: F,
    ) -> bool
    where
        F: FnOnce(ScriptPair) -> Fut,
        Fut: Future<Output = Answer>,
    {
        if self.is_approved(pair) {
            return true;
        }
        if pair.too_long() || self.declined_earlier(pair, who) {
            return false;
        }
        let _turn = self.asking.lock().await;
        if self.is_approved(pair) {
            return true;
        }
        if self.declined_earlier(pair, who) {
            return false;
        }
        match ask(pair.clone()).await {
            Answer::Allowed => {
                self.approve(pair);
                true
            }
            Answer::Declined => {
                lock(&self.declined).insert(pair.clone());
                false
            }
            Answer::Unavailable(Unavailable::NoDialogProgram) => {
                if !self.warned_unconfirmed.swap(true, Ordering::Relaxed) {
                    tracing::warn!(
                        "[completion-script] zenity is not installed, so completion scripts run without confirmation"
                    );
                }
                true
            }
            Answer::Unavailable(Unavailable::NotShown) => {
                tracing::warn!(
                    "[completion-script] the confirmation dialog could not be shown, script not run"
                );
                false
            }
        }
    }

    fn approve(&self, pair: &ScriptPair) {
        let mut approved: Vec<ScriptPair> = {
            let mut set = lock(&self.approved);
            set.insert(pair.clone());
            set.iter().cloned().collect()
        };
        lock(&self.declined).remove(pair);
        approved.sort();
        if let Err(e) = save_approvals(&self.file, approved) {
            tracing::warn!("[completion-script] could not store the approval: {e}");
        }
    }
}

fn load_approvals(file: &Path) -> HashSet<ScriptPair> {
    let bytes = match std::fs::read(file) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return HashSet::new(),
        Err(e) => {
            tracing::warn!("[completion-script] could not read {}: {e}", file.display());
            return HashSet::new();
        }
    };
    match serde_json::from_slice::<ApprovalsFile>(&bytes) {
        Ok(parsed) => parsed.approved.into_iter().collect(),
        Err(e) => {
            tracing::warn!(
                "[completion-script] {} could not be parsed ({e}), no script counts as approved",
                file.display()
            );
            HashSet::new()
        }
    }
}

fn save_approvals(file: &Path, approved: Vec<ScriptPair>) -> Result<(), String> {
    let data = serde_json::to_vec_pretty(&ApprovalsFile { approved })
        .map_err(|e| format!("Failed to serialize: {e}"))?;
    write_file_atomically(file, &data)
}

static STORE: OnceLock<ApprovalStore> = OnceLock::new();

fn store(handle: &AppHandle) -> &'static ApprovalStore {
    STORE.get_or_init(|| {
        ApprovalStore::open(
            TauriConfigDir::new(handle)
                .config_dir()
                .join(APPROVALS_FILE),
        )
    })
}

pub(super) async fn ensure_approved(handle: &AppHandle, pair: &ScriptPair, who: Requester) -> bool {
    let asker = handle.clone();
    store(handle)
        .ensure_approved(pair, who, move |pair| ask_with_dialog(asker, pair))
        .await
}

async fn ask_with_dialog(handle: AppHandle, pair: ScriptPair) -> Answer {
    if cfg!(target_os = "linux") && !program_on_path("zenity", std::env::var_os("PATH").as_deref())
    {
        return Answer::Unavailable(Unavailable::NoDialogProgram);
    }
    let prompt = prompt(&pair, prompt_language(&user_locale(&handle)));
    let (tx, rx) = tokio::sync::oneshot::channel();
    handle
        .dialog()
        .message(prompt.body)
        .title("Risuko")
        .kind(MessageDialogKind::Warning)
        .buttons(MessageDialogButtons::OkCancelCustom(
            prompt.allow.to_string(),
            prompt.deny.to_string(),
        ))
        .show(move |allowed| {
            let _ = tx.send(allowed);
        });
    match rx.await {
        Ok(true) => Answer::Allowed,
        Ok(false) => Answer::Declined,
        Err(_) => Answer::Unavailable(Unavailable::NotShown),
    }
}

fn user_locale(handle: &AppHandle) -> String {
    let Some(state) = handle.try_state::<AppState>() else {
        return String::new();
    };
    let Ok(config) = state.config.lock() else {
        return String::new();
    };
    config
        .get_user_config()
        .get("locale")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

fn program_on_path(name: &str, path: Option<&OsStr>) -> bool {
    path.is_some_and(|path| std::env::split_paths(path).any(|dir| is_executable(&dir.join(name))))
}

fn is_executable(path: &Path) -> bool {
    let Ok(meta) = std::fs::metadata(path) else {
        return false;
    };
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        meta.is_file() && meta.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        meta.is_file()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Language {
    English,
    SimplifiedChinese,
    TraditionalChinese,
}

fn prompt_language(locale: &str) -> Language {
    match locale {
        "zh-CN" => Language::SimplifiedChinese,
        "zh-TW" => Language::TraditionalChinese,
        _ => Language::English,
    }
}

struct Prompt {
    body: String,
    allow: &'static str,
    deny: &'static str,
}

fn prompt(pair: &ScriptPair, language: Language) -> Prompt {
    let command = visible(&pair.command);
    let args = (!pair.args.trim().is_empty()).then(|| visible(&pair.args));
    match language {
        Language::English => Prompt {
            body: format!(
                "Risuko is about to run this program when a download finishes or fails:\n\nProgram: {command}\nArguments: {}\n\nAllow it only if you set this up yourself.",
                args.as_deref().unwrap_or("(none)")
            ),
            allow: "Allow",
            deny: "Don't Allow",
        },
        Language::SimplifiedChinese => Prompt {
            body: format!(
                "Risuko 将在下载完成或失败时运行以下程序：\n\n程序：{command}\n参数：{}\n\n只有在这是你自己设置的情况下才允许。",
                args.as_deref().unwrap_or("（无）")
            ),
            allow: "允许",
            deny: "不允许",
        },
        Language::TraditionalChinese => Prompt {
            body: format!(
                "Risuko 將在下載完成或失敗時執行以下程式：\n\n程式：{command}\n參數：{}\n\n只有在這是你自己設定的情況下才允許。",
                args.as_deref().unwrap_or("（無）")
            ),
            allow: "允許",
            deny: "不允許",
        },
    }
}

fn visible(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        match ch {
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            ch if hides_layout(ch) => {
                let _ = write!(out, "\\u{{{:04x}}}", u32::from(ch));
            }
            ch => out.push(ch),
        }
    }
    out
}

fn hides_layout(ch: char) -> bool {
    ch.is_control()
        || matches!(
            ch,
            '\u{061c}'
                | '\u{200b}'..='\u{200f}'
                | '\u{2028}'..='\u{202e}'
                | '\u{2060}'..='\u{2064}'
                | '\u{2066}'..='\u{2069}'
                | '\u{feff}'
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use std::sync::Arc;
    use std::time::Duration;

    fn pair(command: &str, args: &str) -> ScriptPair {
        ScriptPair::new(command, args)
    }

    fn store_in(dir: &Path) -> ApprovalStore {
        ApprovalStore::open(dir.join(APPROVALS_FILE))
    }

    fn answering(
        answer: Answer,
        calls: &AtomicUsize,
    ) -> impl FnOnce(ScriptPair) -> std::future::Ready<Answer> + '_ {
        move |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            std::future::ready(answer)
        }
    }

    async fn slowly(answer: Answer, calls: &AtomicUsize) -> Answer {
        calls.fetch_add(1, Ordering::SeqCst);
        for _ in 0..5 {
            tokio::task::yield_now().await;
        }
        answer
    }

    #[tokio::test]
    async fn an_unknown_pair_is_asked_once_and_stored_when_allowed() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_in(dir.path());
        let calls = AtomicUsize::new(0);
        let notify = pair("/usr/bin/notify", "{path}");

        assert!(
            store
                .ensure_approved(&notify, Requester::App, answering(Answer::Allowed, &calls))
                .await
        );
        assert!(
            store
                .ensure_approved(&notify, Requester::App, answering(Answer::Allowed, &calls))
                .await
        );
        assert!(
            store
                .ensure_approved(&notify, Requester::User, answering(Answer::Allowed, &calls))
                .await
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let saved: Value =
            serde_json::from_slice(&std::fs::read(dir.path().join(APPROVALS_FILE)).unwrap())
                .unwrap();
        assert_eq!(
            saved,
            serde_json::json!({"approved": [{"command": "/usr/bin/notify", "args": "{path}"}]})
        );
    }

    #[tokio::test]
    async fn a_stored_pair_is_not_asked_and_survives_a_new_store() {
        let dir = tempfile::tempdir().unwrap();
        let calls = AtomicUsize::new(0);
        let notify = pair("/usr/bin/notify", "--done");
        let declined = pair("/usr/bin/other", "");
        {
            let store = store_in(dir.path());
            assert!(
                store
                    .ensure_approved(&notify, Requester::User, answering(Answer::Allowed, &calls))
                    .await
            );
            assert!(
                !store
                    .ensure_approved(
                        &declined,
                        Requester::User,
                        answering(Answer::Declined, &calls)
                    )
                    .await
            );
        }
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        let store = store_in(dir.path());
        assert!(
            store
                .ensure_approved(&notify, Requester::App, answering(Answer::Declined, &calls))
                .await
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(
            store
                .ensure_approved(
                    &declined,
                    Requester::App,
                    answering(Answer::Allowed, &calls)
                )
                .await
        );
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn a_declined_pair_is_not_asked_again_by_the_app_but_is_by_the_user() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_in(dir.path());
        let calls = AtomicUsize::new(0);
        let script = pair("/usr/bin/notify", "");

        assert!(
            !store
                .ensure_approved(&script, Requester::App, answering(Answer::Declined, &calls))
                .await
        );
        assert!(
            !store
                .ensure_approved(&script, Requester::App, answering(Answer::Allowed, &calls))
                .await
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(!dir.path().join(APPROVALS_FILE).exists());

        assert!(
            !store
                .ensure_approved(
                    &script,
                    Requester::User,
                    answering(Answer::Declined, &calls)
                )
                .await
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(!dir.path().join(APPROVALS_FILE).exists());

        assert!(
            store
                .ensure_approved(&script, Requester::User, answering(Answer::Allowed, &calls))
                .await
        );
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert!(
            store
                .ensure_approved(&script, Requester::App, answering(Answer::Declined, &calls))
                .await
        );
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn two_argument_templates_for_one_program_are_two_pairs() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_in(dir.path());
        let calls = AtomicUsize::new(0);

        assert!(
            store
                .ensure_approved(
                    &pair("/usr/bin/notify", "{path}"),
                    Requester::User,
                    answering(Answer::Allowed, &calls)
                )
                .await
        );
        assert!(
            !store
                .ensure_approved(
                    &pair("/usr/bin/notify", "{path} --rm"),
                    Requester::App,
                    answering(Answer::Declined, &calls)
                )
                .await
        );
        assert!(
            !store
                .ensure_approved(
                    &pair("/usr/bin/notify", ""),
                    Requester::App,
                    answering(Answer::Declined, &calls)
                )
                .await
        );
        assert!(
            store
                .ensure_approved(
                    &pair("  /usr/bin/notify ", "{path}"),
                    Requester::App,
                    answering(Answer::Declined, &calls)
                )
                .await
        );
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn concurrent_requests_for_one_pair_ask_once() {
        for answer in [Answer::Allowed, Answer::Declined] {
            let dir = tempfile::tempdir().unwrap();
            let store = store_in(dir.path());
            let calls = AtomicUsize::new(0);
            let script = pair("/usr/bin/notify", "{path}");

            let results = futures_util::future::join_all((0..10).map(|_| {
                store.ensure_approved(&script, Requester::App, |_| slowly(answer, &calls))
            }))
            .await;

            assert_eq!(calls.load(Ordering::SeqCst), 1);
            assert_eq!(results, vec![answer == Answer::Allowed; 10]);
        }
    }

    #[tokio::test]
    async fn an_approved_pair_does_not_wait_behind_an_open_question() {
        let dir = tempfile::tempdir().unwrap();
        let store = Arc::new(store_in(dir.path()));
        let calls = AtomicUsize::new(0);
        let known = pair("/usr/bin/known", "");
        assert!(
            store
                .ensure_approved(&known, Requester::User, answering(Answer::Allowed, &calls))
                .await
        );

        let (asked_tx, asked_rx) = tokio::sync::oneshot::channel();
        let (answer_tx, answer_rx) = tokio::sync::oneshot::channel();
        let open = tokio::spawn({
            let store = store.clone();
            async move {
                let new = pair("/usr/bin/new", "");
                store
                    .ensure_approved(&new, Requester::App, move |_| async move {
                        let _ = asked_tx.send(());
                        answer_rx.await.unwrap_or(Answer::Declined)
                    })
                    .await
            }
        });
        asked_rx.await.unwrap();

        let quick = tokio::time::timeout(
            Duration::from_secs(5),
            store.ensure_approved(&known, Requester::App, answering(Answer::Declined, &calls)),
        )
        .await;
        assert_eq!(quick, Ok(true));
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        answer_tx.send(Answer::Allowed).unwrap();
        assert!(open.await.unwrap());
    }

    #[tokio::test]
    async fn a_pair_that_is_too_long_is_refused_without_asking() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_in(dir.path());
        let calls = AtomicUsize::new(0);
        let long_command = pair(&"a".repeat(MAX_COMMAND_CHARS + 1), "");
        let long_args = pair("/usr/bin/notify", &"a".repeat(MAX_ARGS_CHARS + 1));

        for script in [&long_command, &long_args] {
            assert!(script.too_long());
            for who in [Requester::User, Requester::App] {
                assert!(
                    !store
                        .ensure_approved(script, who, answering(Answer::Allowed, &calls))
                        .await
                );
            }
            assert!(script.refusal().contains("too long to be confirmed"));
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        let widest = pair(&"é".repeat(MAX_COMMAND_CHARS), &"é".repeat(MAX_ARGS_CHARS));
        assert!(!widest.too_long());
        assert!(
            store
                .ensure_approved(&widest, Requester::User, answering(Answer::Allowed, &calls))
                .await
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(
            pair("/usr/bin/notify", "").refusal(),
            "the completion script was not allowed"
        );
    }

    #[tokio::test]
    async fn without_zenity_the_script_runs_unconfirmed_and_nothing_is_stored() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_in(dir.path());
        let calls = AtomicUsize::new(0);
        let script = pair("/usr/bin/notify", "");
        let missing = Answer::Unavailable(Unavailable::NoDialogProgram);

        for who in [Requester::App, Requester::App, Requester::User] {
            assert!(
                store
                    .ensure_approved(&script, who, answering(missing, &calls))
                    .await
            );
        }
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert!(!dir.path().join(APPROVALS_FILE).exists());
        assert!(!store_in(dir.path()).is_approved(&script));
    }

    #[tokio::test]
    async fn a_dialog_that_could_not_be_shown_refuses_without_remembering() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_in(dir.path());
        let calls = AtomicUsize::new(0);
        let script = pair("/usr/bin/notify", "");
        let failed = Answer::Unavailable(Unavailable::NotShown);

        assert!(
            !store
                .ensure_approved(&script, Requester::App, answering(failed, &calls))
                .await
        );
        assert!(
            !store
                .ensure_approved(&script, Requester::App, answering(failed, &calls))
                .await
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(!dir.path().join(APPROVALS_FILE).exists());
        assert!(
            store
                .ensure_approved(&script, Requester::App, answering(Answer::Allowed, &calls))
                .await
        );
    }

    #[tokio::test]
    async fn a_corrupt_file_counts_as_empty_and_is_replaced_on_the_next_approval() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join(APPROVALS_FILE);
        std::fs::write(&file, b"{\"approved\": [{\"command\": ").unwrap();
        let store = store_in(dir.path());
        let calls = AtomicUsize::new(0);
        let script = pair("/usr/bin/notify", "{path}");

        assert!(
            !store
                .ensure_approved(&script, Requester::App, answering(Answer::Declined, &calls))
                .await
        );
        assert!(
            store
                .ensure_approved(&script, Requester::User, answering(Answer::Allowed, &calls))
                .await
        );
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(store_in(dir.path()).is_approved(&script));
    }

    #[test]
    fn the_prompt_shows_control_characters_as_escapes() {
        let text = prompt(
            &pair(
                "/bin/sh\nProgram: /usr/bin/safe",
                "-c\t'rm'\r\u{1b}[2J\u{2028}x\u{202e}",
            ),
            Language::English,
        )
        .body;
        assert_eq!(
            text,
            "Risuko is about to run this program when a download finishes or fails:\n\nProgram: /bin/sh\\nProgram: /usr/bin/safe\nArguments: -c\\t'rm'\\r\\u{001b}[2J\\u{2028}x\\u{202e}\n\nAllow it only if you set this up yourself."
        );
        assert_eq!(text.lines().count(), 6);
    }

    #[test]
    fn empty_arguments_show_as_none() {
        let english = prompt(&pair("/usr/bin/notify", " "), Language::English);
        assert!(english.body.contains("\nArguments: (none)\n"));
        assert_eq!((english.allow, english.deny), ("Allow", "Don't Allow"));
        let chinese = prompt(&pair("/usr/bin/notify", ""), Language::SimplifiedChinese);
        assert!(chinese.body.contains("\n参数：（无）\n"));
    }

    #[test]
    fn chinese_is_chosen_only_for_zh_cn_and_zh_tw() {
        assert_eq!(prompt_language("zh-CN"), Language::SimplifiedChinese);
        assert_eq!(prompt_language("zh-TW"), Language::TraditionalChinese);
        for other in ["en-US", "auto", "", "ja", "zh", "zh-cn", "zh-HK"] {
            assert_eq!(prompt_language(other), Language::English, "{other}");
        }

        let script = pair("/usr/bin/notify", "{path}");
        let simplified = prompt(&script, Language::SimplifiedChinese);
        assert!(simplified
            .body
            .contains("程序：/usr/bin/notify\n参数：{path}"));
        assert_eq!((simplified.allow, simplified.deny), ("允许", "不允许"));
        let traditional = prompt(&script, Language::TraditionalChinese);
        assert!(traditional
            .body
            .contains("程式：/usr/bin/notify\n參數：{path}"));
        assert_eq!((traditional.allow, traditional.deny), ("允許", "不允許"));
    }

    #[cfg(unix)]
    #[test]
    fn only_an_executable_file_on_the_path_counts() {
        use std::os::unix::fs::PermissionsExt;

        let dir = tempfile::tempdir().unwrap();
        let empty = tempfile::tempdir().unwrap();
        let program = dir.path().join("zenity");
        std::fs::write(&program, b"#!/bin/sh\n").unwrap();
        let search = std::env::join_paths([empty.path(), dir.path()]).unwrap();

        std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(!program_on_path("zenity", Some(&search)));
        std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(program_on_path("zenity", Some(&search)));
        assert!(!program_on_path("zenity", Some(empty.path().as_os_str())));
        assert!(!program_on_path("zenity", None));
    }
}
