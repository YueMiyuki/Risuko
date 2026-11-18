use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

use super::types::{is_adc_uri, parse_adc_hub_uri, parse_dchub_file_uri, AdcError};

pub async fn run_adc_download(
    uri: &str,
    total: Arc<AtomicU64>,
    cancel_token: CancellationToken,
) -> Result<PathBuf, String> {
    if !is_adc_uri(uri) {
        return Err(format!("not an ADC/DC URI: {uri}"));
    }

    parse_adc_hub_uri(uri).map_err(|e| e.to_string())?;
    let file = parse_dchub_file_uri(uri).ok_or_else(|| {
        "ADC/DC hub-only URIs (without TTH+size+name) are not yet supported".to_string()
    })?;

    if file.tth.is_none() {
        return Err("ADC/DC URI missing TTH parameter".to_string());
    }
    if file.file_size == 0 {
        return Err("ADC/DC URI missing xl/size parameter".to_string());
    }

    total.store(file.file_size, Ordering::Relaxed);

    if cancel_token.is_cancelled() {
        return Err("cancelled".into());
    }

    Err(AdcError::NoSource.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn run(uri: &str) -> (Result<PathBuf, String>, u64) {
        let total = Arc::new(AtomicU64::new(0));
        let result = run_adc_download(uri, total.clone(), CancellationToken::new()).await;
        (result, total.load(Ordering::Relaxed))
    }

    #[tokio::test]
    async fn rejects_non_adc_uri() {
        assert!(run("magnet:?xt=urn:btih:abcd").await.0.is_err());
    }

    #[tokio::test]
    async fn rejects_hub_only_uri_without_tth() {
        assert!(run("dchub://hub.example.com:411").await.0.is_err());
    }

    #[tokio::test]
    async fn a_complete_link_reports_its_size_then_fails_with_no_source() {
        let (result, total) =
            run("dchub://hub.example.com:411/?TTH=ABCDEFGHIJKLMNOPQRSTUVWXYZ234567ABCDEFG&xl=42&dn=a.bin")
                .await;
        assert_eq!(result.unwrap_err(), AdcError::NoSource.to_string());
        assert_eq!(total, 42);
    }
}
