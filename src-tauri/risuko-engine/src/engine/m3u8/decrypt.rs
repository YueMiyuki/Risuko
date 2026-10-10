use aes::cipher::{block_padding::Pkcs7, BlockModeDecrypt, KeyIvInit};

type Aes128CbcDec = cbc::Decryptor<aes::Aes128>;

const MAX_KEY_BYTES: usize = 1024;

pub async fn fetch_decryption_key(
    key_uri: &str,
    client: &risuko_http::Client,
) -> Result<[u8; 16], String> {
    let resp = client
        .get(key_uri)
        .send()
        .await
        .map_err(|e| format!("Failed to fetch decryption key: {e}"))?;

    if !resp.status().is_success() {
        return Err(format!("Key fetch failed with status {}", resp.status()));
    }

    let bytes = resp
        .bytes_limited(MAX_KEY_BYTES)
        .await
        .map_err(|e| format!("Failed to read key body: {e}"))?;

    if bytes.len() != 16 {
        return Err(format!(
            "Invalid key length: expected 16 bytes, got {}",
            bytes.len()
        ));
    }

    let mut key = [0u8; 16];
    key.copy_from_slice(&bytes);
    Ok(key)
}

pub fn iv_from_sequence(sequence_number: u64) -> [u8; 16] {
    let val = sequence_number as u128;
    val.to_be_bytes()
}

pub fn decrypt_segment(
    mut data: Vec<u8>,
    key: &[u8; 16],
    iv: &[u8; 16],
) -> Result<Vec<u8>, String> {
    if data.is_empty() {
        return Ok(data);
    }

    if !data.len().is_multiple_of(16) {
        return Err(format!(
            "Ciphertext length {} is not a multiple of 16",
            data.len()
        ));
    }

    let plain_len = Aes128CbcDec::new(key.into(), iv.into())
        .decrypt_padded::<Pkcs7>(&mut data)
        .map_err(|e| format!("AES decryption failed: {e}"))?
        .len();
    data.truncate(plain_len);
    Ok(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn serve_body(len: usize) -> String {
        use tokio::io::AsyncWriteExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {len}\r\nConnection: close\r\n\r\n"
                );
                let _ = stream.write_all(head.as_bytes()).await;
                let _ = stream.write_all(&vec![b'x'; len]).await;
            }
        });
        format!("http://127.0.0.1:{port}/")
    }

    #[tokio::test]
    async fn oversized_key_bodies_are_rejected_before_the_length_check() {
        let url = serve_body(2 * MAX_KEY_BYTES).await;
        let client = risuko_http::Client::builder().build().unwrap();
        let error = fetch_decryption_key(&url, &client).await.unwrap_err();
        assert!(error.contains("Failed to read key body"), "{error}");
    }

    #[test]
    fn test_iv_from_sequence() {
        let iv = iv_from_sequence(0);
        assert_eq!(iv, [0u8; 16]);

        let iv = iv_from_sequence(1);
        assert_eq!(iv[15], 1);
        assert_eq!(iv[0..15], [0u8; 15]);

        let iv = iv_from_sequence(256);
        assert_eq!(iv[14], 1);
        assert_eq!(iv[15], 0);
    }

    #[test]
    fn test_decrypt_empty() {
        let key = [0u8; 16];
        let iv = [0u8; 16];
        let result = decrypt_segment(Vec::new(), &key, &iv);
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn test_decrypt_invalid_length() {
        let key = [0u8; 16];
        let iv = [0u8; 16];
        let result = decrypt_segment(vec![1, 2, 3], &key, &iv);
        assert!(result.is_err());
    }

    #[test]
    fn decrypt_round_trip_in_place() {
        use aes::cipher::{BlockModeEncrypt, KeyIvInit};
        let (key, iv) = ([7u8; 16], [9u8; 16]);
        let plain = b"hello hls segment payload".to_vec();
        let mut buf = plain.clone();
        buf.resize(plain.len() + 16, 0);
        let ct = cbc::Encryptor::<aes::Aes128>::new(&key.into(), &iv.into())
            .encrypt_padded::<Pkcs7>(&mut buf, plain.len())
            .unwrap()
            .to_vec();
        assert_eq!(decrypt_segment(ct, &key, &iv).unwrap(), plain);
    }
}
