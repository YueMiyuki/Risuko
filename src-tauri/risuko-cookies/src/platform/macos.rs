use aes::Aes128;
use cipher::{block_padding::Pkcs7, BlockModeDecrypt, KeyIvInit};
use eyre::{bail, Result};
use pbkdf2::pbkdf2_hmac;
use security_framework::passwords::get_generic_password;
use sha1::Sha1;

type Aes128CbcDec = cbc::Decryptor<Aes128>;

pub fn decrypt_value(encrypted: &[u8], key: &[u8]) -> Result<Vec<u8>> {
    if encrypted.len() < 3 {
        bail!("encrypted data too short");
    }

    if &encrypted[..3] == b"v10" {
        decrypt_v10(encrypted, key)
    } else {
        Ok(encrypted.to_vec())
    }
}

fn decrypt_v10(data: &[u8], master_key: &[u8]) -> Result<Vec<u8>> {
    if data.len() < 19 {
        bail!("v10 data too short");
    }

    let iv: [u8; 16] = data[3..19].try_into()?;
    let ciphertext = &data[19..];

    let mut key = [0u8; 16];
    pbkdf2_hmac::<Sha1>(master_key, b"saltysalt", 1003, &mut key);

    let decrypted = Aes128CbcDec::new(&key.into(), &iv.into())
        .decrypt_padded_vec::<Pkcs7>(ciphertext)
        .map_err(|e| eyre::eyre!("aes-cbc decrypt failed: {:?}", e))?;

    Ok(decrypted)
}

pub fn extract_master_key(
    _local_state_path: &std::path::Path,
    (service, account): (&str, &str),
) -> Result<Vec<u8>> {
    get_generic_password(service, account)
        .map(|pw| pw.to_vec())
        .map_err(|e| eyre::eyre!("keychain access failed: {}", e))
}
