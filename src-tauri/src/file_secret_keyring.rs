use dbx_core::file_secrets::FileSecretRootKey;
use dbx_core::storage::Storage;
use std::time::Duration;
use zeroize::Zeroizing;

const FILE_SECRET_KEYCHAIN_SERVICE: &str = "com.dbx.app.file-secrets";
const FILE_SECRET_ROOT_KEY_VERSION: u8 = 1;
const FILE_SECRET_ROOT_KEY_SIZE: usize = 32;
const FILE_SECRET_BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(5);
const FILE_SECRET_KEYCHAIN_TIMEOUT: Duration = Duration::from_secs(15);
const FILE_SECRET_UNLOCK_TIMEOUT: Duration = Duration::from_secs(120);

pub async fn unlock_storage(storage: &Storage) -> Result<(), String> {
    let bootstrap = tokio::time::timeout(FILE_SECRET_BOOTSTRAP_TIMEOUT, storage.file_secret_bootstrap())
        .await
        .map_err(|_| "File secret bootstrap timed out; file secrets remain locked".to_string())??;
    let account = bootstrap.key_id;
    let may_create = bootstrap.may_create_key;
    let root_key = tokio::time::timeout(
        FILE_SECRET_KEYCHAIN_TIMEOUT,
        tokio::task::spawn_blocking(move || load_or_create_root_key(&account, may_create)),
    )
    .await
    .map_err(|_| "OS keychain timed out; file secrets remain locked".to_string())?
    .map_err(|_| "OS keychain is unavailable; file secrets remain locked".to_string())??;
    storage.unlock_file_secrets_with_timeout(root_key, FILE_SECRET_UNLOCK_TIMEOUT).await
}

fn load_or_create_root_key(account: &str, may_create: bool) -> Result<FileSecretRootKey, String> {
    let entry = keyring::Entry::new(FILE_SECRET_KEYCHAIN_SERVICE, account)
        .map_err(|_| "OS keychain is unavailable; file secrets remain locked".to_string())?;
    match entry.get_secret() {
        Ok(payload) => {
            let payload = Zeroizing::new(payload);
            decode_root_key(&payload)
        }
        Err(keyring::Error::NoEntry) if may_create => {
            let mut root_key = FileSecretRootKey::new([0_u8; FILE_SECRET_ROOT_KEY_SIZE]);
            getrandom::fill(root_key.as_mut_bytes())
                .map_err(|_| "OS keychain is unavailable; file secrets remain locked".to_string())?;
            let mut payload = Zeroizing::new(Vec::with_capacity(FILE_SECRET_ROOT_KEY_SIZE + 1));
            payload.push(FILE_SECRET_ROOT_KEY_VERSION);
            payload.extend_from_slice(root_key.as_bytes());
            entry
                .set_secret(&payload)
                .map_err(|_| "OS keychain is unavailable; file secrets remain locked".to_string())?;
            Ok(root_key)
        }
        Err(_) => Err("OS keychain is unavailable; file secrets remain locked".to_string()),
    }
}

fn decode_root_key(payload: &[u8]) -> Result<FileSecretRootKey, String> {
    if payload.first() != Some(&FILE_SECRET_ROOT_KEY_VERSION) {
        return Err("OS keychain file secret key has an unsupported version".to_string());
    }
    FileSecretRootKey::try_from_slice(&payload[1..]).map_err(|_| "OS keychain file secret key is invalid".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn root_key_payload_is_versioned_and_length_checked() {
        let mut payload = vec![FILE_SECRET_ROOT_KEY_VERSION];
        payload.extend_from_slice(&[7_u8; FILE_SECRET_ROOT_KEY_SIZE]);
        assert_eq!(decode_root_key(&payload).unwrap().as_bytes(), &[7_u8; FILE_SECRET_ROOT_KEY_SIZE]);
        assert!(decode_root_key(&payload[..payload.len() - 1]).is_err());
        payload[0] = 9;
        assert!(decode_root_key(&payload).is_err());
    }
}
