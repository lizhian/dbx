use std::collections::BTreeMap;
use std::fmt;
use std::sync::OnceLock;

use aes_gcm::aead::{Aead, AeadCore, KeyInit, OsRng, Payload};
use aes_gcm::{Aes256Gcm, Nonce};
use regex::Regex;
use serde::{de::Error as _, Deserialize, Deserializer, Serialize};
use sha2::{Digest, Sha256};
use zeroize::{Zeroize, ZeroizeOnDrop, Zeroizing};

pub const FILE_SECRETS_LOCKED: &str = "FILE_SECRETS_LOCKED";
pub const FILE_SECRET_ENVELOPE_VERSION: i64 = 1;
pub const FILE_SECRET_NONCE_SIZE: usize = 12;

pub struct FileSecretRootKey(Zeroizing<[u8; 32]>);

impl FileSecretRootKey {
    pub fn new(value: [u8; 32]) -> Self {
        Self(Zeroizing::new(value))
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        self.0.as_ref().try_into().expect("root key length is fixed")
    }

    pub fn as_mut_bytes(&mut self) -> &mut [u8; 32] {
        self.0.as_mut().try_into().expect("root key length is fixed")
    }

    pub fn try_from_slice(value: &[u8]) -> Result<Self, String> {
        if value.len() != 32 {
            return Err("File secret root key has an invalid length".to_string());
        }
        let mut key = Self::new([0_u8; 32]);
        key.0.as_mut().copy_from_slice(value);
        Ok(key)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileSecretProtocol {
    Ftp,
    Sftp,
    S3,
    Webdav,
    Webhdfs,
    HdfsNative,
}

impl FileSecretProtocol {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ftp => "ftp",
            Self::Sftp => "sftp",
            Self::S3 => "s3",
            Self::Webdav => "webdav",
            Self::Webhdfs => "webhdfs",
            Self::HdfsNative => "hdfs_native",
        }
    }

    pub fn from_storage(value: &str) -> Result<Self, String> {
        match value {
            "ftp" => Ok(Self::Ftp),
            "sftp" => Ok(Self::Sftp),
            "s3" => Ok(Self::S3),
            "webdav" => Ok(Self::Webdav),
            "webhdfs" => Ok(Self::Webhdfs),
            "hdfs_native" => Ok(Self::HdfsNative),
            _ => Err("Stored file secret protocol is invalid".to_string()),
        }
    }

    const fn code(self) -> u8 {
        match self {
            Self::Ftp => 1,
            Self::Sftp => 2,
            Self::S3 => 3,
            Self::Webdav => 4,
            Self::Webhdfs => 5,
            Self::HdfsNative => 6,
        }
    }

    pub const fn allows(self, key: FileSecretKey) -> bool {
        match self {
            Self::Ftp => matches!(key, FileSecretKey::Password),
            Self::Sftp => matches!(key, FileSecretKey::SftpPrivateKey | FileSecretKey::SftpPrivateKeyPassphrase),
            Self::S3 => matches!(
                key,
                FileSecretKey::S3AccessKeyId | FileSecretKey::S3SecretAccessKey | FileSecretKey::S3SessionToken
            ),
            Self::Webdav => matches!(key, FileSecretKey::Password | FileSecretKey::WebdavToken),
            Self::Webhdfs => matches!(key, FileSecretKey::WebhdfsDelegationToken),
            Self::HdfsNative => false,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum FileSecretKey {
    Password,
    S3AccessKeyId,
    S3SecretAccessKey,
    S3SessionToken,
    WebdavToken,
    SftpPrivateKey,
    SftpPrivateKeyPassphrase,
    WebhdfsDelegationToken,
}

impl FileSecretKey {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Password => "password",
            Self::S3AccessKeyId => "access_key_id",
            Self::S3SecretAccessKey => "secret_access_key",
            Self::S3SessionToken => "session_token",
            Self::WebdavToken => "webdav_token",
            Self::SftpPrivateKey => "sftp_private_key",
            Self::SftpPrivateKeyPassphrase => "sftp_private_key_passphrase",
            Self::WebhdfsDelegationToken => "webhdfs_delegation_token",
        }
    }

    pub fn from_storage(value: &str) -> Result<Self, String> {
        match value {
            "password" => Ok(Self::Password),
            "access_key_id" => Ok(Self::S3AccessKeyId),
            "secret_access_key" => Ok(Self::S3SecretAccessKey),
            "session_token" => Ok(Self::S3SessionToken),
            "webdav_token" => Ok(Self::WebdavToken),
            "sftp_private_key" => Ok(Self::SftpPrivateKey),
            "sftp_private_key_passphrase" => Ok(Self::SftpPrivateKeyPassphrase),
            "webhdfs_delegation_token" => Ok(Self::WebhdfsDelegationToken),
            _ => Err("Stored file secret key is invalid".to_string()),
        }
    }

    const fn code(self) -> u8 {
        match self {
            Self::Password => 1,
            Self::S3AccessKeyId => 2,
            Self::S3SecretAccessKey => 3,
            Self::S3SessionToken => 4,
            Self::WebdavToken => 5,
            Self::SftpPrivateKey => 6,
            Self::SftpPrivateKeyPassphrase => 7,
            Self::WebhdfsDelegationToken => 8,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct FileSecretScopeDigest([u8; 32]);

impl FileSecretScopeDigest {
    pub fn from_scope(scope: &str) -> Self {
        Self(Sha256::digest(scope.as_bytes()).into())
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        let value: [u8; 32] = bytes.try_into().map_err(|_| "Stored file secret scope digest is invalid".to_string())?;
        Ok(Self(value))
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct FileSecret(String);

impl FileSecret {
    pub fn new(value: String) -> Result<Self, String> {
        if value.is_empty() {
            return Err("File connection secrets cannot be empty; use the explicit clear operation".to_string());
        }
        Ok(Self(value))
    }

    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl std::ops::Deref for FileSecret {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.expose_secret()
    }
}

impl fmt::Debug for FileSecret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("[REDACTED]")
    }
}

impl<'de> Deserialize<'de> for FileSecret {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let mut value = Zeroizing::<String>::deserialize(deserializer)?;
        Self::new(std::mem::take(&mut *value)).map_err(D::Error::custom)
    }
}

impl rusqlite::types::FromSql for FileSecret {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        let value = <String as rusqlite::types::FromSql>::column_result(value)?;
        Self::new(value).map_err(|error| rusqlite::types::FromSqlError::Other(error.into()))
    }
}

#[derive(Clone, Default)]
pub struct FileSecretRedactor {
    variants: Vec<Zeroizing<String>>,
}

impl FileSecretRedactor {
    pub fn from_secrets<'a>(secrets: impl IntoIterator<Item = &'a FileSecret>) -> Self {
        let mut variants = Vec::new();
        for secret in secrets {
            let raw = secret.expose_secret();
            if raw.is_empty() {
                continue;
            }
            let mut candidates = vec![
                raw.to_string(),
                percent_encoding::utf8_percent_encode(raw, percent_encoding::NON_ALPHANUMERIC).to_string(),
                url_form_encode(raw),
            ];
            if raw.contains("\r\n") {
                candidates.push(raw.replace("\r\n", "\n"));
            } else if raw.contains('\n') {
                candidates.push(raw.replace('\n', "\r\n"));
            }
            for variant in candidates {
                if !variants.iter().any(|existing: &Zeroizing<String>| existing.as_str() == variant) {
                    variants.push(Zeroizing::new(variant));
                }
            }
        }
        Self { variants }
    }

    pub fn redact(&self, message: impl AsRef<str>) -> RedactedFileText {
        let mut message = redact_sftp_key_paths(message.as_ref());
        message = redact_basic_authorization(&message);
        for variant in &self.variants {
            message = message.replace(variant.as_str(), "[REDACTED]");
        }
        RedactedFileText(message)
    }
}

#[derive(Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct RedactedFileText(String);

impl RedactedFileText {
    pub fn from_static(message: &'static str) -> Self {
        Self(message.to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub(crate) fn into_inner(self) -> String {
        self.0
    }
}

impl fmt::Debug for RedactedFileText {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("RedactedFileText").field(&self.0).finish()
    }
}

impl fmt::Display for RedactedFileText {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

fn url_form_encode(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(char::from(byte));
            }
            b' ' => encoded.push('+'),
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}

fn redact_sftp_key_paths(message: &str) -> String {
    static TEMP_KEY_PATH: OnceLock<Regex> = OnceLock::new();
    TEMP_KEY_PATH
        .get_or_init(|| {
            Regex::new(r#"(?i)(?:[A-Z]:)?[^\s"'<>]*dbx-sftp-keys-[^\s"'<>/\\]+[/\\][^\s"'<>]+"#)
                .expect("SFTP temporary-key redaction regex must compile")
        })
        .replace_all(message, "[SFTP_KEY_MATERIAL]")
        .into_owned()
}

fn redact_basic_authorization(message: &str) -> String {
    static BASIC_AUTHORIZATION: OnceLock<Regex> = OnceLock::new();
    BASIC_AUTHORIZATION
        .get_or_init(|| {
            Regex::new(r"(?i)\b(Basic\s+)[A-Za-z0-9+/]+={0,2}")
                .expect("Basic Authorization redaction regex must compile")
        })
        .replace_all(message, "${1}[REDACTED]")
        .into_owned()
}

pub struct FileSecretBundle {
    protocol: FileSecretProtocol,
    scope_digest: FileSecretScopeDigest,
    values: BTreeMap<FileSecretKey, FileSecret>,
}

impl FileSecretBundle {
    pub fn try_new(
        protocol: FileSecretProtocol,
        scope: &str,
        values: Vec<(FileSecretKey, FileSecret)>,
    ) -> Result<Self, String> {
        let mut bundle =
            Self { protocol, scope_digest: FileSecretScopeDigest::from_scope(scope), values: BTreeMap::new() };
        for (key, value) in values {
            if !protocol.allows(key) {
                return Err("File secret key is not allowed for this protocol".to_string());
            }
            if bundle.values.insert(key, value).is_some() {
                return Err("File connection secret keys must be unique".to_string());
            }
        }
        Ok(bundle)
    }

    pub fn empty(protocol: FileSecretProtocol, scope: &str) -> Self {
        Self { protocol, scope_digest: FileSecretScopeDigest::from_scope(scope), values: BTreeMap::new() }
    }

    pub const fn protocol(&self) -> FileSecretProtocol {
        self.protocol
    }

    pub const fn scope_digest(&self) -> FileSecretScopeDigest {
        self.scope_digest
    }

    pub fn get(&self, key: FileSecretKey) -> Option<&FileSecret> {
        self.values.get(&key)
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn redactor(&self) -> FileSecretRedactor {
        FileSecretRedactor::from_secrets(self.values.values())
    }

    pub(crate) fn values(&self) -> impl Iterator<Item = (FileSecretKey, &FileSecret)> {
        self.values.iter().map(|(key, value)| (*key, value))
    }

    pub(crate) fn from_decrypted(
        protocol: FileSecretProtocol,
        scope_digest: FileSecretScopeDigest,
        values: Vec<(FileSecretKey, FileSecret)>,
    ) -> Result<Self, String> {
        let mut bundle = Self { protocol, scope_digest, values: BTreeMap::new() };
        for (key, value) in values {
            if !protocol.allows(key) || bundle.values.insert(key, value).is_some() {
                return Err("Stored file secret bundle is invalid".to_string());
            }
        }
        Ok(bundle)
    }
}

pub enum FileSecretUpdate {
    Preserve { protocol: FileSecretProtocol, scope_digest: FileSecretScopeDigest },
    Replace(FileSecretBundle),
}

impl FileSecretUpdate {
    pub fn preserve(protocol: FileSecretProtocol, scope: &str) -> Self {
        Self::Preserve { protocol, scope_digest: FileSecretScopeDigest::from_scope(scope) }
    }

    pub fn replace(bundle: FileSecretBundle) -> Self {
        Self::Replace(bundle)
    }

    pub const fn protocol(&self) -> FileSecretProtocol {
        match self {
            Self::Preserve { protocol, .. } => *protocol,
            Self::Replace(bundle) => bundle.protocol(),
        }
    }

    pub const fn scope_digest(&self) -> FileSecretScopeDigest {
        match self {
            Self::Preserve { scope_digest, .. } => *scope_digest,
            Self::Replace(bundle) => bundle.scope_digest(),
        }
    }

    pub const fn is_replace(&self) -> bool {
        matches!(self, Self::Replace(_))
    }
}

pub struct EncryptedFileSecret {
    pub nonce: [u8; FILE_SECRET_NONCE_SIZE],
    pub ciphertext: Vec<u8>,
}

pub struct FileSecretVault {
    root_key: FileSecretRootKey,
}

impl FileSecretVault {
    pub fn new(root_key: FileSecretRootKey) -> Self {
        Self { root_key }
    }

    pub fn encrypt(
        &self,
        db_uuid: &str,
        connection_id: &str,
        protocol: FileSecretProtocol,
        key: FileSecretKey,
        scope_digest: FileSecretScopeDigest,
        plaintext: &FileSecret,
    ) -> Result<EncryptedFileSecret, String> {
        let cipher = Aes256Gcm::new_from_slice(self.root_key.as_bytes())
            .map_err(|_| "File secret encryption failed".to_string())?;
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);
        let aad = envelope_aad(db_uuid, connection_id, protocol, key, scope_digest);
        let ciphertext = cipher
            .encrypt(&nonce, Payload { msg: plaintext.expose_secret().as_bytes(), aad: &aad })
            .map_err(|_| "File secret encryption failed".to_string())?;
        Ok(EncryptedFileSecret { nonce: nonce.into(), ciphertext })
    }

    pub fn decrypt(
        &self,
        db_uuid: &str,
        connection_id: &str,
        protocol: FileSecretProtocol,
        key: FileSecretKey,
        scope_digest: FileSecretScopeDigest,
        nonce: &[u8],
        ciphertext: &[u8],
    ) -> Result<FileSecret, String> {
        let cipher = Aes256Gcm::new_from_slice(self.root_key.as_bytes())
            .map_err(|_| "Stored file secrets are unavailable".to_string())?;
        let nonce: &[u8; FILE_SECRET_NONCE_SIZE] =
            nonce.try_into().map_err(|_| "Stored file secrets are unavailable".to_string())?;
        let aad = envelope_aad(db_uuid, connection_id, protocol, key, scope_digest);
        let plaintext = cipher
            .decrypt(Nonce::from_slice(nonce), Payload { msg: ciphertext, aad: &aad })
            .map_err(|_| "Stored file secrets are unavailable".to_string())?;
        let plaintext = Zeroizing::new(plaintext);
        let value = std::str::from_utf8(plaintext.as_slice())
            .map_err(|_| "Stored file secrets are unavailable".to_string())?
            .to_owned();
        FileSecret::new(value).map_err(|_| "Stored file secrets are unavailable".to_string())
    }
}

fn envelope_aad(
    db_uuid: &str,
    connection_id: &str,
    protocol: FileSecretProtocol,
    key: FileSecretKey,
    scope_digest: FileSecretScopeDigest,
) -> Vec<u8> {
    let mut aad = Vec::with_capacity(db_uuid.len() + connection_id.len() + 48);
    aad.extend_from_slice(b"dbx-file-secret");
    aad.extend_from_slice(&FILE_SECRET_ENVELOPE_VERSION.to_be_bytes());
    append_len_prefixed(&mut aad, db_uuid.as_bytes());
    append_len_prefixed(&mut aad, connection_id.as_bytes());
    aad.push(protocol.code());
    aad.push(key.code());
    aad.extend_from_slice(scope_digest.as_bytes());
    aad
}

fn append_len_prefixed(target: &mut Vec<u8>, value: &[u8]) {
    target.extend_from_slice(&(value.len() as u64).to_be_bytes());
    target.extend_from_slice(value);
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: [u8; 32] = [0x42; 32];

    #[test]
    fn protocol_policy_is_fail_closed() {
        assert!(FileSecretBundle::try_new(
            FileSecretProtocol::HdfsNative,
            "scope",
            vec![(FileSecretKey::WebhdfsDelegationToken, FileSecret::new("token".to_string()).unwrap())],
        )
        .is_err());
        assert!(FileSecretBundle::try_new(
            FileSecretProtocol::Ftp,
            "scope",
            vec![(FileSecretKey::S3SecretAccessKey, FileSecret::new("secret".to_string()).unwrap())],
        )
        .is_err());
    }

    #[test]
    fn secret_deserialization_and_bundle_failure_paths_use_typed_ownership() {
        let secret: FileSecret = serde_json::from_str("\"ipc-canary\"").unwrap();
        assert_eq!(secret.expose_secret(), "ipc-canary");
        assert!(serde_json::from_str::<FileSecret>("\"\"").is_err());

        let error = FileSecretBundle::try_new(
            FileSecretProtocol::Ftp,
            "scope",
            vec![
                (FileSecretKey::Password, FileSecret::new("first-canary".to_string()).unwrap()),
                (FileSecretKey::Password, FileSecret::new("second-canary".to_string()).unwrap()),
            ],
        )
        .err()
        .expect("duplicate keys must be rejected");
        assert_eq!(error, "File connection secret keys must be unique");
        assert!(!error.contains("canary"));
    }

    #[test]
    fn typed_secret_debug_and_redactor_do_not_expose_raw_or_encoded_material() {
        use base64::Engine;

        let token = FileSecret::new("delegation/+ token%&?".to_string()).unwrap();
        let private_key = FileSecret::new(
            "-----BEGIN OPENSSH PRIVATE KEY-----\ninline-pem-canary\n-----END OPENSSH PRIVATE KEY-----".to_string(),
        )
        .unwrap();
        assert_eq!(format!("{token:?}"), "[REDACTED]");
        assert!(!serde_json::to_string(&RedactedFileText::from_static("safe")).unwrap().contains("delegation"));

        let percent = percent_encoding::utf8_percent_encode(token.expose_secret(), percent_encoding::NON_ALPHANUMERIC)
            .to_string();
        let form = url_form_encode(token.expose_secret());
        let basic = base64::engine::general_purpose::STANDARD.encode(format!("dbx:{}", token.expose_secret()));
        let normalized_private_key = private_key.expose_secret().replace('\n', "\r\n");
        let redactor = FileSecretRedactor::from_secrets([&token, &private_key]);
        let redacted = redactor.redact(format!(
            "{} {percent} {form} Authorization: Basic {basic} {normalized_private_key} \
             /tmp/dbx-sftp-keys-123/private-key.pem",
            token.expose_secret(),
        ));
        for canary in [
            token.expose_secret(),
            percent.as_str(),
            form.as_str(),
            basic.as_str(),
            "inline-pem-canary",
            "dbx-sftp-keys-123",
            "private-key.pem",
        ] {
            assert!(!redacted.as_str().contains(canary), "{canary} leaked through the redactor");
        }
        assert!(redacted.as_str().contains("[REDACTED]"));
        assert!(redacted.as_str().contains("[SFTP_KEY_MATERIAL]"));
    }

    #[test]
    fn envelope_uses_fresh_nonce_and_binds_all_identity_fields() {
        let vault = FileSecretVault::new(FileSecretRootKey::new(KEY));
        let scope = FileSecretScopeDigest::from_scope("scope");
        let plaintext = FileSecret::new("secret".to_string()).unwrap();
        let first = vault
            .encrypt("db", "connection", FileSecretProtocol::S3, FileSecretKey::S3SecretAccessKey, scope, &plaintext)
            .unwrap();
        let second = vault
            .encrypt("db", "connection", FileSecretProtocol::S3, FileSecretKey::S3SecretAccessKey, scope, &plaintext)
            .unwrap();
        assert_ne!(first.nonce, second.nonce);
        assert_eq!(
            vault
                .decrypt(
                    "db",
                    "connection",
                    FileSecretProtocol::S3,
                    FileSecretKey::S3SecretAccessKey,
                    scope,
                    &first.nonce,
                    &first.ciphertext,
                )
                .unwrap()
                .expose_secret(),
            "secret"
        );
        assert!(vault
            .decrypt(
                "db",
                "other",
                FileSecretProtocol::S3,
                FileSecretKey::S3SecretAccessKey,
                scope,
                &first.nonce,
                &first.ciphertext,
            )
            .is_err());
        assert!(FileSecretVault::new(FileSecretRootKey::new([0x24; 32]))
            .decrypt(
                "db",
                "connection",
                FileSecretProtocol::S3,
                FileSecretKey::S3SecretAccessKey,
                scope,
                &first.nonce,
                &first.ciphertext,
            )
            .is_err());
    }
}
