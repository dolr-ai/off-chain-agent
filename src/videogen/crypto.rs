use aes_gcm::{
    aead::{Aead, KeyInit},
    Aes256Gcm, Nonce,
};
use anyhow::{anyhow, Context, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use rand::RngCore;
use std::env;
use yral_types::delegated_identity::DelegatedIdentityWire;

fn get_encryption_key() -> [u8; 32] {
    let secret = env::var("INTERNAL_ENCRYPTION_SECRET").expect(
        "INTERNAL_ENCRYPTION_SECRET environment variable NOT SET. Mandatory for identity encryption."
    );

    let mut key = [0u8; 32];
    let secret_bytes = secret.as_bytes();
    let len = secret_bytes.len().min(32);
    key[..len].copy_from_slice(&secret_bytes[..len]);
    key
}

pub fn encrypt_identity(identity: &DelegatedIdentityWire) -> Result<String> {
    let json = serde_json::to_string(identity).context("Failed to serialize identity")?;
    let key_bytes = get_encryption_key();
    let cipher = Aes256Gcm::new_from_slice(&key_bytes).map_err(|e| anyhow!("Invalid key: {e}"))?;

    // Generate 96-bit random nonce
    let mut nonce_bytes = [0u8; 12];
    rand::rng().fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);

    let ciphertext = cipher
        .encrypt(nonce, json.as_bytes())
        .map_err(|e| anyhow!("Encryption failed: {e}"))?;

    // Prepend nonce to ciphertext
    let mut combined = nonce_bytes.to_vec();
    combined.extend_from_slice(&ciphertext);

    Ok(BASE64.encode(combined))
}

pub fn decrypt_identity(encrypted_base64: &str) -> Result<DelegatedIdentityWire> {
    let combined = BASE64
        .decode(encrypted_base64)
        .context("Failed to decode base64")?;
    if combined.len() < 12 {
        return Err(anyhow!("Invalid encrypted payload (too short)"));
    }

    let (nonce_bytes, ciphertext) = combined.split_at(12);
    let nonce = Nonce::from_slice(nonce_bytes);
    let key_bytes = get_encryption_key();
    let cipher = Aes256Gcm::new_from_slice(&key_bytes).map_err(|e| anyhow!("Invalid key: {e}"))?;

    let plaintext = cipher
        .decrypt(nonce, ciphertext)
        .map_err(|e| anyhow!("Decryption failed: {e}"))?;

    let identity: DelegatedIdentityWire =
        serde_json::from_slice(&plaintext).context("Failed to deserialize identity")?;
    Ok(identity)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_identity_encryption_roundtrip() {
        use ic_agent::identity::SignedDelegation;
        use k256::elliptic_curve::JwkEcKey;

        // Generate a random Secp256k1 secret key and convert to JWK for to_secret
        let secret = k256::SecretKey::random(&mut rand::rng());
        let to_secret: JwkEcKey = secret.to_jwk();

        let identity = DelegatedIdentityWire {
            from_key: vec![1, 2, 3, 4, 5],
            to_secret,
            delegation_chain: vec![],
        };

        let encrypted = encrypt_identity(&identity).expect("Encryption failed");
        let decrypted = decrypt_identity(&encrypted).expect("Decryption failed");

        assert_eq!(identity.from_key, decrypted.from_key);
        assert_eq!(
            identity.delegation_chain.len(),
            decrypted.delegation_chain.len()
        );
    }
}
