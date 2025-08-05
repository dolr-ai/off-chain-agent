use jsonwebtoken::{encode, get_current_timestamp, Algorithm, EncodingKey, Header};
use serde::{Deserialize, Serialize};
use std::{env, fs};

#[derive(Debug, Serialize, Deserialize)]
pub struct Claims {
    pub aud: String,
    pub exp: usize,
}

fn main() {
    // Read environment variables or use defaults for testing
    let jwt_pem_file = env::var("JWT_PEM_FILE").unwrap_or_else(|_| {
        eprintln!("JWT_PEM_FILE not set, using test key generation");
        "jwt_private_key.pem".to_string()
    });

    let jwt_aud = env::var("JWT_AUD").unwrap_or_else(|_| {
        eprintln!("JWT_AUD not set, using default 'videogen-api'");
        "videogen-api".to_string()
    });

    // If PEM file doesn't exist, generate a test key pair
    if !std::path::Path::new(&jwt_pem_file).exists() {
        eprintln!("Generating test EdDSA key pair...");

        // Generate Ed25519 key pair using openssl command
        let _ = std::process::Command::new("openssl")
            .args([
                "genpkey",
                "-algorithm",
                "ed25519",
                "-out",
                "jwt_private_key.pem",
            ])
            .output()
            .expect("Failed to generate private key");

        let _ = std::process::Command::new("openssl")
            .args([
                "pkey",
                "-in",
                "jwt_private_key.pem",
                "-pubout",
                "-out",
                "jwt_public_key.pem",
            ])
            .output()
            .expect("Failed to generate public key");

        println!("Generated test keys: jwt_private_key.pem and jwt_public_key.pem");
    }

    // Read the private key
    let enc_key_raw = fs::read(&jwt_pem_file)
        .unwrap_or_else(|_| panic!("Failed to read JWT PEM file: {}", jwt_pem_file));

    let enc_key = EncodingKey::from_ed_pem(&enc_key_raw)
        .expect("Invalid PEM file - must be Ed25519 private key in PEM format");

    let header = Header::new(Algorithm::EdDSA);

    // Generate tokens with different expiration times
    let expiry_options = vec![
        // ("1 hour", 60 * 60),
        // ("1 day", 24 * 60 * 60),
        ("7 days", 7 * 24 * 60 * 60),
        // ("30 days", 30 * 24 * 60 * 60),
        // ("180 days", 180 * 24 * 60 * 60),
    ];

    println!("\n=== JWT Token Generator ===");
    println!("Audience: {}", jwt_aud);
    println!("Private Key: {}", jwt_pem_file);

    // Print public key if it exists
    if let Ok(public_key) = fs::read_to_string("jwt_public_key.pem") {
        println!("\nPublic Key (for JWT_PUBLIC_KEY_PEM env var):");
        println!("{}", public_key);
    }

    println!("\n=== Generated JWT Tokens ===");

    for (duration_name, duration_seconds) in expiry_options {
        let expiry = get_current_timestamp() + duration_seconds;

        let claims = Claims {
            aud: jwt_aud.clone(),
            exp: expiry as usize,
        };

        let token = encode(&header, &claims, &enc_key).expect("Failed to encode JWT");

        println!("\nJWT with {} expiry:", duration_name);
        println!("{}", token);
        println!("Expires at: timestamp {}", expiry);
    }

    println!("\n=== Usage Instructions ===");
    println!("1. Set environment variables:");
    println!("   export JWT_PUBLIC_KEY_PEM=\"$(cat jwt_public_key.pem)\"");
    println!("   export JWT_AUD=\"{}\"", jwt_aud);
    println!("\n2. Use the token in requests:");
    println!("   curl -H \"Authorization: Bearer <JWT_TOKEN>\" ...");
}
