use anyhow::{Context, Result};

/// Convert binary string "010110..." (640 chars) to byte array (80 bytes) for Milvus BINARY_VECTOR
/// Each byte represents 8 bits, MSB first
pub fn phash_to_binary_vector(phash: &str) -> Result<Vec<u8>> {
    anyhow::ensure!(
        phash.len() == 640,
        "phash must be 640 bits, got {}",
        phash.len()
    );
    anyhow::ensure!(
        phash.chars().all(|c| c == '0' || c == '1'),
        "phash must contain only '0' and '1' characters"
    );

    let mut bytes = Vec::with_capacity(80);

    for chunk in phash.as_bytes().chunks(8) {
        let byte = chunk
            .iter()
            .enumerate()
            .fold(0u8, |acc, (i, &bit)| {
                acc | ((bit - b'0') << (7 - i))
            });
        bytes.push(byte);
    }

    Ok(bytes)
}

/// Convert byte array back to binary string for debugging/verification
pub fn binary_vector_to_phash(bytes: &[u8]) -> Result<String> {
    anyhow::ensure!(
        bytes.len() == 80,
        "binary vector must be 80 bytes, got {}",
        bytes.len()
    );

    let mut phash = String::with_capacity(640);

    for byte in bytes {
        for i in (0..8).rev() {
            phash.push(if (byte >> i) & 1 == 1 { '1' } else { '0' });
        }
    }

    Ok(phash)
}

/// Calculate Hamming distance between two binary strings
pub fn hamming_distance(hash1: &str, hash2: &str) -> u32 {
    hash1
        .chars()
        .zip(hash2.chars())
        .filter(|(c1, c2)| c1 != c2)
        .count() as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_phash_to_binary_vector() {
        // Test with a simple pattern
        let phash = "1".repeat(640);
        let bytes = phash_to_binary_vector(&phash).unwrap();

        assert_eq!(bytes.len(), 80);
        assert!(bytes.iter().all(|&b| b == 0xFF));
    }

    #[test]
    fn test_phash_to_binary_vector_zeros() {
        let phash = "0".repeat(640);
        let bytes = phash_to_binary_vector(&phash).unwrap();

        assert_eq!(bytes.len(), 80);
        assert!(bytes.iter().all(|&b| b == 0x00));
    }

    #[test]
    fn test_phash_roundtrip() {
        let original = "01010101".repeat(80); // 640 chars
        let bytes = phash_to_binary_vector(&original).unwrap();
        let recovered = binary_vector_to_phash(&bytes).unwrap();

        assert_eq!(original, recovered);
    }

    #[test]
    fn test_phash_wrong_length() {
        let phash = "0101";
        let result = phash_to_binary_vector(phash);

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("must be 640 bits"));
    }

    #[test]
    fn test_phash_invalid_chars() {
        let phash = "012".repeat(213) + "01"; // 640 chars but contains '2'
        let result = phash_to_binary_vector(&phash);

        assert!(result.is_err());
    }

    #[test]
    fn test_hamming_distance() {
        let hash1 = "11011001";
        let hash2 = "10011101";

        assert_eq!(hamming_distance(hash1, hash2), 2);
    }

    #[test]
    fn test_hamming_distance_identical() {
        let hash = "01010101".repeat(80);
        assert_eq!(hamming_distance(&hash, &hash), 0);
    }

    #[test]
    fn test_hamming_distance_all_different() {
        let hash1 = "0".repeat(640);
        let hash2 = "1".repeat(640);
        assert_eq!(hamming_distance(&hash1, &hash2), 640);
    }
}
