use crate::config::CompressionConfig;

use super::Compressor;
use async_compression::tokio::bufread::{Lz4Decoder, Lz4Encoder};
use async_trait::async_trait;
use std::io;
use tokio::io::{AsyncReadExt, BufReader};

pub struct Lz4Compressor {
    level: u32,
}

impl Lz4Compressor {
    pub fn new(level: Option<u32>) -> Self {
        Self {
            level: level.unwrap_or(4), // Default LZ4 level
        }
    }
    /// Checks `level` if one is set; without one, `new` uses the default. LZ4
    /// tops out at 12: async-compression caps higher levels to 12, so 13-16
    /// would be accepted but behave as 12.
    pub fn validate(config: &CompressionConfig) -> Result<(), miette::Error> {
        if let Some(level) = config.level
            && !(1..=12).contains(&level)
        {
            return Err(miette::miette!(
                "lz4 compression level must be between 1 and 12, got {}",
                level
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl Compressor for Lz4Compressor {
    async fn compress(&self, data: &[u8]) -> io::Result<Vec<u8>> {
        use async_compression::Level;

        let cursor = std::io::Cursor::new(data);
        let buf_reader = BufReader::new(cursor);
        let level = Level::Precise(self.level as i32);

        let mut encoder = Lz4Encoder::with_quality(buf_reader, level);
        let mut compressed = Vec::new();
        encoder.read_to_end(&mut compressed).await?;

        Ok(compressed)
    }

    async fn decompress(&self, data: &[u8]) -> io::Result<Vec<u8>> {
        let cursor = std::io::Cursor::new(data);
        let buf_reader = BufReader::new(cursor);

        let mut decoder = Lz4Decoder::new(buf_reader);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed).await?;

        Ok(decompressed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_with_custom_level() {
        let compressor = Lz4Compressor::new(Some(8));
        assert_eq!(compressor.level, 8);
    }

    #[tokio::test]
    async fn test_compress_decompress_roundtrip() {
        let compressor = Lz4Compressor::new(Some(12));
        let data = b"test data";

        let compressed = compressor.compress(data).await.unwrap();
        let decompressed = compressor.decompress(&compressed).await.unwrap();

        assert_eq!(data.as_slice(), decompressed);
    }

    #[tokio::test]
    async fn test_compress_empty_data() {
        let compressor = Lz4Compressor::new(None);
        let data = b"";

        let compressed = compressor.compress(data).await.unwrap();
        let decompressed = compressor.decompress(&compressed).await.unwrap();

        assert_eq!(data.as_slice(), decompressed);
    }
}
