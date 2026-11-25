use crate::config::CompressionConfig;

use super::Compressor;
use async_compression::tokio::bufread::{ZstdDecoder, ZstdEncoder};
use async_trait::async_trait;
use std::io;
use tokio::io::{AsyncReadExt, BufReader};

pub struct ZstdCompressor {
    level: i32,
}

impl ZstdCompressor {
    pub fn new(level: Option<u32>) -> Self {
        Self {
            level: level.map(|l| l as i32).unwrap_or(3),
        }
    }
    /// Checks `level` if one is set; without one, `new` uses the default.
    pub fn validate(config: &CompressionConfig) -> Result<(), miette::Error> {
        if let Some(level) = config.level
            && !(1..=22).contains(&level)
        {
            return Err(miette::miette!(
                "zstd compression level must be between 1 and 22, got {}",
                level
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl Compressor for ZstdCompressor {
    async fn compress(&self, data: &[u8]) -> io::Result<Vec<u8>> {
        let cursor = std::io::Cursor::new(data);
        let buf_reader = BufReader::new(cursor);

        let mut encoder =
            ZstdEncoder::with_quality(buf_reader, async_compression::Level::Precise(self.level));
        let mut compressed = Vec::new();
        encoder.read_to_end(&mut compressed).await?;

        Ok(compressed)
    }

    async fn decompress(&self, data: &[u8]) -> io::Result<Vec<u8>> {
        let cursor = std::io::Cursor::new(data);
        let buf_reader = BufReader::new(cursor);

        let mut decoder = ZstdDecoder::new(buf_reader);
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
        let compressor = ZstdCompressor::new(Some(10));
        assert_eq!(compressor.level, 10);
    }

    #[test]
    fn test_new_without_level_uses_default() {
        assert_eq!(ZstdCompressor::new(None).level, 3);
    }

    #[tokio::test]
    async fn test_compress_decompress_roundtrip() {
        let compressor = ZstdCompressor::new(Some(15));
        let data = b"test data";

        let compressed = compressor.compress(data).await.unwrap();
        let decompressed = compressor.decompress(&compressed).await.unwrap();

        assert_eq!(data.as_slice(), decompressed);
    }

    #[tokio::test]
    async fn test_compress_empty_data() {
        let compressor = ZstdCompressor::new(None);
        let data = b"";

        let compressed = compressor.compress(data).await.unwrap();
        let decompressed = compressor.decompress(&compressed).await.unwrap();

        assert_eq!(data.as_slice(), decompressed);
    }
}
