use crate::config::CompressionConfig;

use super::Compressor;
use async_compression::tokio::bufread::{BrotliDecoder, BrotliEncoder};
use async_trait::async_trait;
use std::io;
use tokio::io::{AsyncReadExt, BufReader};

pub struct BrotliCompressor {
    level: u32,
}

impl BrotliCompressor {
    pub fn new(level: Option<u32>) -> Self {
        Self {
            level: level.unwrap_or(6),
        }
    }
    pub fn validate(config: &CompressionConfig) -> Result<(), miette::Error> {
        let level = config.level.unwrap_or(0);
        if level > 11 {
            return Err(miette::miette!("Compression level less than 11"));
        }
        Ok(())
    }
}

#[async_trait]
impl Compressor for BrotliCompressor {
    async fn compress(&self, data: &[u8]) -> io::Result<Vec<u8>> {
        use async_compression::Level;

        let cursor = std::io::Cursor::new(data);
        let buf_reader = BufReader::new(cursor);
        let level = Level::Precise(self.level as i32);

        let mut encoder = BrotliEncoder::with_quality(buf_reader, level);
        let mut compressed = Vec::new();
        encoder.read_to_end(&mut compressed).await?;

        Ok(compressed)
    }

    async fn decompress(&self, data: &[u8]) -> io::Result<Vec<u8>> {
        let cursor = std::io::Cursor::new(data);
        let buf_reader = BufReader::new(cursor);

        let mut decoder = BrotliDecoder::new(buf_reader);
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
        let compressor = BrotliCompressor::new(Some(3));
        assert_eq!(compressor.level, 3);
    }

    #[tokio::test]
    async fn test_compress_decompress_roundtrip() {
        let compressor = BrotliCompressor::new(Some(4));
        let data = b"test data";

        let compressed = compressor.compress(data).await.unwrap();
        let decompressed = compressor.decompress(&compressed).await.unwrap();

        assert_eq!(data.as_slice(), decompressed);
    }

    #[tokio::test]
    async fn test_compress_empty_data() {
        let compressor = BrotliCompressor::new(None);
        let data = b"";

        let compressed = compressor.compress(data).await.unwrap();
        let decompressed = compressor.decompress(&compressed).await.unwrap();

        assert_eq!(data.as_slice(), decompressed);
    }
}
