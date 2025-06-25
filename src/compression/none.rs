use crate::config::CompressionConfig;

use super::Compressor;
use async_trait::async_trait;
use std::io;

pub struct NoneCompressor;

impl NoneCompressor {
    pub fn new() -> Self {
        Self
    }
    pub fn validate(_: &CompressionConfig) -> Result<(), miette::Error> {
        return Ok(());
    }
}

#[async_trait]
impl Compressor for NoneCompressor {
    async fn compress(&self, data: &[u8]) -> io::Result<Vec<u8>> {
        // No compression - just return a copy of the data
        Ok(data.to_vec())
    }

    async fn decompress(&self, data: &[u8]) -> io::Result<Vec<u8>> {
        // No decompression needed - just return a copy of the data
        Ok(data.to_vec())
    }
}
