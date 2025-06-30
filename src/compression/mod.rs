use async_trait::async_trait;
use std::io;

pub mod brotli;
pub mod gzip;
pub mod lz4;
pub mod none;
pub mod zstd;

pub use brotli::BrotliCompressor;
pub use gzip::GzipCompressor;
pub use lz4::Lz4Compressor;
pub use none::NoneCompressor;
pub use zstd::ZstdCompressor;

use crate::config::{CompressionConfig, CompressionType};

#[async_trait]
pub trait Compressor: Send + Sync {
    async fn compress(&self, data: &[u8]) -> io::Result<Vec<u8>>;
    async fn decompress(&self, data: &[u8]) -> io::Result<Vec<u8>>;
}

pub fn init_compression(config: CompressionConfig) -> Box<dyn Compressor> {
    match config.algorithm {
        CompressionType::None => Box::new(NoneCompressor::new()),
        CompressionType::Gzip => Box::new(GzipCompressor::new(config.level)),
        CompressionType::Brotli => Box::new(BrotliCompressor::new(config.level)),
        CompressionType::Zstd => Box::new(ZstdCompressor::new(config.level)),
        CompressionType::Lz4 => Box::new(Lz4Compressor::new(config.level)),
    }
}

pub fn validate_config(config: &CompressionConfig) -> Result<(), miette::Error> {
    match config.algorithm {
        CompressionType::None => NoneCompressor::validate(config),
        CompressionType::Gzip => GzipCompressor::validate(config),
        CompressionType::Zstd => ZstdCompressor::validate(config),
        CompressionType::Lz4 => Lz4Compressor::validate(config),
        CompressionType::Brotli => BrotliCompressor::validate(config),
    }
}
