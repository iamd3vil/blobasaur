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

#[cfg(test)]
mod tests {
    use super::*;

    fn config(algorithm: CompressionType, level: Option<u32>) -> CompressionConfig {
        CompressionConfig {
            enabled: true,
            algorithm,
            level,
        }
    }

    #[test]
    fn validate_accepts_a_missing_level() {
        for algorithm in [
            CompressionType::None,
            CompressionType::Gzip,
            CompressionType::Zstd,
            CompressionType::Lz4,
            CompressionType::Brotli,
        ] {
            assert!(
                validate_config(&config(algorithm, None)).is_ok(),
                "{algorithm:?} without a level must use its default"
            );
        }
    }

    #[test]
    fn validate_checks_a_level_that_is_set() {
        let cases = [
            (CompressionType::Zstd, 1, true),
            (CompressionType::Zstd, 22, true),
            (CompressionType::Zstd, 0, false),
            (CompressionType::Zstd, 23, false),
            (CompressionType::Lz4, 1, true),
            (CompressionType::Lz4, 12, true),
            (CompressionType::Lz4, 0, false),
            // async-compression caps LZ4 at 12, so 13-16 would silently act as 12.
            (CompressionType::Lz4, 13, false),
            (CompressionType::Gzip, 1, true),
            (CompressionType::Gzip, 9, true),
            (CompressionType::Gzip, 0, false),
            (CompressionType::Gzip, 10, false),
            (CompressionType::Brotli, 0, true),
            (CompressionType::Brotli, 11, true),
            (CompressionType::Brotli, 12, false),
        ];
        for (algorithm, level, valid) in cases {
            assert_eq!(
                validate_config(&config(algorithm, Some(level))).is_ok(),
                valid,
                "{algorithm:?} level {level}"
            );
        }
    }
}
