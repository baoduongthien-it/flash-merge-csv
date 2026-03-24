use clap::Parser;
use std::path::PathBuf;

/// Flash Merge CSV — High-performance tool to merge multiple large CSV files.
#[derive(Parser, Debug)]
#[command(name = "flash-merge-csv", version, about, long_about = None)]
pub struct Args {
    /// Input CSV files to merge (supports glob patterns like '*.csv')
    #[arg(required = true)]
    pub input_files: Vec<PathBuf>,

    /// Output file path
    #[arg(short, long, default_value = "merged_output.csv")]
    pub output: PathBuf,

    /// Whether the input files have a header row.
    /// If true, the header from the first file is kept and headers from
    /// subsequent files are skipped.
    #[arg(long, default_value_t = true)]
    pub has_header: bool,

    /// CSV delimiter character (default: comma)
    #[arg(short, long, default_value_t = b',')]
    pub delimiter: u8,

    /// I/O buffer size in bytes (default: 8MB for optimal throughput on large files)
    #[arg(long, default_value_t = 8 * 1024 * 1024)]
    pub buffer_size: usize,

    /// Validate rows against the header schema while merging (slower but safer).
    /// When disabled, raw byte-level copy is used for maximum speed.
    #[arg(long, default_value_t = false)]
    pub validate: bool,

    /// Disable the progress bar for slightly less overhead.
    #[arg(long, default_value_t = false)]
    pub no_progress: bool,
}

impl Args {
    pub fn parse_args() -> Self {
        Self::parse()
    }
}
