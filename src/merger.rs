use anyhow::{Context, Result};
use crossbeam_channel::{bounded, Receiver, Sender};
use indicatif::{ProgressBar, ProgressStyle};
use memchr::memchr;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, ErrorKind, Read, Write};
#[cfg(windows)]
use std::os::windows::io::AsRawHandle;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Instant;

// ---------------------------------------------------------------------------
// TUNING CONSTANTS
// ---------------------------------------------------------------------------

/// Number of buffers in the ring-buffer pipeline.
/// 8 buffers (up from 4) gives the reader more runway to stay ahead of the
/// writer, reducing stalls when disk latency spikes (common on HDD).
const NUM_PIPELINE_BUFFERS: usize = 8;

/// Progress bar update interval in bytes.
const PROGRESS_UPDATE_INTERVAL: u64 = 10 * 1024 * 1024; // 10 MB

// ---------------------------------------------------------------------------
// PUBLIC API
// ---------------------------------------------------------------------------

pub struct MergeConfig {
    pub input_files: Vec<PathBuf>,
    pub output: PathBuf,
    pub has_header: bool,
    pub delimiter: u8,
    pub buffer_size: usize,
    pub validate: bool,
    pub no_progress: bool,
}

pub fn merge(config: MergeConfig) -> Result<()> {
    let start = Instant::now();

    if config.validate {
        merge_validated(&config)?;
    } else {
        merge_raw_pipeline(&config)?;
    }

    let elapsed = start.elapsed();
    let output_size = fs::metadata(&config.output).map(|m| m.len()).unwrap_or(0);

    eprintln!();
    eprintln!("✅ Merge complete!");
    eprintln!("   Files merged : {}", config.input_files.len());
    eprintln!(
        "   Output       : {} ({})",
        config.output.display(),
        format_size(output_size)
    );
    eprintln!("   Time         : {:.2}s", elapsed.as_secs_f64());
    if elapsed.as_secs_f64() > 0.0 {
        let throughput = output_size as f64 / elapsed.as_secs_f64();
        eprintln!("   Throughput   : {}/s", format_size(throughput as u64));
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// PLATFORM-SPECIFIC I/O HINTS
// ---------------------------------------------------------------------------
// Windows : FILE_FLAG_SEQUENTIAL_SCAN  → aggressive read-ahead, discard pages
// Linux   : posix_fadvise SEQUENTIAL   → same effect via the page cache
// Both hints are critical for HDDs — they tell the OS to prefetch ahead
// rather than waiting for each read() call.
// ---------------------------------------------------------------------------

#[cfg(windows)]
const FILE_FLAG_SEQUENTIAL_SCAN: u32 = 0x0800_0000;

fn open_with_hints(path: &Path) -> Result<File> {
    #[cfg(windows)]
    {
        use std::os::windows::fs::OpenOptionsExt;
        let f = OpenOptions::new()
            .read(true)
            .custom_flags(FILE_FLAG_SEQUENTIAL_SCAN)
            .open(path)
            .with_context(|| format!("Failed to open: {}", path.display()))?;
        Ok(f)
    }
    #[cfg(not(windows))]
    {
        let f = File::open(path).with_context(|| format!("Failed to open: {}", path.display()))?;

        // Linux: POSIX_FADV_SEQUENTIAL triggers aggressive read-ahead in the
        // kernel, which is especially impactful on HDD where seek latency
        // dominates. Best-effort — ignore errors on unsupported kernels.
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::io::AsRawFd;
            unsafe {
                libc::posix_fadvise(f.as_raw_fd(), 0, 0, libc::POSIX_FADV_SEQUENTIAL);
            }
        }

        Ok(f)
    }
}

// ---------------------------------------------------------------------------
// RAW PIPELINE MERGE (Maximum speed)
// ---------------------------------------------------------------------------
// Architecture:
//   Reader thread (main)  ──[filled buffers]──▶  Writer thread
//                          ◀──[empty buffers]──
//
// Changes vs original:
//   1. crossbeam bounded channels replace std::sync::mpsc::sync_channel.
//      crossbeam uses a lock-free ring buffer internally — lower latency and
//      no Mutex contention between reader and writer threads.
//   2. NUM_PIPELINE_BUFFERS raised from 4 → 8 (more slack for HDD spikes).
//   3. Header skipping uses memchr instead of BufReader::read_line, avoiding
//      a String allocation and UTF-8 validation on every file.
//   4. posix_fadvise(SEQUENTIAL) on Linux (see open_with_hints above).
// ---------------------------------------------------------------------------

fn merge_raw_pipeline(config: &MergeConfig) -> Result<()> {
    let total_bytes: u64 = config
        .input_files
        .iter()
        .map(|p| fs::metadata(p).map(|m| m.len()).unwrap_or(0))
        .sum();

    let pb = create_progress_bar(total_bytes, config.no_progress);
    pb.set_message("Merging (pipeline)");

    // Pre-allocate output file to reduce filesystem fragmentation on HDD
    let out_file = File::create(&config.output)
        .with_context(|| format!("Failed to create output: {}", config.output.display()))?;
    let estimated = estimate_output_size(config, total_bytes);
    // TỐI ƯU HDD WINDOWS: Pre-allocate vật lý để giảm Seek
    if estimated > 0 {
        let _ = out_file.set_len(estimated);
        #[cfg(windows)]
        {
            // Ép Windows cấp phát không gian thực tế trên đĩa
            // Điều này giúp đầu từ HDD chạy mượt hơn vì dữ liệu ít bị phân mảnh
            unsafe {
                use windows_sys::Win32::Storage::FileSystem::SetFileValidData;
                // Lưu ý: Cần thêm crate windows-sys = "0.52" vào Cargo.toml
                let _ = SetFileValidData(out_file.as_raw_handle() as _, estimated as i64);
            }
        }
    }

    // Nâng Buffer lên 64MB cho HDD (thay vì 8MB mặc định)
    let effective_buffer_size = if config.buffer_size < 64 * 1024 * 1024 {
        64 * 1024 * 1024
    } else {
        config.buffer_size
    };

    let buffer_size = effective_buffer_size;

    // --- crossbeam bounded channels (lock-free, lower latency than mpsc) ---
    // data channel: reader → writer  (filled buffers + valid byte count)
    let (data_tx, data_rx): (Sender<(Vec<u8>, usize)>, Receiver<(Vec<u8>, usize)>) =
        bounded(NUM_PIPELINE_BUFFERS);
    // return channel: writer → reader  (empty buffers for reuse)
    let (buf_return_tx, buf_return_rx): (Sender<Vec<u8>>, Receiver<Vec<u8>>) =
        bounded(NUM_PIPELINE_BUFFERS);

    // Clone so the reader can also recycle buffers on EOF without going
    // through the writer.
    let buf_recycle_tx = buf_return_tx.clone();

    // Pre-fill buffer pool — all buffers start on the return channel
    for _ in 0..NUM_PIPELINE_BUFFERS {
        buf_return_tx
            .send(vec![0u8; buffer_size])
            .map_err(|_| anyhow::anyhow!("Failed to initialize buffer pool"))?;
    }

    // --- Spawn writer thread ---
    let writer_handle = thread::spawn(move || -> Result<u64> {
        let mut writer = BufWriter::with_capacity(buffer_size, out_file);
        let mut bytes_written: u64 = 0;

        while let Ok((buf, len)) = data_rx.recv() {
            writer.write_all(&buf[..len])?;
            bytes_written += len as u64;
            // Return the now-empty buffer to the pool
            let _ = buf_return_tx.send(buf);
        }

        writer.flush()?;
        Ok(bytes_written)
    });

    // --- Reader (main thread) ---
    let mut pending_progress: u64 = 0;

    for (i, path) in config.input_files.iter().enumerate() {
        let file = open_with_hints(path)?;

        pb.set_message(format!(
            "[{}/{}] {}",
            i + 1,
            config.input_files.len(),
            path.file_name().unwrap_or_default().to_string_lossy()
        ));

        let mut reader = RawReader::new(file, buffer_size);

        // Skip the header line for every file after the first.
        //
        // Old approach: BufReader::read_line → allocates a String, validates
        // UTF-8, copies bytes into the heap.
        //
        // New approach: fill one raw buffer, find the first '\n' with memchr
        // (SIMD-accelerated on x86/ARM), then keep the remainder of the
        // buffer as the start of the data region. Zero allocations, no
        // UTF-8 overhead, and memchr scans ~32 bytes/cycle with AVX2.
        if i > 0 && config.has_header {
            let (skipped, kept) = reader.skip_first_line()?;
            pending_progress += skipped as u64;
            // `kept` bytes after the newline are already buffered — the next
            // read_full call will return them first.
            let _ = kept; // used internally by RawReader
        }

        // Hot loop: fill buffers, ship to writer
        loop {
            let mut buf = buf_return_rx
                .recv()
                .map_err(|_| anyhow::anyhow!("Writer thread terminated unexpectedly"))?;

            let n = reader.read_full(&mut buf)?;
            if n == 0 {
                let _ = buf_recycle_tx.send(buf);
                break;
            }

            pending_progress += n as u64;
            if pending_progress >= PROGRESS_UPDATE_INTERVAL {
                pb.inc(pending_progress);
                pending_progress = 0;
            }

            data_tx
                .send((buf, n))
                .map_err(|_| anyhow::anyhow!("Writer thread terminated unexpectedly"))?;
        }
    }

    if pending_progress > 0 {
        pb.inc(pending_progress);
    }

    // Dropping data_tx signals the writer that no more data is coming
    drop(data_tx);

    let bytes_written = writer_handle
        .join()
        .map_err(|_| anyhow::anyhow!("Writer thread panicked"))??;

    // Truncate to actual size (pre-allocation is a best-effort over-estimate)
    if let Ok(f) = OpenOptions::new().write(true).open(&config.output) {
        let _ = f.set_len(bytes_written);
    }

    pb.finish_with_message("Done!");
    Ok(())
}

// ---------------------------------------------------------------------------
// RawReader — thin wrapper around File that:
//   • Maintains an internal buffer (same size as the pipeline buffers)
//   • Exposes skip_first_line() using memchr — no allocation, SIMD-fast
//   • Exposes read_full() that drains any leftover bytes first
// ---------------------------------------------------------------------------

struct RawReader {
    file: File,
    // Internal buffer used only for the header-skip pass
    inner_buf: Vec<u8>,
    // How many valid bytes are in inner_buf starting at inner_pos
    inner_len: usize,
    inner_pos: usize,
}

impl RawReader {
    fn new(file: File, capacity: usize) -> Self {
        Self {
            file,
            inner_buf: vec![0u8; capacity],
            inner_len: 0,
            inner_pos: 0,
        }
    }

    /// Skip bytes up to and including the first '\n'.
    /// Returns (bytes_skipped, bytes_remaining_in_internal_buffer).
    /// After this call, read_full() will drain leftover bytes before
    /// issuing new read() syscalls.
    fn skip_first_line(&mut self) -> Result<(usize, usize)> {
        // Fill internal buffer if it is empty
        if self.inner_len == 0 {
            let n = fill_raw(&mut self.file, &mut self.inner_buf)?;
            self.inner_len = n;
            self.inner_pos = 0;
        }

        let haystack = &self.inner_buf[self.inner_pos..self.inner_pos + self.inner_len];

        // memchr is SIMD-accelerated: scans ~32 bytes/cycle with AVX2.
        // For typical CSV headers (< 512 bytes) this finishes in 1–2 cycles.
        if let Some(nl_offset) = memchr(b'\n', haystack) {
            let skipped = nl_offset + 1; // include the '\n' itself
            self.inner_pos += skipped;
            self.inner_len -= skipped;
            Ok((skipped, self.inner_len))
        } else {
            // Header is longer than one buffer (unusual but handle gracefully)
            // Consume the entire buffer and keep reading until we find '\n'
            let mut total_skipped = self.inner_len;
            self.inner_len = 0;
            self.inner_pos = 0;

            loop {
                let n = fill_raw(&mut self.file, &mut self.inner_buf)?;
                if n == 0 {
                    break; // EOF without newline — nothing more to skip
                }
                if let Some(nl_offset) = memchr(b'\n', &self.inner_buf[..n]) {
                    let skipped = nl_offset + 1;
                    total_skipped += skipped;
                    self.inner_pos = skipped;
                    self.inner_len = n - skipped;
                    break;
                }
                total_skipped += n;
            }
            Ok((total_skipped, self.inner_len))
        }
    }

    /// Fill `buf` completely, draining leftover bytes from the internal
    /// buffer before issuing new read() syscalls.
    fn read_full(&mut self, buf: &mut [u8]) -> Result<usize> {
        let mut total = 0;

        // Drain leftover bytes from a previous skip_first_line() call
        if self.inner_len > 0 {
            let take = self.inner_len.min(buf.len());
            buf[..take].copy_from_slice(&self.inner_buf[self.inner_pos..self.inner_pos + take]);
            self.inner_pos += take;
            self.inner_len -= take;
            total += take;
        }

        // Then read directly from the file for the rest
        while total < buf.len() {
            match self.file.read(&mut buf[total..]) {
                Ok(0) => break,
                Ok(n) => total += n,
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => return Err(e.into()),
            }
        }

        Ok(total)
    }
}

/// Single fill of a raw buffer from a File (used by RawReader internals).
fn fill_raw(file: &mut File, buf: &mut [u8]) -> Result<usize> {
    let mut total = 0;
    while total < buf.len() {
        match file.read(&mut buf[total..]) {
            Ok(0) => break,
            Ok(n) => total += n,
            Err(e) if e.kind() == ErrorKind::Interrupted => continue,
            Err(e) => return Err(e.into()),
        }
    }
    Ok(total)
}

fn estimate_output_size(config: &MergeConfig, total_input: u64) -> u64 {
    if !config.has_header || config.input_files.len() <= 1 {
        return total_input;
    }
    let headers_to_skip = config.input_files.len() as u64 - 1;
    total_input.saturating_sub(headers_to_skip * 256)
}

// ---------------------------------------------------------------------------
// VALIDATED MERGE (Slower but safe) — unchanged from original
// ---------------------------------------------------------------------------

fn merge_validated(config: &MergeConfig) -> Result<()> {
    let total_bytes: u64 = config
        .input_files
        .iter()
        .map(|p| fs::metadata(p).map(|m| m.len()).unwrap_or(0))
        .sum();

    let pb = create_progress_bar(total_bytes, config.no_progress);
    pb.set_message("Merging (validated)");

    let out_file = File::create(&config.output)
        .with_context(|| format!("Failed to create output: {}", config.output.display()))?;
    let buf_writer = BufWriter::with_capacity(config.buffer_size, out_file);
    let mut csv_writer = csv::WriterBuilder::new()
        .delimiter(config.delimiter)
        .from_writer(buf_writer);

    let mut expected_columns: Option<usize> = None;
    let mut total_rows: u64 = 0;
    let mut skipped_rows: u64 = 0;

    for (i, path) in config.input_files.iter().enumerate() {
        let file = open_with_hints(path)?;
        let file_size = file.metadata()?.len();

        pb.set_message(format!(
            "[{}/{}] {}",
            i + 1,
            config.input_files.len(),
            path.file_name().unwrap_or_default().to_string_lossy()
        ));

        let buf_reader = std::io::BufReader::with_capacity(config.buffer_size, file);
        let mut csv_reader = csv::ReaderBuilder::new()
            .has_headers(config.has_header)
            .delimiter(config.delimiter)
            .flexible(true)
            .from_reader(buf_reader);

        if config.has_header {
            let headers = csv_reader.headers()?.clone();
            if i == 0 {
                expected_columns = Some(headers.len());
                csv_writer.write_record(&headers)?;
            } else if let Some(expected) = expected_columns {
                if headers.len() != expected {
                    eprintln!(
                        "⚠️  Warning: {} has {} columns (expected {}). Proceeding anyway.",
                        path.display(),
                        headers.len(),
                        expected
                    );
                }
            }
        }

        let mut record = csv::ByteRecord::new();
        let mut bytes_tracked: u64 = 0;
        while csv_reader.read_byte_record(&mut record)? {
            if let Some(expected) = expected_columns {
                if record.len() != expected {
                    skipped_rows += 1;
                    continue;
                }
            } else if i == 0 && !config.has_header {
                expected_columns = Some(record.len());
            }

            csv_writer.write_byte_record(&record)?;
            total_rows += 1;

            let pos = csv_reader.position().byte();
            if pos.saturating_sub(bytes_tracked) > PROGRESS_UPDATE_INTERVAL {
                pb.inc(pos.saturating_sub(bytes_tracked));
                bytes_tracked = pos;
            }
        }

        let remaining = file_size.saturating_sub(bytes_tracked);
        if remaining > 0 {
            pb.inc(remaining);
        }
    }

    csv_writer.flush()?;
    pb.finish_with_message("Done!");

    eprintln!("   Rows written : {total_rows}");
    if skipped_rows > 0 {
        eprintln!("   Rows skipped : {skipped_rows} (column count mismatch)");
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn create_progress_bar(total: u64, hidden: bool) -> ProgressBar {
    if hidden {
        return ProgressBar::hidden();
    }
    let pb = ProgressBar::new(total);
    pb.set_style(
        ProgressStyle::with_template(
            "{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}) {msg}",
        )
        .unwrap()
        .progress_chars("█▓▒░  "),
    );
    pb
}

fn format_size(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{bytes} B")
    }
}
