mod cli;
mod merger;

use anyhow::Result;
use cli::Args;
use glob::glob;

fn main() -> Result<()> {
    let args = Args::parse_args();

    if args.input_files.is_empty() {
        anyhow::bail!("No input files provided. Use --help for usage.");
    }

    let mut expanded_files = Vec::new();

    for pattern_or_path in args.input_files {
        let path_str = pattern_or_path.to_string_lossy();
        if path_str.contains('*') || path_str.contains('?') || path_str.contains('[') {
            let mut found = false;
            for entry in glob(&path_str)? {
                match entry {
                    Ok(path) => {
                        if path.is_file() {
                            expanded_files.push(path);
                            found = true;
                        }
                    }
                    Err(e) => anyhow::bail!("Error reading glob entry: {:?}", e),
                }
            }
            if !found {
                anyhow::bail!("No files found matching pattern: {}", path_str);
            }
        } else {
            // Treat as a regular file and validate that it exists
            if !pattern_or_path.exists() {
                anyhow::bail!("Input file not found: {}", pattern_or_path.display());
            }
            expanded_files.push(pattern_or_path);
        }
    }

    // Check output file can be created
    if let Some(parent) = args.output.parent() {
        if !parent.exists() {
            std::fs::create_dir_all(parent)?;
        }
    }

    let config = merger::MergeConfig {
        input_files: expanded_files,
        output: args.output,
        has_header: args.has_header,
        delimiter: args.delimiter,
        buffer_size: args.buffer_size,
        validate: args.validate,
        no_progress: args.no_progress,
    };

    merger::merge(config)?;

    Ok(())
}
