#!/usr/bin/env python3
"""
Simple script to merge Parquet files into multiple ~1.6GB chunks.
Edit the INPUT_DIR and OUTPUT_DIR variables below to match your needs.
"""

import os
import sys
from pathlib import Path

# ============================================
# CONFIGURATION - EDIT THESE VALUES
# ============================================

# Directory containing your Parquet files
INPUT_DIR = "/path/to/your/parquet/files"

# Directory where the merged chunk files will be saved
OUTPUT_DIR = "/path/to/output/directory"

# Prefix for output files (files will be named: prefix_0000.parquet, prefix_0001.parquet, etc.)
OUTPUT_PREFIX = "merged_chunk"

# Target size for each output file in GB (default: 1.6 GB)
TARGET_SIZE_GB = 1.6

# Number of CPU cores to use (None = all available)
NUM_WORKERS = None

# Use memory-efficient mode for very large datasets
# Set to True if you're running out of memory
MEMORY_EFFICIENT = False

# ============================================
# MAIN EXECUTION (DO NOT EDIT BELOW)
# ============================================

def main():
    """Run the chunked parquet merger with configured settings."""
    
    # Import the chunked merger
    from parquet_merger_chunked import ChunkedParquetMerger
    
    # Validate input directory
    if not os.path.exists(INPUT_DIR):
        print(f"ERROR: Input directory does not exist: {INPUT_DIR}")
        print("Please edit the INPUT_DIR variable in this script.")
        sys.exit(1)
    
    # Create output directory if it doesn't exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    print("=" * 60)
    print("PARQUET CHUNKED MERGER")
    print("=" * 60)
    print(f"Input directory: {INPUT_DIR}")
    print(f"Output directory: {OUTPUT_DIR}")
    print(f"Target chunk size: {TARGET_SIZE_GB} GB")
    print(f"Output file prefix: {OUTPUT_PREFIX}")
    print(f"Number of workers: {NUM_WORKERS or 'all available'}")
    print(f"Memory efficient mode: {MEMORY_EFFICIENT}")
    print("=" * 60)
    print()
    
    # Create merger instance
    merger = ChunkedParquetMerger(
        input_dir=INPUT_DIR,
        output_dir=OUTPUT_DIR,
        output_prefix=OUTPUT_PREFIX,
        target_size_gb=TARGET_SIZE_GB,
        num_workers=NUM_WORKERS
    )
    
    # Run the merger
    merger.run(memory_efficient=MEMORY_EFFICIENT)
    
    print("\n✅ Merge operation completed!")
    print(f"Output files are in: {OUTPUT_DIR}")
    print(f"Files are prefixed with: {OUTPUT_PREFIX}")
    
    # List the created files
    output_files = sorted(Path(OUTPUT_DIR).glob(f"{OUTPUT_PREFIX}_*.parquet"))
    if output_files:
        print(f"\nCreated {len(output_files)} chunk files:")
        for output_file in output_files:
            size_gb = output_file.stat().st_size / (1024**3)
            print(f"  - {output_file.name}: {size_gb:.2f} GB")


if __name__ == "__main__":
    main()