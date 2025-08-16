#!/usr/bin/env python3
"""
Simple script to merge Parquet files with predefined settings.
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

# Directory where the merged file will be saved
OUTPUT_DIR = "/path/to/output/directory"

# Name of the merged output file
OUTPUT_FILENAME = "merged_data.parquet"

# Choose which merger to use:
# - "multiprocessing" for the standard parallel merger
# - "dask" for the Dask-based merger (better for millions of files)
MERGER_TYPE = "dask"  

# Number of CPU cores to use (None = all available)
NUM_WORKERS = None

# Memory limit per worker (for Dask)
MEMORY_LIMIT = "4GB"

# Use batching for extremely large datasets (millions of files)
USE_BATCHING = True

# Batch size (number of files per batch)
BATCH_SIZE = 10000

# ============================================
# MAIN EXECUTION (DO NOT EDIT BELOW)
# ============================================

def main():
    """Run the appropriate merger based on configuration."""
    
    # Validate input directory
    if not os.path.exists(INPUT_DIR):
        print(f"ERROR: Input directory does not exist: {INPUT_DIR}")
        print("Please edit the INPUT_DIR variable in this script.")
        sys.exit(1)
    
    # Create output directory if it doesn't exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    if MERGER_TYPE == "multiprocessing":
        # Use the multiprocessing-based merger
        from parquet_merger import ParquetMerger
        
        print(f"Using multiprocessing merger with {NUM_WORKERS or 'all'} CPU cores")
        
        merger = ParquetMerger(
            input_dir=INPUT_DIR,
            output_dir=OUTPUT_DIR,
            output_filename=OUTPUT_FILENAME,
            num_workers=NUM_WORKERS,
            batch_size=BATCH_SIZE,
            chunk_size=100
        )
        
        merger.merge_parallel_optimized()
        
    elif MERGER_TYPE == "dask":
        # Use the Dask-based merger
        from parquet_merger_dask import DaskParquetMerger
        
        print(f"Using Dask merger with {NUM_WORKERS or 'auto'} workers")
        print(f"Memory limit per worker: {MEMORY_LIMIT}")
        
        merger = DaskParquetMerger(
            input_dir=INPUT_DIR,
            output_dir=OUTPUT_DIR,
            output_filename=OUTPUT_FILENAME,
            num_workers=NUM_WORKERS,
            memory_limit=MEMORY_LIMIT
        )
        
        merger.run(use_batching=USE_BATCHING, batch_size=BATCH_SIZE)
        
    else:
        print(f"ERROR: Invalid MERGER_TYPE: {MERGER_TYPE}")
        print("Please use either 'multiprocessing' or 'dask'")
        sys.exit(1)
    
    print("\n✅ Merge operation completed!")
    print(f"Output file: {os.path.join(OUTPUT_DIR, OUTPUT_FILENAME)}")


if __name__ == "__main__":
    main()