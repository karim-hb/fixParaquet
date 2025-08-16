#!/usr/bin/env python3
"""
High-Performance Parquet File Merger
Merges multiple Parquet files into a single large file using parallel processing.
Optimized for AMD Ryzen 7950X3D (16 cores/32 threads)
"""

import os
import sys
import time
import argparse
import logging
import multiprocessing as mp
from pathlib import Path
from typing import List, Optional, Tuple
from concurrent.futures import ProcessPoolExecutor, as_completed
from functools import partial
import warnings

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
from tqdm import tqdm

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class ParquetMerger:
    """High-performance Parquet file merger with parallel processing."""
    
    def __init__(self, 
                 input_dir: str, 
                 output_dir: str, 
                 output_filename: str = "merged_output.parquet",
                 num_workers: Optional[int] = None,
                 batch_size: int = 1000,
                 chunk_size: int = 100):
        """
        Initialize the Parquet merger.
        
        Args:
            input_dir: Directory containing Parquet files to merge
            output_dir: Directory where the merged file will be saved
            output_filename: Name of the output merged file
            num_workers: Number of parallel workers (defaults to CPU count)
            batch_size: Number of files to process in each batch
            chunk_size: Number of files to read in memory at once
        """
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.output_path = self.output_dir / output_filename
        
        # Use all available CPU cores if not specified
        self.num_workers = num_workers or mp.cpu_count()
        self.batch_size = batch_size
        self.chunk_size = chunk_size
        
        # Create output directory if it doesn't exist
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Initialized ParquetMerger with {self.num_workers} workers")
        logger.info(f"Input directory: {self.input_dir}")
        logger.info(f"Output path: {self.output_path}")
    
    def find_parquet_files(self) -> List[Path]:
        """Find all Parquet files in the input directory recursively."""
        logger.info("Scanning for Parquet files...")
        parquet_files = list(self.input_dir.rglob("*.parquet"))
        parquet_files.extend(self.input_dir.rglob("*.pq"))
        
        logger.info(f"Found {len(parquet_files)} Parquet files")
        return parquet_files
    
    @staticmethod
    def read_parquet_file(file_path: Path) -> Optional[pa.Table]:
        """
        Read a single Parquet file and return as PyArrow Table.
        
        Args:
            file_path: Path to the Parquet file
            
        Returns:
            PyArrow Table or None if error
        """
        try:
            return pq.read_table(file_path)
        except Exception as e:
            logger.warning(f"Failed to read {file_path}: {e}")
            return None
    
    def read_files_batch(self, file_paths: List[Path]) -> List[pa.Table]:
        """
        Read a batch of Parquet files in parallel.
        
        Args:
            file_paths: List of file paths to read
            
        Returns:
            List of PyArrow Tables
        """
        tables = []
        
        with ProcessPoolExecutor(max_workers=self.num_workers) as executor:
            futures = {executor.submit(self.read_parquet_file, fp): fp 
                      for fp in file_paths}
            
            for future in as_completed(futures):
                table = future.result()
                if table is not None:
                    tables.append(table)
        
        return tables
    
    def merge_tables_batch(self, tables: List[pa.Table]) -> pa.Table:
        """
        Merge a batch of PyArrow tables.
        
        Args:
            tables: List of PyArrow Tables to merge
            
        Returns:
            Merged PyArrow Table
        """
        if not tables:
            raise ValueError("No tables to merge")
        
        # Use PyArrow's concat_tables for efficient merging
        return pa.concat_tables(tables, promote=True)
    
    def process_in_batches(self, parquet_files: List[Path]) -> None:
        """
        Process Parquet files in batches to manage memory usage.
        
        Args:
            parquet_files: List of all Parquet file paths
        """
        total_files = len(parquet_files)
        total_batches = (total_files + self.batch_size - 1) // self.batch_size
        
        logger.info(f"Processing {total_files} files in {total_batches} batches")
        
        # Initialize the Parquet writer
        writer = None
        schema = None
        
        # Process files in batches with progress bar
        with tqdm(total=total_files, desc="Processing files", unit="files") as pbar:
            for batch_idx in range(0, total_files, self.batch_size):
                batch_files = parquet_files[batch_idx:batch_idx + self.batch_size]
                
                # Process batch in chunks for memory efficiency
                for chunk_idx in range(0, len(batch_files), self.chunk_size):
                    chunk_files = batch_files[chunk_idx:chunk_idx + self.chunk_size]
                    
                    # Read files in parallel
                    tables = self.read_files_batch(chunk_files)
                    
                    if tables:
                        # Merge tables in this chunk
                        merged_chunk = self.merge_tables_batch(tables)
                        
                        # Initialize writer with schema from first chunk
                        if writer is None:
                            schema = merged_chunk.schema
                            writer = pq.ParquetWriter(
                                self.output_path, 
                                schema,
                                compression='snappy',  # Fast compression
                                use_dictionary=True,
                                data_page_size=1024*1024,  # 1MB pages
                                write_statistics=True
                            )
                        
                        # Write the chunk to the output file
                        writer.write_table(merged_chunk)
                        
                        # Update progress
                        pbar.update(len(chunk_files))
                        
                        # Clear memory
                        del tables
                        del merged_chunk
        
        # Close the writer
        if writer:
            writer.close()
            logger.info(f"Successfully merged all files to {self.output_path}")
        else:
            logger.warning("No valid Parquet files were processed")
    
    def merge_parallel_optimized(self) -> None:
        """
        Main method to merge all Parquet files using parallel processing.
        Optimized for maximum CPU utilization.
        """
        start_time = time.time()
        
        # Find all Parquet files
        parquet_files = self.find_parquet_files()
        
        if not parquet_files:
            logger.error("No Parquet files found in the input directory")
            return
        
        # Process files in batches
        self.process_in_batches(parquet_files)
        
        # Calculate and report statistics
        elapsed_time = time.time() - start_time
        output_size = self.output_path.stat().st_size if self.output_path.exists() else 0
        
        logger.info(f"Merge completed in {elapsed_time:.2f} seconds")
        logger.info(f"Output file size: {output_size / (1024**3):.2f} GB")
        logger.info(f"Processing rate: {len(parquet_files) / elapsed_time:.2f} files/second")


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="High-performance Parquet file merger with parallel processing"
    )
    parser.add_argument(
        "input_dir",
        type=str,
        help="Directory containing Parquet files to merge"
    )
    parser.add_argument(
        "output_dir",
        type=str,
        help="Directory where the merged file will be saved"
    )
    parser.add_argument(
        "--output-filename",
        type=str,
        default="merged_output.parquet",
        help="Name of the output merged file (default: merged_output.parquet)"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Number of parallel workers (default: all CPU cores)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Number of files to process in each batch (default: 1000)"
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=100,
        help="Number of files to read in memory at once (default: 100)"
    )
    
    args = parser.parse_args()
    
    # Validate input directory
    if not os.path.exists(args.input_dir):
        logger.error(f"Input directory does not exist: {args.input_dir}")
        sys.exit(1)
    
    # Create merger instance
    merger = ParquetMerger(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        output_filename=args.output_filename,
        num_workers=args.workers,
        batch_size=args.batch_size,
        chunk_size=args.chunk_size
    )
    
    # Run the merge
    try:
        merger.merge_parallel_optimized()
        logger.info("Merge operation completed successfully!")
    except Exception as e:
        logger.error(f"Merge operation failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()