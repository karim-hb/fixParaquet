#!/usr/bin/env python3
"""
Chunked Parquet File Merger
Merges multiple Parquet files into multiple output files of a specified size (default ~1.6GB).
Optimized for handling large datasets while maintaining manageable file sizes.
"""

import os
import sys
import time
import argparse
import logging
import multiprocessing as mp
from pathlib import Path
from typing import List, Optional, Tuple
import warnings

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
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


class ChunkedParquetMerger:
    """Parquet file merger that outputs multiple files of specified size."""
    
    def __init__(self, 
                 input_dir: str, 
                 output_dir: str, 
                 output_prefix: str = "merged_chunk",
                 target_size_gb: float = 1.6,
                 num_workers: Optional[int] = None):
        """
        Initialize the Chunked Parquet merger.
        
        Args:
            input_dir: Directory containing Parquet files to merge
            output_dir: Directory where the merged files will be saved
            output_prefix: Prefix for output files (will be numbered)
            target_size_gb: Target size for each output file in GB
            num_workers: Number of parallel workers (defaults to CPU count)
        """
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.output_prefix = output_prefix
        self.target_size_bytes = int(target_size_gb * 1024 * 1024 * 1024)  # Convert GB to bytes
        
        # Use all available CPU cores if not specified
        self.num_workers = num_workers or mp.cpu_count()
        
        # Create output directory if it doesn't exist
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Initialized ChunkedParquetMerger")
        logger.info(f"Input directory: {self.input_dir}")
        logger.info(f"Output directory: {self.output_dir}")
        logger.info(f"Target file size: {target_size_gb} GB ({self.target_size_bytes:,} bytes)")
        logger.info(f"Number of workers: {self.num_workers}")
    
    def find_parquet_files(self) -> List[Path]:
        """Find all Parquet files in the input directory recursively."""
        logger.info("Scanning for Parquet files...")
        parquet_files = list(self.input_dir.rglob("*.parquet"))
        parquet_files.extend(self.input_dir.rglob("*.pq"))
        
        # Sort files for consistent processing
        parquet_files.sort()
        
        logger.info(f"Found {len(parquet_files)} Parquet files")
        return parquet_files
    
    def get_file_info(self, file_path: Path) -> Tuple[Path, int, int]:
        """
        Get file information including size and row count.
        
        Args:
            file_path: Path to the Parquet file
            
        Returns:
            Tuple of (file_path, file_size_bytes, row_count)
        """
        try:
            file_size = file_path.stat().st_size
            # Get row count without loading entire file
            parquet_file = pq.ParquetFile(file_path)
            row_count = parquet_file.metadata.num_rows
            return (file_path, file_size, row_count)
        except Exception as e:
            logger.warning(f"Failed to get info for {file_path}: {e}")
            return (file_path, 0, 0)
    
    def merge_to_chunks(self, parquet_files: List[Path]) -> None:
        """
        Merge Parquet files into multiple output files of target size.
        
        Args:
            parquet_files: List of Parquet file paths
        """
        if not parquet_files:
            logger.warning("No Parquet files found to merge")
            return
        
        # Get file information in parallel
        logger.info("Analyzing file sizes...")
        with mp.Pool(self.num_workers) as pool:
            file_infos = list(tqdm(
                pool.imap(self.get_file_info, parquet_files),
                total=len(parquet_files),
                desc="Analyzing files"
            ))
        
        # Filter out failed files
        file_infos = [(f, s, r) for f, s, r in file_infos if s > 0]
        
        if not file_infos:
            logger.error("No valid Parquet files found")
            return
        
        total_size = sum(info[1] for info in file_infos)
        total_rows = sum(info[2] for info in file_infos)
        estimated_chunks = max(1, int(total_size / self.target_size_bytes) + 1)
        
        logger.info(f"Total data size: {total_size / (1024**3):.2f} GB")
        logger.info(f"Total rows: {total_rows:,}")
        logger.info(f"Estimated output files: {estimated_chunks}")
        
        # Process files and write chunks
        current_chunk = 0
        current_tables = []
        current_size = 0
        files_processed = 0
        
        # Create progress bar
        pbar = tqdm(total=len(file_infos), desc="Processing files")
        
        for file_path, file_size, row_count in file_infos:
            try:
                # Read the parquet file
                table = pq.read_table(file_path)
                current_tables.append(table)
                current_size += file_size
                files_processed += 1
                pbar.update(1)
                
                # Check if we should write current chunk
                should_write = (
                    current_size >= self.target_size_bytes or 
                    files_processed == len(file_infos)  # Last file
                )
                
                if should_write and current_tables:
                    # Combine tables and write to file
                    combined_table = pa.concat_tables(current_tables)
                    output_path = self.output_dir / f"{self.output_prefix}_{current_chunk:04d}.parquet"
                    
                    logger.info(f"Writing chunk {current_chunk}: {output_path.name} "
                              f"({current_size / (1024**3):.2f} GB, "
                              f"{combined_table.num_rows:,} rows)")
                    
                    pq.write_table(
                        combined_table,
                        output_path,
                        compression='snappy',
                        use_dictionary=True,
                        compression_level=None,
                        use_byte_stream_split=False,
                        data_page_version='1.0'
                    )
                    
                    # Reset for next chunk
                    current_chunk += 1
                    current_tables = []
                    current_size = 0
                    
                    # Free memory
                    del combined_table
                    
            except Exception as e:
                logger.error(f"Failed to process {file_path}: {e}")
                pbar.update(1)
                continue
        
        pbar.close()
        
        logger.info(f"✅ Successfully created {current_chunk} output files")
        
        # List created files
        output_files = sorted(self.output_dir.glob(f"{self.output_prefix}_*.parquet"))
        logger.info("\nCreated files:")
        for output_file in output_files:
            size_gb = output_file.stat().st_size / (1024**3)
            logger.info(f"  - {output_file.name}: {size_gb:.2f} GB")
    
    def merge_to_chunks_memory_efficient(self, parquet_files: List[Path]) -> None:
        """
        Memory-efficient version that processes files in smaller batches.
        Use this for very large datasets or limited memory.
        
        Args:
            parquet_files: List of Parquet file paths
        """
        if not parquet_files:
            logger.warning("No Parquet files found to merge")
            return
        
        logger.info("Using memory-efficient chunking method...")
        
        # Get file information
        logger.info("Analyzing file sizes...")
        file_infos = []
        for file_path in tqdm(parquet_files, desc="Analyzing files"):
            info = self.get_file_info(file_path)
            if info[1] > 0:  # Valid file
                file_infos.append(info)
        
        if not file_infos:
            logger.error("No valid Parquet files found")
            return
        
        total_size = sum(info[1] for info in file_infos)
        logger.info(f"Total data size: {total_size / (1024**3):.2f} GB")
        
        current_chunk = 0
        current_writer = None
        current_size = 0
        current_rows = 0
        
        pbar = tqdm(total=len(file_infos), desc="Processing files")
        
        for file_path, file_size, row_count in file_infos:
            try:
                # Read the parquet file
                table = pq.read_table(file_path)
                
                # Initialize writer if needed
                if current_writer is None:
                    output_path = self.output_dir / f"{self.output_prefix}_{current_chunk:04d}.parquet"
                    current_writer = pq.ParquetWriter(
                        output_path,
                        table.schema,
                        compression='snappy',
                        use_dictionary=True,
                        data_page_version='1.0'
                    )
                
                # Write table to current chunk
                current_writer.write_table(table)
                current_size += file_size
                current_rows += table.num_rows
                
                # Free memory
                del table
                
                # Check if we should start a new chunk
                if current_size >= self.target_size_bytes:
                    current_writer.close()
                    logger.info(f"Completed chunk {current_chunk}: "
                              f"{output_path.name} ({current_size / (1024**3):.2f} GB, "
                              f"{current_rows:,} rows)")
                    
                    current_chunk += 1
                    current_writer = None
                    current_size = 0
                    current_rows = 0
                
                pbar.update(1)
                
            except Exception as e:
                logger.error(f"Failed to process {file_path}: {e}")
                pbar.update(1)
                continue
        
        # Close final writer if exists
        if current_writer is not None:
            current_writer.close()
            output_path = self.output_dir / f"{self.output_prefix}_{current_chunk:04d}.parquet"
            logger.info(f"Completed final chunk {current_chunk}: "
                      f"{output_path.name} ({current_size / (1024**3):.2f} GB, "
                      f"{current_rows:,} rows)")
        
        pbar.close()
        
        logger.info(f"✅ Successfully created {current_chunk + 1} output files")
        
        # List created files
        output_files = sorted(self.output_dir.glob(f"{self.output_prefix}_*.parquet"))
        logger.info("\nCreated files:")
        for output_file in output_files:
            size_gb = output_file.stat().st_size / (1024**3)
            logger.info(f"  - {output_file.name}: {size_gb:.2f} GB")
    
    def run(self, memory_efficient: bool = False) -> None:
        """
        Run the chunked merge operation.
        
        Args:
            memory_efficient: Use memory-efficient method (slower but uses less RAM)
        """
        start_time = time.time()
        
        # Find all parquet files
        parquet_files = self.find_parquet_files()
        
        if not parquet_files:
            logger.error("No Parquet files found in the input directory")
            return
        
        # Perform merge
        if memory_efficient:
            self.merge_to_chunks_memory_efficient(parquet_files)
        else:
            self.merge_to_chunks(parquet_files)
        
        elapsed_time = time.time() - start_time
        logger.info(f"Total processing time: {elapsed_time:.2f} seconds")


def main():
    """Main entry point for the script."""
    parser = argparse.ArgumentParser(
        description="Merge Parquet files into multiple chunks of specified size"
    )
    parser.add_argument(
        "input_dir",
        help="Directory containing Parquet files to merge"
    )
    parser.add_argument(
        "output_dir",
        help="Directory where merged chunk files will be saved"
    )
    parser.add_argument(
        "--prefix",
        default="merged_chunk",
        help="Prefix for output files (default: merged_chunk)"
    )
    parser.add_argument(
        "--size",
        type=float,
        default=1.6,
        help="Target size for each output file in GB (default: 1.6)"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Number of parallel workers (default: all CPU cores)"
    )
    parser.add_argument(
        "--memory-efficient",
        action="store_true",
        help="Use memory-efficient method (slower but uses less RAM)"
    )
    
    args = parser.parse_args()
    
    # Validate input directory
    if not os.path.exists(args.input_dir):
        logger.error(f"Input directory does not exist: {args.input_dir}")
        sys.exit(1)
    
    # Create merger instance
    merger = ChunkedParquetMerger(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        output_prefix=args.prefix,
        target_size_gb=args.size,
        num_workers=args.workers
    )
    
    # Run merge operation
    merger.run(memory_efficient=args.memory_efficient)


if __name__ == "__main__":
    main()