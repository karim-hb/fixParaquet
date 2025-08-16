#!/usr/bin/env python3
"""
Ultra High-Performance Parquet File Merger using Dask
Optimized for millions of files and maximum CPU utilization on AMD Ryzen 7950X3D
"""

import os
import sys
import time
import argparse
import logging
from pathlib import Path
from typing import List, Optional
import warnings

import dask
import dask.dataframe as dd
from dask.distributed import Client, as_completed, progress
import pyarrow.parquet as pq
import pandas as pd

# Suppress warnings
warnings.filterwarnings('ignore')

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


class DaskParquetMerger:
    """Ultra-fast Parquet merger using Dask distributed computing."""
    
    def __init__(self,
                 input_dir: str,
                 output_dir: str,
                 output_filename: str = "merged_output.parquet",
                 num_workers: Optional[int] = None,
                 threads_per_worker: int = 2,
                 memory_limit: str = '4GB'):
        """
        Initialize the Dask-based Parquet merger.
        
        Args:
            input_dir: Directory containing Parquet files
            output_dir: Output directory for merged file
            output_filename: Name of output file
            num_workers: Number of Dask workers (default: CPU count / threads_per_worker)
            threads_per_worker: Threads per worker
            memory_limit: Memory limit per worker
        """
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.output_path = self.output_dir / output_filename
        
        # For Ryzen 7950X3D with 16 cores/32 threads
        # Optimal configuration: 16 workers with 2 threads each
        if num_workers is None:
            import multiprocessing
            num_workers = multiprocessing.cpu_count() // threads_per_worker
        
        self.num_workers = num_workers
        self.threads_per_worker = threads_per_worker
        self.memory_limit = memory_limit
        
        # Create output directory
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Configured Dask merger with {self.num_workers} workers")
        logger.info(f"Threads per worker: {self.threads_per_worker}")
        logger.info(f"Memory limit per worker: {self.memory_limit}")
    
    def find_parquet_files(self) -> List[str]:
        """Find all Parquet files in input directory."""
        logger.info("Scanning for Parquet files...")
        
        # Find all parquet files
        pattern1 = str(self.input_dir / "**/*.parquet")
        pattern2 = str(self.input_dir / "**/*.pq")
        
        # Return as string paths for Dask
        files = list(self.input_dir.rglob("*.parquet"))
        files.extend(self.input_dir.rglob("*.pq"))
        
        file_paths = [str(f) for f in files]
        logger.info(f"Found {len(file_paths)} Parquet files")
        
        return file_paths
    
    def merge_with_dask(self, file_paths: List[str]) -> None:
        """
        Merge Parquet files using Dask for maximum performance.
        
        Args:
            file_paths: List of Parquet file paths
        """
        # Initialize Dask client for distributed computing
        with Client(
            n_workers=self.num_workers,
            threads_per_worker=self.threads_per_worker,
            memory_limit=self.memory_limit,
            processes=True,  # Use processes for true parallelism
            silence_logs=logging.WARNING
        ) as client:
            
            logger.info(f"Dask dashboard available at: {client.dashboard_link}")
            
            # Read all Parquet files lazily with Dask
            logger.info("Creating Dask DataFrame from Parquet files...")
            
            # Use Dask to read all files in parallel
            # This creates a lazy computation graph
            ddf = dd.read_parquet(
                file_paths,
                engine='pyarrow',
                index=False,  # Don't use index for faster reading
                aggregate_files=True,  # Aggregate small files
                chunksize="128MB"  # Optimal chunk size for processing
            )
            
            # Get approximate row count (lazy operation)
            logger.info("Preparing merge operation...")
            
            # Repartition for optimal performance
            # This helps with memory management for millions of files
            optimal_partitions = self.num_workers * 4  # 4 partitions per worker
            ddf = ddf.repartition(npartitions=optimal_partitions)
            
            # Write merged Parquet file with optimal settings
            logger.info(f"Writing merged file to {self.output_path}")
            
            # Use to_parquet with optimized settings
            ddf.to_parquet(
                str(self.output_path),
                engine='pyarrow',
                compression='snappy',  # Fast compression
                write_index=False,
                write_metadata_file=True,  # Write _metadata file for faster reads
                overwrite=True,
                schema='infer'  # Infer schema from data
            )
            
            logger.info("Merge operation completed successfully!")
    
    def merge_with_batching(self, file_paths: List[str], batch_size: int = 10000) -> None:
        """
        Alternative method: Merge files in batches for extremely large datasets.
        
        Args:
            file_paths: List of Parquet file paths
            batch_size: Number of files per batch
        """
        total_files = len(file_paths)
        total_batches = (total_files + batch_size - 1) // batch_size
        
        logger.info(f"Processing {total_files} files in {total_batches} batches")
        
        with Client(
            n_workers=self.num_workers,
            threads_per_worker=self.threads_per_worker,
            memory_limit=self.memory_limit,
            processes=True,
            silence_logs=logging.WARNING
        ) as client:
            
            logger.info(f"Dask dashboard: {client.dashboard_link}")
            
            # Process first batch to get schema
            first_batch = file_paths[:batch_size]
            ddf = dd.read_parquet(first_batch, engine='pyarrow', index=False)
            
            # Write first batch
            ddf.to_parquet(
                str(self.output_path),
                engine='pyarrow',
                compression='snappy',
                write_index=False,
                overwrite=True
            )
            
            # Append remaining batches
            for i in range(batch_size, total_files, batch_size):
                batch = file_paths[i:i + batch_size]
                logger.info(f"Processing batch {i // batch_size + 1}/{total_batches}")
                
                ddf = dd.read_parquet(batch, engine='pyarrow', index=False)
                
                # Append to existing file
                ddf.to_parquet(
                    str(self.output_path),
                    engine='pyarrow',
                    compression='snappy',
                    write_index=False,
                    append=True,  # Append mode
                    overwrite=False
                )
            
            logger.info("Batch processing completed!")
    
    def run(self, use_batching: bool = False, batch_size: int = 10000) -> None:
        """
        Main entry point to run the merge operation.
        
        Args:
            use_batching: Whether to use batch processing
            batch_size: Size of batches if batching is enabled
        """
        start_time = time.time()
        
        # Find all Parquet files
        file_paths = self.find_parquet_files()
        
        if not file_paths:
            logger.error("No Parquet files found!")
            return
        
        # Choose merge strategy
        if use_batching or len(file_paths) > 100000:  # Use batching for > 100k files
            logger.info("Using batch processing strategy")
            self.merge_with_batching(file_paths, batch_size)
        else:
            logger.info("Using direct merge strategy")
            self.merge_with_dask(file_paths)
        
        # Report statistics
        elapsed_time = time.time() - start_time
        
        if self.output_path.exists():
            output_size = self.output_path.stat().st_size
            logger.info(f"Merge completed in {elapsed_time:.2f} seconds")
            logger.info(f"Output file size: {output_size / (1024**3):.2f} GB")
            logger.info(f"Processing rate: {len(file_paths) / elapsed_time:.2f} files/second")
        else:
            logger.error("Output file was not created!")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Ultra-fast Parquet merger using Dask distributed computing"
    )
    parser.add_argument(
        "input_dir",
        type=str,
        help="Directory containing Parquet files"
    )
    parser.add_argument(
        "output_dir",
        type=str,
        help="Output directory for merged file"
    )
    parser.add_argument(
        "--output-filename",
        type=str,
        default="merged_output.parquet",
        help="Output filename (default: merged_output.parquet)"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Number of Dask workers (default: auto)"
    )
    parser.add_argument(
        "--threads-per-worker",
        type=int,
        default=2,
        help="Threads per worker (default: 2)"
    )
    parser.add_argument(
        "--memory-limit",
        type=str,
        default="4GB",
        help="Memory limit per worker (default: 4GB)"
    )
    parser.add_argument(
        "--use-batching",
        action="store_true",
        help="Use batch processing for extremely large datasets"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10000,
        help="Batch size for batch processing (default: 10000)"
    )
    
    args = parser.parse_args()
    
    # Validate input
    if not os.path.exists(args.input_dir):
        logger.error(f"Input directory does not exist: {args.input_dir}")
        sys.exit(1)
    
    # Create merger
    merger = DaskParquetMerger(
        input_dir=args.input_dir,
        output_dir=args.output_dir,
        output_filename=args.output_filename,
        num_workers=args.workers,
        threads_per_worker=args.threads_per_worker,
        memory_limit=args.memory_limit
    )
    
    # Run merge
    try:
        merger.run(
            use_batching=args.use_batching,
            batch_size=args.batch_size
        )
    except Exception as e:
        logger.error(f"Merge failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()