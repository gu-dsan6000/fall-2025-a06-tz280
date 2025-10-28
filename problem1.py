"""
Problem 1: Log Level Distribution Analysis
Analyzes the distribution of log levels (INFO, WARN, ERROR, DEBUG) across all Spark log files.

Usage:
    # Local testing with sample data
    python problem1.py local[*] --net-id YOUR-NET-ID --local-test
    
    # On cluster with full dataset
    uv run python problem1.py spark://MASTER_IP:7077 --net-id YOUR-NET-ID
"""

import sys
import argparse
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit
from pyspark.sql.types import StructType, StructField, StringType
import pandas as pd
import os


def extract_log_level(log_line):
    """
    Extract log level from a log line.
    
    Expected format: YY/MM/DD HH:MM:SS LEVEL ClassName: message
    Example: 17/03/29 10:04:41 INFO ApplicationMaster: Registered signal handlers
    
    Args:
        log_line: A single line from the log file
        
    Returns:
        Tuple of (log_line, log_level) or (log_line, None) if no level found
    """
    # Regex pattern to match log levels after timestamp
    # Pattern: date/time followed by a log level (INFO, WARN, ERROR, DEBUG)
    pattern = r'\d{2}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2}\s+(INFO|WARN|ERROR|DEBUG)\s+'
    
    match = re.search(pattern, log_line)
    if match:
        return (log_line, match.group(1))
    else:
        return (log_line, None)


def main(master_url, net_id, local_test=False):
    """
    Main function to process Spark logs and analyze log level distribution.
    
    Args:
        master_url: Spark master URL (e.g., "local[*]" or "spark://master:7077")
        net_id: Your net ID for S3 bucket access
        local_test: If True, uses local sample data instead of S3
    """
    
    print(f"Starting Problem 1: Log Level Distribution Analysis")
    print(f"Master URL: {master_url}")
    print(f"Net ID: {net_id}")
    print(f"Local test mode: {local_test}")
    print("-" * 80)
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("Problem1-LogLevelDistribution") \
        .master(master_url) \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()
    
    try:
        # Determine data path
        if local_test:
            # Use local sample data for testing
            data_path = "data/sample/application_*/container_*"
            print(f"Using local sample data: {data_path}")
        else:
            # Use S3 data for full analysis
            data_path = f"s3a://{net_id}-assignment-spark-cluster-logs/data/application_*/container_*"
            print(f"Using S3 data: {data_path}")
        
        # Read all log files as text
        print("\n1. Reading log files...")
        logs_rdd = spark.sparkContext.textFile(data_path)
        total_lines = logs_rdd.count()
        print(f"   Total log lines read: {total_lines:,}")
        
        # Extract log levels from each line
        print("\n2. Extracting log levels...")
        logs_with_levels = logs_rdd.map(extract_log_level)
        
        # Filter to only lines with identified log levels
        valid_logs = logs_with_levels.filter(lambda x: x[1] is not None)
        total_valid = valid_logs.count()
        print(f"   Lines with log levels: {total_valid:,}")
        
        # Count by log level
        print("\n3. Aggregating log level counts...")
        level_counts = valid_logs.map(lambda x: (x[1], 1)) \
                                 .reduceByKey(lambda a, b: a + b) \
                                 .collect()
        
        # Sort by count (descending)
        level_counts_sorted = sorted(level_counts, key=lambda x: x[1], reverse=True)
        
        print("\n   Log level distribution:")
        for level, count in level_counts_sorted:
            percentage = (count / total_valid) * 100 if total_valid > 0 else 0
            print(f"   {level:5s}: {count:,} ({percentage:.2f}%)")
        
        # Save counts to CSV using pandas
        output_path_counts = "data/output/problem1_counts.csv"
        print(f"\n4. Saving counts to {output_path_counts}...")
        
        # Create pandas DataFrame and save
        import pandas as pd
        counts_df = pd.DataFrame(level_counts_sorted, columns=['log_level', 'count'])
        os.makedirs('data/output', exist_ok=True)
        counts_df.to_csv(output_path_counts, index=False)
        print(f"   ✓ Saved: {output_path_counts}")
        
        # Sample 10 random log entries with their levels
        print("\n5. Sampling random log entries...")
        sample_logs = valid_logs.takeSample(False, 10, seed=42)
        
        # Save sample to CSV using pandas
        output_path_sample = "data/output/problem1_sample.csv"
        print(f"   Saving sample to {output_path_sample}...")
        
        sample_df = pd.DataFrame(sample_logs, columns=['log_entry', 'log_level'])
        sample_df.to_csv(output_path_sample, index=False)
        print(f"   ✓ Saved: {output_path_sample}")
        
        # Generate summary statistics
        print("\n6. Generating summary statistics...")
        output_path_summary = "data/output/problem1_summary.txt"
        
        with open(output_path_summary, 'w') as f:
            f.write("=" * 80 + "\n")
            f.write("Problem 1: Log Level Distribution - Summary Statistics\n")
            f.write("=" * 80 + "\n\n")
            
            f.write(f"Total log lines processed: {total_lines:,}\n")
            f.write(f"Total lines with log levels: {total_valid:,}\n")
            f.write(f"Lines without identifiable log levels: {total_lines - total_valid:,}\n")
            f.write(f"Unique log levels found: {len(level_counts_sorted)}\n")
            f.write("\n")
            
            f.write("Log level distribution:\n")
            f.write("-" * 80 + "\n")
            for level, count in level_counts_sorted:
                percentage = (count / total_valid) * 100 if total_valid > 0 else 0
                f.write(f"  {level:5s}: {count:>10,} ({percentage:>6.2f}%)\n")
            
            f.write("\n")
            f.write("=" * 80 + "\n")
            f.write("Analysis complete!\n")
            f.write("=" * 80 + "\n")
        
        print(f"   ✓ Saved: {output_path_summary}")
        
        # Display summary
        print("\n" + "=" * 80)
        print("SUMMARY")
        print("=" * 80)
        print(f"Total log lines processed: {total_lines:,}")
        print(f"Total lines with log levels: {total_valid:,}")
        print(f"Unique log levels found: {len(level_counts_sorted)}")
        print("\nLog level distribution:")
        for level, count in level_counts_sorted:
            percentage = (count / total_valid) * 100 if total_valid > 0 else 0
            print(f"  {level:5s}: {count:>10,} ({percentage:>6.2f}%)")
        print("=" * 80)
        
        print("\n✅ Problem 1 completed successfully!")
        print("\nOutput files generated:")
        print(f"  1. {output_path_counts}")
        print(f"  2. {output_path_sample}")
        print(f"  3. {output_path_summary}")
        
    except Exception as e:
        print(f"\n❌ Error occurred: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        # Stop Spark session
        spark.stop()
        print("\nSpark session stopped.")


if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(
        description="Problem 1: Analyze log level distribution in Spark logs"
    )
    parser.add_argument(
        "master",
        help="Spark master URL (e.g., 'local[*]' for local, 'spark://master:7077' for cluster)"
    )
    parser.add_argument(
        "--net-id",
        required=True,
        help="Your net ID (used for S3 bucket name)"
    )
    parser.add_argument(
        "--local-test",
        action="store_true",
        help="Use local sample data instead of S3 (for testing)"
    )
    
    args = parser.parse_args()
    
    # Run main function
    main(args.master, args.net_id, args.local_test)