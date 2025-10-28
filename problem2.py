"""
Problem 2: Cluster Usage Analysis
Analyzes cluster usage patterns to understand which clusters are most heavily used over time.

Usage:
    # Local testing with sample data
    python problem2.py local[*] --net-id YOUR-NET-ID --local-test
    
    # Full Spark processing on cluster (10-20 minutes)
    uv run python problem2.py spark://MASTER_IP:7077 --net-id YOUR-NET-ID
    
    # Skip Spark and regenerate visualizations from existing CSVs (fast)
    uv run python problem2.py --skip-spark
"""

import sys
import argparse
import re
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, min as spark_min, max as spark_max, count
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np


def parse_application_path(path):
    """
    Parse application directory path to extract cluster_id, application_id, and app_number.
    
    Expected format: .../application_<cluster_id>_<app_number>/container_*
    Example: s3a://bucket/data/application_1485248649253_0052/container_000001
    
    Args:
        path: Full path to the log file
        
    Returns:
        Tuple of (cluster_id, application_id, app_number) or (None, None, None) if parsing fails
    """
    # Extract application directory name from path
    pattern = r'application_(\d+)_(\d+)'
    match = re.search(pattern, path)
    
    if match:
        cluster_id = match.group(1)
        app_number = match.group(2)
        application_id = f"application_{cluster_id}_{app_number}"
        return (cluster_id, application_id, app_number)
    else:
        return (None, None, None)


def extract_timestamp(log_line):
    """
    Extract timestamp from a log line.
    
    Expected format: YY/MM/DD HH:MM:SS LEVEL ClassName: message
    Example: 17/03/29 10:04:41 INFO ApplicationMaster: Registered signal handlers
    
    Args:
        log_line: A single line from the log file
        
    Returns:
        datetime object or None if no timestamp found
    """
    # Pattern to match timestamp at the beginning of log line
    pattern = r'^(\d{2}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2})'
    
    match = re.search(pattern, log_line)
    if match:
        timestamp_str = match.group(1)
        try:
            # Parse timestamp (assumes 20XX year)
            dt = datetime.strptime(timestamp_str, '%y/%m/%d %H:%M:%S')
            return dt
        except ValueError:
            return None
    return None


def process_logs_spark(spark, data_path):
    """
    Process logs using Spark to extract cluster usage patterns.
    
    Args:
        spark: SparkSession
        data_path: Path to log files (S3 or local)
        
    Returns:
        pandas DataFrame with timeline data
    """
    print("\n" + "="*80)
    print("SPARK PROCESSING STARTED")
    print("="*80)
    
    # Read all log files with their paths
    print("\n1. Reading log files with file paths...")
    logs_rdd = spark.sparkContext.wholeTextFiles(data_path)
    print(f"   Total files read: {logs_rdd.count():,}")
    
    # Parse each file to extract cluster info and timestamps
    print("\n2. Parsing application info and timestamps...")
    
    def parse_file(file_tuple):
        """Parse a single file to extract cluster info and timestamps."""
        path, content = file_tuple
        
        # Extract cluster and app info from path
        cluster_id, application_id, app_number = parse_application_path(path)
        
        if cluster_id is None:
            return []
        
        # Extract all timestamps from log lines
        timestamps = []
        for line in content.split('\n'):
            ts = extract_timestamp(line)
            if ts:
                timestamps.append(ts)
        
        if not timestamps:
            return []
        
        # Get min and max timestamps
        start_time = min(timestamps)
        end_time = max(timestamps)
        
        return [(cluster_id, application_id, app_number, start_time, end_time)]
    
    # Process all files
    results = logs_rdd.flatMap(parse_file).collect()
    print(f"   Applications processed: {len(results):,}")
    
    # Convert to pandas DataFrame
    print("\n3. Creating DataFrame...")
    df = pd.DataFrame(results, columns=['cluster_id', 'application_id', 'app_number', 'start_time', 'end_time'])
    
    # Sort by cluster_id and start_time
    df = df.sort_values(['cluster_id', 'start_time']).reset_index(drop=True)
    
    print(f"   DataFrame shape: {df.shape}")
    print(f"   Unique clusters: {df['cluster_id'].nunique()}")
    print(f"   Total applications: {len(df)}")
    
    return df


def generate_cluster_summary(timeline_df):
    """
    Generate cluster summary statistics.
    
    Args:
        timeline_df: DataFrame with timeline data
        
    Returns:
        pandas DataFrame with cluster summary
    """
    print("\n4. Generating cluster summary...")
    
    summary = timeline_df.groupby('cluster_id').agg({
        'application_id': 'count',
        'start_time': 'min',
        'end_time': 'max'
    }).reset_index()
    
    summary.columns = ['cluster_id', 'num_applications', 'cluster_first_app', 'cluster_last_app']
    summary = summary.sort_values('num_applications', ascending=False)
    
    print(f"   Cluster summary generated for {len(summary)} clusters")
    
    return summary


def generate_stats_file(timeline_df, cluster_summary_df, output_path):
    """
    Generate summary statistics text file.
    
    Args:
        timeline_df: DataFrame with timeline data
        cluster_summary_df: DataFrame with cluster summary
        output_path: Path to save the stats file
    """
    print("\n5. Generating summary statistics...")
    
    total_clusters = timeline_df['cluster_id'].nunique()
    total_apps = len(timeline_df)
    avg_apps_per_cluster = total_apps / total_clusters if total_clusters > 0 else 0
    
    with open(output_path, 'w') as f:
        f.write("=" * 80 + "\n")
        f.write("Problem 2: Cluster Usage Analysis - Summary Statistics\n")
        f.write("=" * 80 + "\n\n")
        
        f.write(f"Total unique clusters: {total_clusters}\n")
        f.write(f"Total applications: {total_apps}\n")
        f.write(f"Average applications per cluster: {avg_apps_per_cluster:.2f}\n")
        f.write("\n")
        
        f.write("Most heavily used clusters:\n")
        f.write("-" * 80 + "\n")
        for _, row in cluster_summary_df.iterrows():
            f.write(f"  Cluster {row['cluster_id']}: {row['num_applications']} applications\n")
        
        f.write("\n")
        f.write("Cluster timeline details:\n")
        f.write("-" * 80 + "\n")
        for _, row in cluster_summary_df.iterrows():
            f.write(f"  Cluster {row['cluster_id']}:\n")
            f.write(f"    First app: {row['cluster_first_app']}\n")
            f.write(f"    Last app:  {row['cluster_last_app']}\n")
            f.write(f"    Duration:  {row['cluster_last_app'] - row['cluster_first_app']}\n")
            f.write("\n")
        
        f.write("=" * 80 + "\n")
        f.write("Analysis complete!\n")
        f.write("=" * 80 + "\n")
    
    print(f"   ✓ Saved: {output_path}")


def generate_visualizations(timeline_df, cluster_summary_df):
    """
    Generate bar chart and density plot visualizations.
    
    Args:
        timeline_df: DataFrame with timeline data
        cluster_summary_df: DataFrame with cluster summary
    """
    print("\n6. Generating visualizations...")
    
    # Set style
    sns.set_style("whitegrid")
    plt.rcParams['figure.dpi'] = 100
    
    # === Bar Chart: Applications per Cluster ===
    print("   Creating bar chart...")
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Sort by number of applications for better visualization
    plot_data = cluster_summary_df.sort_values('num_applications', ascending=False)
    
    # Create bar chart
    bars = ax.bar(range(len(plot_data)), 
                   plot_data['num_applications'],
                   color=sns.color_palette("husl", len(plot_data)))
    
    # Add value labels on top of bars
    for i, (bar, value) in enumerate(zip(bars, plot_data['num_applications'])):
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(value)}',
                ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    # Set labels and title
    ax.set_xlabel('Cluster ID', fontsize=12, fontweight='bold')
    ax.set_ylabel('Number of Applications', fontsize=12, fontweight='bold')
    ax.set_title('Applications per Cluster', fontsize=14, fontweight='bold', pad=20)
    
    # Set x-axis labels
    ax.set_xticks(range(len(plot_data)))
    ax.set_xticklabels(plot_data['cluster_id'], rotation=45, ha='right')
    
    # Add grid
    ax.grid(axis='y', alpha=0.3)
    
    plt.tight_layout()
    bar_chart_path = 'data/output/problem2_bar_chart.png'
    plt.savefig(bar_chart_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"   ✓ Saved: {bar_chart_path}")
    
    # === Density Plot: Job Duration Distribution (Largest Cluster) ===
    print("   Creating density plot...")
    
    # Find the largest cluster
    largest_cluster_id = cluster_summary_df.iloc[0]['cluster_id']
    largest_cluster_data = timeline_df[timeline_df['cluster_id'] == largest_cluster_id].copy()
    
    # Calculate job duration in minutes
    largest_cluster_data['duration_minutes'] = (
        (largest_cluster_data['end_time'] - largest_cluster_data['start_time']).dt.total_seconds() / 60
    )
    
    # Remove zero or negative durations
    largest_cluster_data = largest_cluster_data[largest_cluster_data['duration_minutes'] > 0]
    
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Use seaborn's histplot which includes KDE automatically
    durations = largest_cluster_data['duration_minutes']
    
    # Create histogram with KDE overlay using seaborn
    sns.histplot(data=durations, bins=30, kde=True, stat='density', 
                 alpha=0.6, color='skyblue', edgecolor='black', 
                 line_kws={'linewidth': 2, 'color': 'red'}, ax=ax)
    
    # Set log scale on x-axis to handle skewed data
    ax.set_xscale('log')
    
    # Labels and title
    ax.set_xlabel('Job Duration (minutes, log scale)', fontsize=12, fontweight='bold')
    ax.set_ylabel('Density', fontsize=12, fontweight='bold')
    ax.set_title(f'Job Duration Distribution - Cluster {largest_cluster_id} (n={len(durations)})',
                 fontsize=14, fontweight='bold', pad=20)
    
    ax.grid(alpha=0.3)
    
    plt.tight_layout()
    density_plot_path = 'data/output/problem2_density_plot.png'
    plt.savefig(density_plot_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"   ✓ Saved: {density_plot_path}")


def main(master_url=None, net_id=None, local_test=False, skip_spark=False):
    """
    Main function to process Spark logs and analyze cluster usage.
    
    Args:
        master_url: Spark master URL (required if not skip_spark)
        net_id: Your net ID for S3 bucket access (required if not skip_spark and not local_test)
        local_test: If True, uses local sample data instead of S3
        skip_spark: If True, skip Spark processing and use existing CSVs
    """
    print("=" * 80)
    print("Problem 2: Cluster Usage Analysis")
    print("=" * 80)
    if net_id:
        print(f"Net ID: {net_id}")
    print(f"Local test mode: {local_test}")
    print(f"Skip Spark: {skip_spark}")
    print("-" * 80)
    
    # Create output directory
    os.makedirs('data/output', exist_ok=True)
    
    timeline_csv_path = 'data/output/problem2_timeline.csv'
    cluster_summary_csv_path = 'data/output/problem2_cluster_summary.csv'
    stats_txt_path = 'data/output/problem2_stats.txt'
    
    if skip_spark:
        # Skip Spark processing, load existing CSVs
        print("\n⚡ Skipping Spark processing, loading existing CSVs...")
        
        if not os.path.exists(timeline_csv_path) or not os.path.exists(cluster_summary_csv_path):
            print("❌ Error: CSV files not found. Run with Spark processing first.")
            sys.exit(1)
        
        timeline_df = pd.read_csv(timeline_csv_path, parse_dates=['start_time', 'end_time'])
        cluster_summary_df = pd.read_csv(cluster_summary_csv_path, parse_dates=['cluster_first_app', 'cluster_last_app'])
        
        print(f"   ✓ Loaded timeline data: {timeline_df.shape}")
        print(f"   ✓ Loaded cluster summary: {cluster_summary_df.shape}")
        
    else:
        # Full Spark processing
        if master_url is None:
            print("❌ Error: master_url is required when not skipping Spark")
            sys.exit(1)
        
        # Create Spark session
        print(f"\nCreating Spark session with master: {master_url}")
        spark = SparkSession.builder \
            .appName("Problem2-ClusterUsageAnalysis") \
            .master(master_url) \
            .config("spark.executor.memory", "2g") \
            .config("spark.driver.memory", "2g") \
            .getOrCreate()
        
        try:
            # Determine data path
            if local_test:
                data_path = "data/sample/application_*/container_*"
                print(f"Using local sample data: {data_path}")
            else:
                if net_id is None:
                    print("❌ Error: net_id is required when not using local test mode")
                    sys.exit(1)
                data_path = f"s3a://{net_id}-assignment-spark-cluster-logs/data/application_*/container_*"
                print(f"Using S3 data: {data_path}")
            
            # Process logs with Spark
            timeline_df = process_logs_spark(spark, data_path)
            
            # Generate cluster summary
            cluster_summary_df = generate_cluster_summary(timeline_df)
            
            # Save CSV files
            print("\n   Saving CSV files...")
            timeline_df.to_csv(timeline_csv_path, index=False)
            print(f"   ✓ Saved: {timeline_csv_path}")
            
            cluster_summary_df.to_csv(cluster_summary_csv_path, index=False)
            print(f"   ✓ Saved: {cluster_summary_csv_path}")
            
        except Exception as e:
            print(f"\n❌ Error during Spark processing: {str(e)}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
        
        finally:
            spark.stop()
            print("\nSpark session stopped.")
    
    # Generate stats file
    generate_stats_file(timeline_df, cluster_summary_df, stats_txt_path)
    
    # Generate visualizations
    generate_visualizations(timeline_df, cluster_summary_df)
    
    # Print summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total unique clusters: {timeline_df['cluster_id'].nunique()}")
    print(f"Total applications: {len(timeline_df)}")
    print(f"Average applications per cluster: {len(timeline_df) / timeline_df['cluster_id'].nunique():.2f}")
    print("\nMost heavily used clusters:")
    for _, row in cluster_summary_df.head(5).iterrows():
        print(f"  Cluster {row['cluster_id']}: {row['num_applications']} applications")
    print("=" * 80)
    
    print("\n✅ Problem 2 completed successfully!")
    print("\nOutput files generated:")
    print(f"  1. {timeline_csv_path}")
    print(f"  2. {cluster_summary_csv_path}")
    print(f"  3. {stats_txt_path}")
    print(f"  4. data/output/problem2_bar_chart.png")
    print(f"  5. data/output/problem2_density_plot.png")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Problem 2: Analyze cluster usage patterns in Spark logs"
    )
    parser.add_argument(
        "master",
        nargs='?',
        default=None,
        help="Spark master URL (e.g., 'local[*]' for local, 'spark://master:7077' for cluster). Not required with --skip-spark"
    )
    parser.add_argument(
        "--net-id",
        help="Your net ID (used for S3 bucket name). Required unless using --local-test or --skip-spark"
    )
    parser.add_argument(
        "--local-test",
        action="store_true",
        help="Use local sample data instead of S3 (for testing)"
    )
    parser.add_argument(
        "--skip-spark",
        action="store_true",
        help="Skip Spark processing and regenerate visualizations from existing CSVs"
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if not args.skip_spark:
        if args.master is None:
            parser.error("master URL is required unless --skip-spark is specified")
        if not args.local_test and args.net_id is None:
            parser.error("--net-id is required unless using --local-test or --skip-spark")
    
    # Run main function
    main(args.master, args.net_id, args.local_test, args.skip_spark)