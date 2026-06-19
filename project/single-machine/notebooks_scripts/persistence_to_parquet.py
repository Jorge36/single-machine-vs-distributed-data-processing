import duckdb
import logging
from common import setup_logger, log_info_msg, log_error_msg, ResourceMonitor, compute_dataset_size_mib, PERSISTENCE_TO_PARQUET
from pathlib import Path
from typing import Any
import pandas as pd

# Persist a DataFrame to partitioned parquet files using DuckDB
# Data is partitioned by year and month
# parquet_path/year=YYYY/month=MM/
# Parameters:
# df: input DataFrame to persist
# parquet_path: output directory for parquet files
# table_name: temporary DuckDB table name used for the DataFrame
# threads: number of DuckDB execution threads
# logger: Logger instance for reporting progress and errors
# verify_count_rows: if to validate written parquet files by counting rows
# Returns:
# True if persistence completed successfully, false otherwise
# The number of created files
# Total size of the parquet files
def persist_partitioned_parquet_duckdb(
    df: pd.DataFrame, 
    parquet_path: str | Path, 
    table_name: str = "yellow_taxi", 
    threads: int = 0, 
    logger: logging.Logger | None = None, 
    verify_count_rows: bool = False,
    connection: duckdb.DuckDBPyConnection | None = None
) -> tuple[bool, int, float]:

    if logger is None:
        logger = setup_logger()

    MIB = 1024 * 1024
    valid_mask = df["year"].notna() & df["month"].notna()
    expected_rows = int(valid_mask.sum())
    nrows = len(df)
    
    if expected_rows != nrows:
        log_error_msg(logger, "Some rows have null year/month values, parquet persistence aborted")
        return False, 0, 0.0

    created_files = set()
    parquet_files_size_mib = 0.0
    created_local_connection = False
    
    try: 
        if connection is None:
            connection = duckdb.connect()
            created_local_connection = True
        if threads > 0:
            connection.execute(f"PRAGMA threads={threads}")
    except duckdb.Error as e:
        msg = f"Failed to initialize DuckDB connection (threads = {threads}): {type(e).__name__}: {e}"
        log_error_msg(logger, msg)
        return False, len(created_files), parquet_files_size_mib          

    try:
        
        connection.register(table_name, df)

        parquet_path = Path(parquet_path)
    
        existing_files = set(parquet_path.rglob("*.parquet"))
        
        query = f"""
        COPY (
            SELECT *
            FROM {table_name}
            WHERE year IS NOT NULL AND month IS NOT NULL
        ) TO '{parquet_path.as_posix()}'
        (FORMAT PARQUET, PARTITION_BY (year, month), APPEND true, FILENAME_PATTERN 'data_{{uuid}}');
        """

        connection.execute(query)
        
        new_files = set(parquet_path.rglob("*.parquet"))

        created_files = new_files - existing_files
        
        if not created_files:
            msg = f"No new parquet files were created in: {parquet_path}"
            log_error_msg(logger, msg)
            return False, len(created_files), parquet_files_size_mib

        size_bytes = [f.stat().st_size for f in created_files]
        parquet_files_size_mib = sum(size_bytes) / MIB

        created_paths = [f.as_posix() for f in created_files]
        paths_sql = ", ".join(f"'{p}'" for p in created_paths)
        
        if verify_count_rows:
            readback_nrows = connection.execute(f"SELECT COUNT(*) FROM read_parquet([{paths_sql}])").fetchone()[0]
            if readback_nrows != expected_rows:
                msg = f"Expected {expected_rows} rows, but new parquet files contain {readback_nrows}"
                log_error_msg(logger, msg)
                return False, len(created_files), parquet_files_size_mib

        msg = f"DataFrame persisted successfully to {parquet_path}. Created {len(created_files)} parquet file(s) with {expected_rows} row(s)"
        log_info_msg(logger, msg)
        return True, len(created_files), parquet_files_size_mib
               
    except Exception as e:
        msg = f"Failed to persist partitioned parquet: {type(e).__name__}: {e}"
        log_error_msg(logger, msg)
        return False, len(created_files), parquet_files_size_mib
        
    finally: 
        if connection is not None:
            try:
                connection.unregister(table_name)
            except Exception:
                pass
        if created_local_connection and connection is not None:
            connection.close()

# Build benchmark return for parquet persistence step
def build_return_dict_parquet_persistence(
    slice_path: str | Path,
    parquet_path: str | Path,
    operation: str,
    ntest: int, 
    processed_files: int,
    processed_rows: int,
    rows_after_cleaning: int,
    execution_time: float,
    throughput: float,
    parquet_files_size_mib: float,
    created_parquet_files: int,
    threads: int,
    benchmark_start_timestamp: str,
    benchmark_end_timestamp: str,
    msg: str,
    summary: pd.DataFrame,
    missing_values: pd.DataFrame,
    chunksize: int = 0,
    resource_stats: dict[str, Any] | None = None,
) -> dict[str, Any]:
 
    if resource_stats is None:
        resource_stats = ResourceMonitor.as_dict()

    return {
        # operations:
        "stage": PERSISTENCE_TO_PARQUET,
        # Type of operation
        "operation": operation,
        # Test Number
        "test_number": ntest,   
        # Slice path
        "slice_path": str(slice_path),
        # Parquet path:
        "parquet_path": str(parquet_path),
        # Performance
        "processed_files": processed_files,
        "processed_rows": processed_rows,
        "rows_after_cleaning": rows_after_cleaning,
        "execution_time_sec": execution_time,
        "throughput_rows_sec": throughput,
        "threads": threads,
        "chunksize": chunksize,
        "dataset_size_mib": compute_dataset_size_mib(slice_path),
        "parquet_files_size_mib": parquet_files_size_mib,
        "created_parquet_files": created_parquet_files,
        # Timestamps
        "benchmark_start_timestamp": benchmark_start_timestamp,
        "benchmark_end_timestamp": benchmark_end_timestamp,
        # Resources
        "avg_rss_mib": resource_stats["avg_rss_mib"],
        "peak_rss_mib": resource_stats["peak_rss_mib"],
        "memory_percent_of_total": resource_stats["memory_percent_of_total"],
        "avg_process_cpu_percent": resource_stats["avg_process_cpu_percent"],
        "peak_process_cpu_percent": resource_stats["peak_process_cpu_percent"],
        "avg_system_cpu_per_core_percent": resource_stats["avg_system_cpu_per_core_percent"],
        "peak_system_cpu_per_core_percent": resource_stats["peak_system_cpu_per_core_percent"],
        "logical_read_mib": resource_stats["logical_read_mib"],
        "logical_write_mib": resource_stats["logical_write_mib"],  
        "physical_read_mib": resource_stats["physical_read_mib"],
        "physical_write_mib": resource_stats["physical_write_mib"],  
        # Message
        "msg": msg,
        # Profiling
        "summary": summary,
        "missing_values": missing_values
    } 
