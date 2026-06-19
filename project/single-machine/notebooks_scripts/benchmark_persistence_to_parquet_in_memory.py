import pandas as pd
from persistence_to_parquet import persist_partitioned_parquet_duckdb, build_return_dict_parquet_persistence
import logging
from pathlib import Path
import gc
from common import setup_logger, log_info_msg, log_error_msg, Timer, ResourceMonitor, CSV_PATTERN, IN_MEMORY, is_empty, save_result_to_csv_and_json_cleaning
from typing import Any
from benchmark_cleaning_transf_profiling_in_memory import pandas_ingestion, profile_vendor_specific_differences, profile_missing_values
from cleaning_transformation import extract_year_month_from_filename, apply_master_schema, cleaning_transformation


# Perform in-memory cleaning, transformation and profiling using Pandas library
# Ingest all CSV files from the given slice directory
# Applies schema normalisation
# Performs cleaning and transformation steps
# Computes profiling statistics
# Parameters:
# slice_path: input slice directory containing CSV files
# Returns:
# cleander and transformed DataFrame, number of successfully processed files,
# if number of successfully processed files, profiling DataFrames 
def pandas_cleaning_transformation(
    slice_path: str | Path, 
    logger: logging.Logger | None = None
) -> tuple[pd.DataFrame, int, int, bool, pd.DataFrame, pd.DataFrame]:

    if logger is None:
        logger = setup_logger()

    REMOVED_FOLDER = "removed"
    
    df, total_files, total_rows, ingested = pandas_ingestion(slice_path = slice_path, logger = logger)

    if not ingested:
        return df, total_files, total_rows, False, pd.DataFrame, pd.DataFrame
    
    df_normalised = pd.DataFrame()
    if not is_empty(df): 
        # Apply new schema
        df_normalised = apply_master_schema(df = df, logger = logger)
        del df
        gc.collect()
    df_cleaned = pd.DataFrame()
    if not is_empty(df_normalised):
        # Clean dataset and transform it
        removed_slice_path = Path(slice_path)
        removed_slice_path = removed_slice_path.parents[1] / REMOVED_FOLDER / removed_slice_path.name
        removed_slice_path.mkdir(parents = True, exist_ok = True)
        df_cleaned = cleaning_transformation(df = df_normalised, removed_slice_path = removed_slice_path, logger = logger, process_type = IN_MEMORY, header = True)
        del df_normalised
        gc.collect()
    # Profiling
    summary = pd.DataFrame()
    missing_values = pd.DataFrame()
    if not is_empty(df_cleaned):
        summary = profile_vendor_specific_differences(df_cleaned)
        missing_values = profile_missing_values(df_cleaned)

    log_info_msg(logger, "Cleaning and transformation completed")
    
    return df_cleaned, total_files, total_rows, True, summary, missing_values


# Benchamrk persistence of cleaned data to partitioned parquet using duckdb
# Parameters:
# slice_path: input slice directory containing CSV files
# parquet_path: output directory for partitioned parquet files
# threads: number of DuckDB execution threads
# logger: Logger instance for reporting progress and errors
# ntest: test number
# verify_count_rows: if to verify row counts after compaction
# Returns:
# benchmark results including slice path, processed files, processed rows, rows after cleaning, execution time, 
# throughput, threads, timestamps, resource statistics, a message and profiling results
def benchmark_duckdb_parquet_persistence(
    slice_path: str | Path, 
    ntest: int,
    parquet_path: str | Path, 
    threads: int = 0, 
    logger: logging.Logger | None = None,
    verify_count_rows: bool = False,
) -> dict[str, Any]:

    if logger is None:
        logger = setup_logger()

    timer = Timer()
    monitor = ResourceMonitor()
    benchmark_start_timestamp = Timer.get_timestamp()
    print(benchmark_start_timestamp.isoformat())
    timer.start("total")
    monitor.start()
    MSG_ERROR = "Ingestion, cleaning, transformation, profiling and persistence to parquet completed with errors, check pipeline.log"
    created_parquet_files = 0
    df, total_files, total_rows, cleaned_ok, summary, missing_values = pandas_cleaning_transformation(slice_path = slice_path, logger = logger)
    
    if not cleaned_ok:
        timer.stop("total")
        resource_stats = monitor.stop()
        benchmark_end_timestamp = Timer.get_timestamp()
        execution_time = timer.duration("total")
        throughput = total_rows / execution_time if execution_time > 0 else 0.0
        return build_return_dict_parquet_persistence(slice_path = slice_path, parquet_path = parquet_path, operation = IN_MEMORY, ntest = ntest, processed_files = total_files, processed_rows = total_rows, 
                                                     rows_after_cleaning = len(df), execution_time = execution_time, throughput = throughput,
                                                     threads = threads, benchmark_start_timestamp = benchmark_start_timestamp.isoformat(),
                                                     benchmark_end_timestamp = benchmark_end_timestamp.isoformat(), created_parquet_files = created_parquet_files,
                                                     msg = MSG_ERROR, summary = summary, missing_values = missing_values, 
                                                     parquet_files_size_mib = 0.0)
            
    persisted = False
    parquet_files_size_mib = 0.0
    if not is_empty(df):
        persisted, created_parquet_files, parquet_files_size_mib = persist_partitioned_parquet_duckdb(df = df, parquet_path = Path(parquet_path), threads = threads,
                                                                                                      logger = logger, verify_count_rows = verify_count_rows)
        
    timer.stop("total")
    resource_stats = monitor.stop()
    benchmark_end_timestamp = Timer.get_timestamp()
    
    execution_time = timer.duration("total")
    
    throughput = total_rows / execution_time if execution_time > 0 else 0.0
        
    print(f"DuckDB persisted {total_files} file(s) and {total_rows} row(s) to Parquet in {execution_time:.2f} seconds")
    print(f"Throughput: {throughput:.2f} rows/sec")

    if persisted:
        msg = "Ingestion, cleaning, transformation, profiling and persistence to parquet completed successfully"
    else:
        msg = "Ingestion, cleaning, transformation, profiling and persistence to parquet completed with errors, check pipeline.log"

    return build_return_dict_parquet_persistence(slice_path = slice_path, parquet_path = parquet_path, operation = IN_MEMORY, ntest = ntest, processed_files = total_files, processed_rows = total_rows, 
                                                 rows_after_cleaning = len(df), execution_time = execution_time, throughput = throughput,
                                                 threads = threads, benchmark_start_timestamp = benchmark_start_timestamp.isoformat(),
                                                 benchmark_end_timestamp = benchmark_end_timestamp.isoformat(),
                                                 resource_stats = resource_stats, created_parquet_files = created_parquet_files,
                                                 msg = msg, summary = summary, missing_values = missing_values,
                                                 parquet_files_size_mib = parquet_files_size_mib)

if __name__ == "__main__":
    # Create logger
    logger = setup_logger()
    result = benchmark_duckdb_parquet_persistence(slice_path = "../data/CSV/raw-data/1st-slice/", 
                                                parquet_path= "../data/parquet/1st-slice/", 
                                                logger = logger, 
                                                ntest = 6, 
                                                verify_count_rows = True, 
                                                threads = 0)
    log_info_msg(logger, result["msg"])
    print(result["msg"])
    timestamp = result['benchmark_start_timestamp'].replace(":", "-")
    save_result_to_csv_and_json_cleaning(result, 
                                	 f"./persistence/csv/persistence_sm.csv",
                                	 f"./persistence/json/persistence_{result['test_number']}_{timestamp}.json")
