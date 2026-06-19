import pandas as pd
from pathlib import Path
from common import setup_logger, log_info_msg, log_error_msg, Timer, ResourceMonitor, CSV_PATTERN, IN_MEMORY, is_empty, save_result_to_csv_and_json_cleaning
from typing import Any
import logging
import gc
from cleaning_transformation import build_return_dict_cleaning_transformation, extract_year_month_from_filename, apply_master_schema, cleaning_transformation

# Profiles by computing missing value count and percentage per column
# and returning a DataFrame sorted in descending order of the number of missing values
def profile_missing_values(
    df: pd.DataFrame
) -> pd.DataFrame:

    total_rows = len(df)
    missing_count = df.isna().sum()
    missing_percent = ((missing_count / total_rows) * 100).round(2)

    result = pd.DataFrame({
        "missing_count": missing_count,
        "missing_percent": missing_percent
    })
    
    return result.sort_values(by = "missing_count", ascending = False)

# Profiles vendor-specific differences by computing descriptive statistics
# (count, mean, median, std, min, max) for numeric fields grouped by vendor_id.
def profile_vendor_specific_differences(
    df: pd.DataFrame
) -> pd.DataFrame:
    
    cols = ["trip_distance", "fare_amount", "tip_amount", "total_amount"]
    summary = df.groupby("vendor_id")[cols].agg(["count", "mean", "median", "std", "min", "max"])
    summary.columns = [
        f"{col}_{stat}" for col, stat in summary.columns
    ]
    return summary

# Performs ingestion of CSV files using Pandas library
# Parameters:
# slice_path: path containing CSV files to ingest
# logger: Logger instance for reporting progress and errors
# Returns:
# Ingested DataFrame, number of processed files, success flag
def pandas_ingestion(
    slice_path: str | Path, 
    logger: logging.Logger | None = None
) -> tuple[pd.DataFrame, int, bool]: 

    if logger is None:
        logger = setup_logger()

    log_info_msg(logger, "NYC Yellow trip taxi dataset")

    CURRENCY = "USD"
    DISTANCE_UNIT = "Miles"
    
    slice_path = Path(slice_path)
    files = sorted(slice_path.glob(CSV_PATTERN))

    if len(files) == 0:
        log_error_msg(logger, f"No CSV files found in folder: {slice_path}")
        return pd.DataFrame(), 0, 0, False
        
    dfs = []
    processed_files = 0
    processed_rows = 0

    for f in files:
        
        print(f"Reading {f}...")

        year, month, msg = extract_year_month_from_filename(f)

        if year is None or month is None:
            log_error_msg(logger, msg)
            return pd.DataFrame(), processed_files, processed_rows, False
            
        try:
            
            df_aux = pd.read_csv(f, low_memory = False)
            processed_rows += len(df_aux)
            df_aux["year"] = year
            df_aux["month"] = month
            df_aux["currency"] = CURRENCY
            df_aux["trip_distance_unit"] = DISTANCE_UNIT
            df_aux["suspicious"] = False
        
        except Exception as e:
            log_error_msg(logger, f"Failed to open file {f}: {type(e).__name__}: {e}")
            return pd.DataFrame(), processed_files, processed_rows, False

        dfs.append(df_aux)
        processed_files += 1

    if not dfs:
        msg = f"No valid CSV files were ingested in {slice_path}"
        log_error_msg(logger, msg)
        return pd.DataFrame(), processed_files, processed_rows, False

    try:
        
        df = pd.concat(dfs, ignore_index = True)

    except Exception as e:
        msg = f"Failed to concatenate DataFrames: {type(e).__name__}: {e}"
        log_error_msg(logger, msg)
        return pd.DataFrame(), processed_files, processed_rows, False
    
    log_info_msg(logger, "Ingestion finished successfully")
    
    return df, processed_files, processed_rows, True

# Benchmark cleaning, transformation and profiling using Pandas library. Ingestion
# is performed before the benchmarking
# It also records execution time, throughput, timestamps,
# resource usage statistics
# Parameters:
# slice_path: input slice directory
# ntest: test number
# logger: Logger instance for reporting progress and errors
# Returns:
# benchmark results including slice path, processed files, processed rows, rows after cleaning, 
# execution time, throughput, timestamps, resource statistics, a message and profiling results
def benchmark_pandas_cleaning_transformation(
    slice_path: str | Path,
    ntest: int,
    logger: logging.Logger | None = None
) -> dict[str, Any]:

    if logger is None:
        logger = setup_logger()

    timer = Timer()
    monitor = ResourceMonitor()
    benchmark_start_timestamp = Timer.get_timestamp()
    print(benchmark_start_timestamp.isoformat())
    timer.start("total")
    monitor.start()
    MSG_ERROR = "Ingestion, cleaning, transformation and profiling completed with errors, check pipeline.log"
    REMOVED_FOLDER = "removed"

    df, total_files, total_rows, ingested = pandas_ingestion(slice_path = slice_path, logger = logger)
    if not ingested:
        timer.stop("total")
        resource_stats = monitor.stop()
        benchmark_end_timestamp = Timer.get_timestamp()
        execution_time = timer.duration("total")
        throughput = total_rows / execution_time if execution_time > 0 else 0.0
        return build_return_dict_cleaning_transformation(slice_path = slice_path, operation = IN_MEMORY, ntest = ntest, processed_files = total_files, processed_rows = total_rows,
                                                         rows_after_cleaning = 0, execution_time = execution_time, throughput = throughput,
                                                         benchmark_start_timestamp = benchmark_start_timestamp.isoformat(), benchmark_end_timestamp = benchmark_end_timestamp.isoformat(),
                                                         resource_stats = resource_stats, msg = MSG_ERROR, 
                                                         summary = pd.DataFrame(), missing_values = pd.DataFrame())
    
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
    
    timer.stop("total")
    resource_stats = monitor.stop()
    benchmark_end_timestamp = Timer.get_timestamp()
    
    execution_time = timer.duration("total")
    throughput = total_rows / execution_time if execution_time > 0 else 0

    print(f"Pandas cleaned and transformed {total_files} file(s) and {total_rows} row(s) in {execution_time:.2f} seconds")
    print(f"Throughput: {throughput:.2f} rows/sec")

    return build_return_dict_cleaning_transformation(slice_path = slice_path, operation = IN_MEMORY, ntest = ntest, processed_files = total_files, processed_rows = total_rows,
                                                     rows_after_cleaning = len(df_cleaned) if not is_empty(df_cleaned) else 0, 
                                                     execution_time = execution_time, throughput = throughput,
                                                     benchmark_start_timestamp = benchmark_start_timestamp.isoformat(), benchmark_end_timestamp = benchmark_end_timestamp.isoformat(),
                                                     resource_stats = resource_stats, msg = "Ingestion, cleaning, transformation and profiling completed successfully", 
                                                     summary = summary, missing_values = missing_values)  
if __name__ == "__main__":
    # Create logger
    logger = setup_logger()
    result = benchmark_pandas_cleaning_transformation(slice_path = "../data/CSV/raw-data/1st-slice/", logger = logger, ntest = 1006)

    log_info_msg(logger, result["msg"])
    print(result["msg"])
    timestamp = result['benchmark_start_timestamp'].replace(":", "-")
    save_result_to_csv_and_json_cleaning(result, 
                                	 f"./cleaning/csv/cleaning_sm.csv",
                                	 f"./cleaning/json/cleaning_{result['test_number']}_{timestamp}.json")
