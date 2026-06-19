import pandas as pd
from pathlib import Path
from common import setup_logger, log_info_msg, log_error_msg, Timer, ResourceMonitor, CSV_PATTERN, CHUNKING, is_empty, save_result_to_csv_and_json_cleaning
from typing import Any
import logging
from cleaning_transformation import build_return_dict_cleaning_transformation, extract_year_month_from_filename, apply_master_schema, cleaning_transformation
import gc

# Update missing value counts using one chunk
# Parameters:
# df: input DataFrame
# missing_counts_total: total of missing counts per column
# Returns:
# update missing counts
def update_profile_missing_values(
    df: pd.DataFrame,
    missing_counts_total: pd.Series | None = None
) -> pd.Series:
    chunk_missing = df.isna().sum()

    if missing_counts_total is None:
        return chunk_missing

    return missing_counts_total.add(chunk_missing, fill_value = 0).astype("int64")

# Build final missing values profile 
# Parameters:
# missing_counts_total: total of missing counts per column
# Total number of rows across all chunks
# Returns:
# DataFrame with missing_count and missing_percent
def finalise_profile_missing_values(
    missing_counts_total: pd.Series | None,
    total_rows: int
) -> pd.DataFrame:
    if missing_counts_total is None or missing_counts_total.empty:
        return pd.DataFrame(columns=["missing_count", "missing_percent"])

    denominator = total_rows if total_rows > 0 else 1

    result = pd.DataFrame({
         "missing_count": missing_counts_total,
         "missing_percent": ((missing_counts_total / denominator) * 100).round(2)
    })

    return result.sort_values(by = "missing_count", ascending = False)

# Benchmark chunked ingestion, cleaning, transformation and
# profiling using Pandas library. It also records execution time, throughput, timestamps,
# resource usage statistics
# Parameters:
# slice_path: input slice directory
# logger: Logger instance for reporting progress and errors
# ntest: test number
# Returns:
# benchmark results including slice path, processed files, processed rows, rows after cleaning, execution time, 
# throughput, timestamps, resource statistics, a message and profiling results
def benchmark_pandas_cleaning_transformation_chunk(
    slice_path: str | Path,
    ntest: int,
    chunksize: int = 100_000, 
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

    log_info_msg(logger, "NYC Yellow trip taxi dataset")
    MSG_ERROR = "Ingestion, cleaning, transformation and profiling completed with errors, check pipeline.log"
    REMOVED_FOLDER = "removed"
    CURRENCY = "USD"
    DISTANCE_UNIT = "Miles"
    processed_rows = 0
    
    slice_path = Path(slice_path)
    files = sorted(slice_path.glob(CSV_PATTERN))

    if len(files) == 0:
        log_error_msg(logger, f"No CSV files found in folder: {slice_path}")
        timer.stop("total")
        resource_stats = monitor.stop()
        benchmark_end_timestamp = Timer.get_timestamp()
        execution_time = timer.duration("total")
        throughput = processed_rows / execution_time if execution_time > 0 else 0.0
        return build_return_dict_cleaning_transformation(slice_path = slice_path, operation = CHUNKING, ntest = ntest, processed_files = 0, processed_rows = processed_rows,
                                                         rows_after_cleaning = 0, execution_time = execution_time, throughput = throughput,
                                                         benchmark_start_timestamp = benchmark_start_timestamp.isoformat(), benchmark_end_timestamp = benchmark_end_timestamp.isoformat(),
                                                         resource_stats = resource_stats, msg = MSG_ERROR, summary = pd.DataFrame(),
                                                         chunksize = chunksize, missing_values = pd.DataFrame())    
    processed_files = 0
    rows_after_cleaning = 0
    first_chunk_header = True
    missing_counts_total = None
    missing_values = pd.DataFrame()

    for f in files:
        print(f"Reading {f}...")

        year, month, msg = extract_year_month_from_filename(f)

        if year is None or month is None:
            log_error_msg(logger, msg)
            timer.stop("total")
            resource_stats = monitor.stop()
            benchmark_end_timestamp = Timer.get_timestamp()
            execution_time = timer.duration("total")
            throughput = processed_rows / execution_time if execution_time > 0 else 0.0

            return build_return_dict_cleaning_transformation(slice_path = slice_path, operation = CHUNKING, ntest = ntest, processed_files = processed_files, processed_rows = processed_rows,
                                                             rows_after_cleaning = 0, execution_time = execution_time, throughput = throughput,
                                                             benchmark_start_timestamp = benchmark_start_timestamp.isoformat(), benchmark_end_timestamp = benchmark_end_timestamp.isoformat(),
                                                             resource_stats = resource_stats, msg = MSG_ERROR, summary = pd.DataFrame(), 
                                                             chunksize = chunksize, missing_values = pd.DataFrame())    
        
        try: 
            chunk_iter = pd.read_csv(
                f,
                chunksize = chunksize,
                dtype = {"store_and_fwd_flag": "string"},
                low_memory = False
            )
        except Exception as e:
            log_error_msg(logger, f"Failed to open file {f} as chunked CSV: {type(e).__name__}: {e}")
            timer.stop("total")
            resource_stats = monitor.stop()
            benchmark_end_timestamp = Timer.get_timestamp()
            execution_time = timer.duration("total")
            throughput = processed_rows / execution_time if execution_time > 0 else 0.0
            return build_return_dict_cleaning_transformation(slice_path = slice_path, operation = CHUNKING, ntest = ntest, processed_files = processed_files, processed_rows = processed_rows,
                                                             rows_after_cleaning = rows_after_cleaning, execution_time = execution_time, throughput = throughput,
                                                             benchmark_start_timestamp = benchmark_start_timestamp.isoformat(), benchmark_end_timestamp = benchmark_end_timestamp.isoformat(),
                                                             resource_stats = resource_stats, msg = MSG_ERROR, summary = pd.DataFrame(), 
                                                             chunksize = chunksize, missing_values = pd.DataFrame())    
            
        path = Path(f)
        removed_slice_path = path.parents[2] / REMOVED_FOLDER / path.parent.name
        removed_slice_path.mkdir(parents = True, exist_ok = True)
        for chunk in chunk_iter:
            chunk["year"] = year
            chunk["month"] = month
            chunk["currency"] = CURRENCY
            chunk["trip_distance_unit"] = DISTANCE_UNIT
            chunk["suspicious"] = False
            df_normalised = pd.DataFrame()
            if not is_empty(chunk): 
                # Apply new schema
                df_normalised = apply_master_schema(chunk, logger)
            df_cleaned = pd.DataFrame()
            if not is_empty(df_normalised):
                # Clean dataset and transform it
                df_cleaned = cleaning_transformation(df = df_normalised, removed_slice_path = removed_slice_path, logger = logger, process_type = CHUNKING, header = first_chunk_header)
                first_chunk_header = False
            rows_after_cleaning += len(df_cleaned)
            # Profiling
            if not is_empty(df_cleaned):
                missing_counts_total = update_profile_missing_values(df_cleaned, missing_counts_total)
            processed_rows += len(chunk)

            # Cleanup
            del chunk
            del df_normalised
            del df_cleaned
            
        processed_files += 1
        del chunk_iter
        gc.collect()

    missing_values = finalise_profile_missing_values(missing_counts_total, rows_after_cleaning)
    
    timer.stop("total")
    resource_stats = monitor.stop()
    benchmark_end_timestamp = Timer.get_timestamp()
    
    execution_time = timer.duration("total")
    throughput = processed_rows / execution_time if execution_time > 0 else 0
    
    print(f"Pandas cleaned and transformed {processed_files} file(s) and {processed_rows} row(s) in {execution_time:.2f} seconds")
    print(f"Throughput: {throughput:.2f} rows/sec")

    return build_return_dict_cleaning_transformation(slice_path = slice_path, operation = CHUNKING, ntest = ntest, processed_files = processed_files, processed_rows = processed_rows,
                                                     rows_after_cleaning = rows_after_cleaning, execution_time = execution_time, throughput = throughput,
                                                     benchmark_start_timestamp = benchmark_start_timestamp.isoformat(), benchmark_end_timestamp = benchmark_end_timestamp.isoformat(),
                                                     resource_stats = resource_stats, msg = "Ingestion, cleaning, transformation and profiling completed successfully. Vendor summary is not computed in chunk mode.", 
                                                     summary = pd.DataFrame(), 
                                                     chunksize = chunksize, missing_values = missing_values)    

if __name__ == "__main__":
    # Create logger
    logger = setup_logger()
    result = benchmark_pandas_cleaning_transformation_chunk(slice_path = "../data/CSV/raw-data/3rd-slice/", 
                                                            logger = logger, 
                                                            chunksize = 100_000, 
                                                            ntest = 28)

    log_info_msg(logger, result["msg"])
    print(result["msg"])
    timestamp = result['benchmark_start_timestamp'].replace(":", "-")
    save_result_to_csv_and_json_cleaning(result, 
                                	 f"./cleaning/csv/cleaning_sm.csv",
                                	 f"./cleaning/json/cleaning_{result['test_number']}_{timestamp}.json")
