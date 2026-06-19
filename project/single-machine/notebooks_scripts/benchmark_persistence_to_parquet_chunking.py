import pandas as pd
import duckdb
from persistence_to_parquet import persist_partitioned_parquet_duckdb, build_return_dict_parquet_persistence
import logging
from pathlib import Path
import gc
from common import setup_logger, log_info_msg, log_error_msg, Timer, ResourceMonitor, CSV_PATTERN, CHUNKING, is_empty, save_result_to_csv_and_json_cleaning
from typing import Any
from benchmark_cleaning_transf_profiling_chunking import update_profile_missing_values, finalise_profile_missing_values
from cleaning_transformation import extract_year_month_from_filename, apply_master_schema, cleaning_transformation

# Benchmark chunked ingestion, cleaning, transformation,
# profiling and parquet persistence using Pandas library and DuckDB. It also records execution time, throughput, timestamps,
# resource usage statistics
# Parameters:
# slice_path: input slice directory containing CSV files
# parquet_path: output directory for partitioned parquet file
# chunksize: number of rows per chunk
# logger: Logger instance for reporting progress and errors
# threads: number of DuckDB execution threads
# ntest: test number
# verify_count_rows: if to verify row counts after compaction
# Returns:
# benchmark results including slice path, processed files, processed rows, rows after cleaning, execution time, 
# throughput, threads, timestamps, resource statistics, a message and profiling results
def benchmark_duckdb_parquet_persistence_chunk(
    slice_path: str | Path,
    ntest: int,
    parquet_path: str | Path, 
    chunksize: int = 100_000, 
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

    log_info_msg(logger, "NYC Yellow trip taxi dataset")
    MSG_ERROR = "Ingestion, cleaning, transformation, profiling and persistence to parquet completed with errors, check pipeline.log"
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
        return build_return_dict_parquet_persistence(slice_path = slice_path, parquet_path = parquet_path, operation = CHUNKING, ntest = ntest, processed_files = 0, processed_rows = processed_rows, 
                                                     rows_after_cleaning = 0, execution_time = execution_time, throughput = throughput,
                                                     threads = threads, benchmark_start_timestamp = benchmark_start_timestamp.isoformat(),
                                                     benchmark_end_timestamp = benchmark_end_timestamp.isoformat(), 
                                                     resource_stats = resource_stats, chunksize = chunksize, created_parquet_files = 0,
                                                     msg = MSG_ERROR, summary = pd.DataFrame(), missing_values = pd.DataFrame(),
                                                     parquet_files_size_mib = 0.0)

    connection = None
    try: 
        connection = duckdb.connect()
    except duckdb.Error as e:
        msg = f"Failed to initialize DuckDB connection (threads = {threads}): {type(e).__name__}: {e}"
        log_error_msg(logger, msg)
        timer.stop("total")
        resource_stats = monitor.stop()
        benchmark_end_timestamp = Timer.get_timestamp()
        execution_time = timer.duration("total")
        throughput = processed_rows / execution_time if execution_time > 0 else 0.0
        return build_return_dict_parquet_persistence(slice_path = slice_path, parquet_path = parquet_path, operation = CHUNKING, ntest = ntest, processed_files = 0, processed_rows = processed_rows, 
                                                     rows_after_cleaning = 0, execution_time = execution_time, throughput = throughput,
                                                     threads = threads, benchmark_start_timestamp = benchmark_start_timestamp.isoformat(),
                                                     benchmark_end_timestamp = benchmark_end_timestamp.isoformat(), 
                                                     resource_stats = resource_stats, chunksize = chunksize, created_parquet_files = 0,
                                                     msg = MSG_ERROR, summary = pd.DataFrame(), missing_values = pd.DataFrame(),
                                                     parquet_files_size_mib = 0.0)
        
    try: 
        processed_files = 0
        created_parquet_files = 0
        parquet_files_size_mib = 0.0
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
                return build_return_dict_parquet_persistence(slice_path = slice_path, parquet_path = parquet_path, operation = CHUNKING, ntest = ntest, processed_files = processed_files, processed_rows = processed_rows, 
                                                             rows_after_cleaning = rows_after_cleaning, execution_time = execution_time, throughput = throughput,
                                                             threads = threads, benchmark_start_timestamp = benchmark_start_timestamp.isoformat(),
                                                             benchmark_end_timestamp = benchmark_end_timestamp.isoformat(), 
                                                             resource_stats = resource_stats, chunksize = chunksize, created_parquet_files = created_parquet_files,
                                                             msg = MSG_ERROR, summary = pd.DataFrame(), parquet_files_size_mib = parquet_files_size_mib,
                                                             missing_values = finalise_profile_missing_values(missing_counts_total, rows_after_cleaning))
                
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
                return build_return_dict_parquet_persistence(slice_path = slice_path, parquet_path = parquet_path, operation = CHUNKING, ntest = ntest, processed_files = processed_files, processed_rows = processed_rows, 
                                                             rows_after_cleaning = rows_after_cleaning, execution_time = execution_time, throughput = throughput,
                                                             threads = threads, benchmark_start_timestamp = benchmark_start_timestamp.isoformat(),
                                                             benchmark_end_timestamp = benchmark_end_timestamp.isoformat(), 
                                                             resource_stats = resource_stats, chunksize = chunksize, 
                                                             created_parquet_files = created_parquet_files, parquet_files_size_mib = parquet_files_size_mib,
                                                             msg = MSG_ERROR, summary = pd.DataFrame(), missing_values = finalise_profile_missing_values(missing_counts_total, rows_after_cleaning))
                            
    
            path = Path(f)
            removed_slice_path = path.parents[2] / REMOVED_FOLDER / path.parent.name
            removed_slice_path.mkdir(parents = True, exist_ok = True)        
            for chunk_id, chunk in enumerate(chunk_iter, start = 1):
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
                    persisted, created_parquet_files_aux, parquet_files_size_mib_aux = persist_partitioned_parquet_duckdb(df = df_cleaned, threads = threads, 
                                                                                                                          parquet_path = Path(parquet_path), 
                                                                                                                          logger = logger,
                                                                                                                          verify_count_rows = verify_count_rows,
                                                                                                                          connection = connection)
                    created_parquet_files += created_parquet_files_aux
                    parquet_files_size_mib += parquet_files_size_mib_aux
                    if not persisted: 
                        log_error_msg(logger, "Persistence to parquet failed")
                        timer.stop("total")
                        resource_stats = monitor.stop()
                        benchmark_end_timestamp = Timer.get_timestamp()
                        execution_time = timer.duration("total")
                        throughput = processed_rows / execution_time if execution_time > 0 else 0.0
                        return build_return_dict_parquet_persistence(slice_path = slice_path, parquet_path = parquet_path, operation = CHUNKING, ntest = ntest, processed_files = processed_files, processed_rows = processed_rows, 
                                                                     rows_after_cleaning = rows_after_cleaning, execution_time = execution_time, throughput = throughput,
                                                                     threads = threads, benchmark_start_timestamp = benchmark_start_timestamp.isoformat(),
                                                                     benchmark_end_timestamp = benchmark_end_timestamp.isoformat(), 
                                                                     resource_stats = resource_stats, chunksize = chunksize, 
                                                                     created_parquet_files = created_parquet_files, parquet_files_size_mib = parquet_files_size_mib,
                                                                     msg = MSG_ERROR, summary = pd.DataFrame(), missing_values = finalise_profile_missing_values(missing_counts_total, rows_after_cleaning))
                                    
                else:
                    logger.info(f"Chunk {chunk_id} of file {f} produced no cleaned rows to persist")
                processed_rows += len(chunk)
    
                # Cleanup
                del chunk
                del df_normalised
                del df_cleaned
                    
            processed_files += 1
            del chunk_iter
            gc.collect()
        
        missing_values = finalise_profile_missing_values(missing_counts_total, rows_after_cleaning)

    except Exception as e:
        log_error_msg(logger, f"Persistence to parquet completed with errors: {type(e).__name__}: {e}")
        timer.stop("total")
        resource_stats = monitor.stop()
        benchmark_end_timestamp = Timer.get_timestamp()
        execution_time = timer.duration("total")
        throughput = processed_rows / execution_time if execution_time > 0 else 0.0
        return build_return_dict_parquet_persistence(slice_path = slice_path, parquet_path = parquet_path, operation = CHUNKING, ntest = ntest, processed_files = processed_files, processed_rows = processed_rows, 
                                                     rows_after_cleaning = rows_after_cleaning, execution_time = execution_time, throughput = throughput,
                                                     threads = threads, benchmark_start_timestamp = benchmark_start_timestamp.isoformat(),
                                                     benchmark_end_timestamp = benchmark_end_timestamp.isoformat(), 
                                                     resource_stats = resource_stats, chunksize = chunksize, 
                                                     created_parquet_files = created_parquet_files, parquet_files_size_mib = parquet_files_size_mib,
                                                     msg = MSG_ERROR, summary = pd.DataFrame(), missing_values = finalise_profile_missing_values(missing_counts_total, rows_after_cleaning))
                        
    finally:
        if connection is not None:
            connection.close()

    timer.stop("total")
    resource_stats = monitor.stop()
    benchmark_end_timestamp = Timer.get_timestamp()

    execution_time = timer.duration("total")
    throughput = processed_rows / execution_time if execution_time > 0 else 0
    
    print(f"DuckDB persisted {processed_files} file(s) and {processed_rows} row(s) to Parquet in {execution_time:.2f} seconds")
    print(f"Throughput: {throughput:.2f} rows/sec")

    return build_return_dict_parquet_persistence(slice_path = slice_path, parquet_path = parquet_path, operation = CHUNKING, ntest = ntest, processed_files = processed_files, processed_rows = processed_rows, 
                                                 rows_after_cleaning = rows_after_cleaning, execution_time = execution_time, throughput = throughput,
                                                 threads = threads, benchmark_start_timestamp = benchmark_start_timestamp.isoformat(),
                                                 benchmark_end_timestamp = benchmark_end_timestamp.isoformat(), 
                                                 resource_stats = resource_stats, chunksize = chunksize, 
                                                 created_parquet_files = created_parquet_files, parquet_files_size_mib = parquet_files_size_mib,
                                                 msg = "Ingestion, cleaning, transformation, profiling and persistence to parquet completed successfully", summary = pd.DataFrame(), missing_values = missing_values)

if __name__ == "__main__":
    logger = setup_logger()
    result = benchmark_duckdb_parquet_persistence_chunk(slice_path = "../data/CSV/raw-data/4th-slice/", 
                                                        parquet_path= "../data/parquet/4th-slice/", 
                                                        chunksize = 100_000,
                                                        logger = logger, 
                                                        ntest = 21, 
                                                        verify_count_rows = True, 
                                                        threads = 3)
    log_info_msg(logger, result["msg"])
    print(result["msg"])
    timestamp = result['benchmark_start_timestamp'].replace(":", "-")
    save_result_to_csv_and_json_cleaning(result, 
                                	 f"./persistence/csv/persistence_sm.csv",
                                	 f"./persistence/json/persistence_{result['test_number']}_{timestamp}.json")
