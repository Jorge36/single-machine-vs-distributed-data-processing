from pathlib import Path
import duckdb
import logging
from common import count_parquet_files, compute_dataset_size_mib, setup_logger, log_info_msg, log_error_msg, Timer, ResourceMonitor, STANDARD, save_result_to_csv_and_json, QUERIES, PARQUET_FOLDERS, PARQUET_PATHS
from typing import Any

DUCKDB_QUERIES = {
    
    "q1_monthly_payment_statistics": """
            SELECT year, month, payment_type_id, COUNT(*) AS trip_count, AVG(trip_distance) AS avg_trip_distance,
                   trip_distance_unit AS trip_distance_unit, AVG(fare_amount) AS avg_fare_amount, AVG(tip_amount) AS avg_tip_amount, 
                   SUM(total_amount) AS sum_total_amount, currency AS currency
             FROM read_parquet('{parquet_path}')
            WHERE suspicious = FALSE
         GROUP BY year, month, payment_type_id, trip_distance_unit, currency
         ORDER BY year, month, payment_type_id
    """,

    "q2_most_valuable_routes": """
            SELECT pickup_location_id, dropoff_location_id, COUNT(*) AS trip_count, AVG(trip_distance) AS avg_trip_distance,
                   trip_distance_unit AS trip_distance_unit, AVG(total_amount) AS avg_total_amount, SUM(total_amount) AS sum_total_amount, 
                   currency AS currency
              FROM read_parquet('{parquet_path}')
          GROUP BY pickup_location_id, dropoff_location_id, trip_distance_unit, currency
            HAVING COUNT(*) > 100
          ORDER BY sum_total_amount DESC, trip_count DESC
             LIMIT 100
    """,

    "q3_hourly_demand_and_speed": """
            WITH BASE AS (
                SELECT EXTRACT(HOUR FROM pickup_datetime) AS pickup_hour, trip_distance, trip_distance_unit, fare_amount, total_amount,
                       currency,
                       EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime)) / 60.0 AS trip_duration_min
                  FROM read_parquet('{parquet_path}')
                 WHERE trip_distance > 0
            ) 
            SELECT pickup_hour, COUNT(*) AS trip_count, AVG(trip_distance) AS avg_trip_distance, trip_distance_unit AS trip_distance_unit,
                   AVG(fare_amount) AS avg_fare_amount, AVG(total_amount) AS avg_total_amount, currency AS currency,
                   AVG(trip_distance / NULLIF(trip_duration_min / 60.0, 0)) AS avg_speed_mph
              FROM base
          GROUP BY pickup_hour, trip_distance_unit, currency
          ORDER BY pickup_hour, trip_distance_unit, currency
    """
}

FULL_DATASET_SIZE_QUERY = "SELECT COUNT(*) FROM read_parquet('{parquet_path}')"

# Get the total number of rows in a parquet lake using DuckDB
def get_total_size_dataset(
    parquet_path: str | Path, 
    threads: int = 0, 
    logger: logging.Logger | None = None,
) -> tuple[int, bool]:

    if logger is None:
        logger = setup_logger()
    
    if "{parquet_path}" not in FULL_DATASET_SIZE_QUERY:
        msg = f"Query to get full dataset size does not contain '{{parquet_path}}': '{parquet_path}'"
        log_error_msg(logger, msg)
        return 0, False

    try:
        query_full_dataset_size = FULL_DATASET_SIZE_QUERY.format(parquet_path = str(parquet_path))
    except Exception as e:
        msg = f"Error formatting query to get full dataset size: {type(e).__name__}: {e}"
        log_error_msg(logger, msg)
        return 0, False
        
    connection = None
    try:
        connection = duckdb.connect()
        if threads > 0:
            connection.execute(f"PRAGMA threads={threads}")
    except Exception as e:
        msg = f"Failed to initialize DuckDB connection (threads = {threads}): {type(e).__name__}: {e}"
        log_error_msg(logger, msg)
        if connection is not None:
            connection.close()
        return 0, False
                            
    try:
        total_rows = connection.execute(query_full_dataset_size).fetchone()[0]
    except Exception as e:
        msg = f"Query to get full dataset size could not be executed due to the following error: {type(e).__name__}: {e}"
        log_error_msg(logger, msg)
        return 0, False
    finally:
        connection.close()

    return total_rows, True

# Build benchmark return for queries step
def build_return_dict_queries(
    slice_name: str,
    parquet_path: str | Path,
    operation: str,
    ntest: int,
    query_name: str,
    total_files: int,
    total_rows_in_dataset: int,
    rows_returned: int,
    execution_time: float,
    throughput: float,
    threads: int,
    benchmark_start_timestamp: str,
    benchmark_end_timestamp: str,
    msg: str,
    resource_stats: dict[str, Any] | None = None,
) -> dict[str, Any]:

    if resource_stats is None:
        resource_stats = ResourceMonitor.as_dict()

    return {
            # operations:
            "stage": QUERIES,
            # Type of operation
            "operation": operation,
            # Test Number
            "test_number": ntest,  
            #Slice name
            "slice_name": slice_name,
            # Slice path
            "parquet_path": str(parquet_path),
            # Query name
            "query_name": query_name,
            # Performance
            "total_files": total_files,
            "total_rows_in_dataset": total_rows_in_dataset,
            "rows_returned": rows_returned,
            "execution_time_sec": execution_time,
            "throughput_rows_sec": throughput,
            "threads": threads,
            "parquet_files_size_mib": compute_dataset_size_mib(parquet_path),
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
            "msg":  msg
    }

# Benchmark DuckDB queries against a parquet dataset
# Parameters:
# slice_name: key used to resolve the parquet dataset path from PARQUET_PATHS
# query_name: key used to resolve the query template from DUCKDB_QUERIES
# ntest: test number
# threads: number of DuckDB execution threads
# logger: Logger instance for reporting progress and errors
# Returns:
# benchmark results including slice name, parquet path, query name, total files, total rows in dataset, execution time, 
# throughput, threads, timestamps, resource statistics and a message
def benchmark_duckdb_queries(
    slice_name: str, 
    query_name: str, 
    ntest: int,
    threads: int = 0, 
    logger: logging.Logger | None = None,
) -> dict[str, Any]:

    if logger is None:
        logger = setup_logger()
        
    timer = Timer()
    benchmark_start_timestamp = Timer.get_timestamp()
    print(benchmark_start_timestamp.isoformat())

    MSG_ERROR = "Query processed with errors, check pipeline.log"
    parquet_path = PARQUET_PATHS[slice_name]
    query_template = DUCKDB_QUERIES[query_name]
    parquet_folder = PARQUET_FOLDERS[slice_name]

    total_files = count_parquet_files(parquet_path = parquet_path, use_pattern = True)
    
    if total_files == 0:
        msg = f"No parquet file(s) were found for that path: {parquet_folder} OR {parquet_folder} does not exist"
        log_error_msg(logger, msg)
        benchmark_end_timestamp = Timer.get_timestamp()
        return build_return_dict_queries(slice_name = slice_name, parquet_path = parquet_folder, query_name = query_name, 
                                         operation = STANDARD, ntest = ntest, total_files = total_files, total_rows_in_dataset = 0,
                                         rows_returned = 0, execution_time = 0.0, throughput = 0.0, threads = threads, 
                                         benchmark_start_timestamp = benchmark_start_timestamp.isoformat(), 
                                         benchmark_end_timestamp = benchmark_end_timestamp.isoformat(),
                                         msg = MSG_ERROR)
    
    if "{parquet_path}" not in query_template:
        msg = f"Query does not contain '{{parquet_path}}': '{parquet_path}'"
        log_error_msg(logger, msg)
        benchmark_end_timestamp = Timer.get_timestamp()
        return build_return_dict_queries(slice_name = slice_name, parquet_path = parquet_folder, query_name = query_name, 
                                         operation = STANDARD, ntest = ntest, total_files = total_files, total_rows_in_dataset = 0,
                                         rows_returned = 0, execution_time = 0.0, throughput = 0.0, threads = threads, 
                                         benchmark_start_timestamp = benchmark_start_timestamp.isoformat(), 
                                         benchmark_end_timestamp = benchmark_end_timestamp.isoformat(),
                                         msg = MSG_ERROR)
                
    try:
        query = query_template.format(parquet_path = parquet_path)
        print(f"Query: \n {query}")
    except Exception as e:
        msg = f"Error formatting query {query_name}: {e}"
        log_error_msg(logger, msg)
        benchmark_end_timestamp = Timer.get_timestamp()
        return build_return_dict_queries(slice_name = slice_name, parquet_path = parquet_folder, query_name = query_name, 
                                         operation = STANDARD, ntest = ntest, total_files = total_files, total_rows_in_dataset = 0,
                                         rows_returned = 0, execution_time = 0.0, throughput = 0.0, threads = threads, 
                                         benchmark_start_timestamp = benchmark_start_timestamp.isoformat(), 
                                         benchmark_end_timestamp = benchmark_end_timestamp.isoformat(),
                                         msg = MSG_ERROR)

    total_rows = 0
    # Start benchmark
    monitor = ResourceMonitor()
    timer.start("total")
    monitor.start()

    connection = None
    
    try:
        connection = duckdb.connect()
        if threads > 0:
            connection.execute(f"PRAGMA threads={threads}")
    except Exception as e:
        msg = f"Failed to initialize DuckDB connection (threads = {threads}): {type(e).__name__}: {e}"
        log_error_msg(logger, msg)
        if connection is not None:
            connection.close()
        timer.stop("total")
        resource_stats = monitor.stop()
        benchmark_end_timestamp = Timer.get_timestamp()
        execution_time = timer.duration("total")
        throughput = total_rows / execution_time if execution_time > 0 else 0.0
        return build_return_dict_queries(slice_name = slice_name, parquet_path = parquet_folder, query_name = query_name, 
                                         operation = STANDARD, ntest = ntest, total_files = total_files, total_rows_in_dataset = total_rows,
                                         rows_returned = 0, execution_time = execution_time, throughput = throughput, threads = threads, 
                                         benchmark_start_timestamp = benchmark_start_timestamp.isoformat(), 
                                         benchmark_end_timestamp = benchmark_end_timestamp.isoformat(),
                                         msg = MSG_ERROR, resource_stats = resource_stats)
    try:
        rows_returned = connection.execute(f"SELECT COUNT(*) FROM ({query}) t").fetchone()[0]
    except Exception as e:
        msg = f"Query could not be executed due to the following error: {type(e).__name__}: {e}"
        log_error_msg(logger, msg)
        timer.stop("total")
        resource_stats = monitor.stop()
        benchmark_end_timestamp = Timer.get_timestamp()
        execution_time = timer.duration("total")
        throughput = total_rows / execution_time if execution_time > 0 else 0.0
        return build_return_dict_queries(slice_name = slice_name, parquet_path = parquet_folder, query_name = query_name, 
                                         operation = STANDARD, ntest = ntest, total_files = total_files, total_rows_in_dataset = total_rows,
                                         rows_returned = 0, execution_time = execution_time, throughput = throughput, threads = threads, 
                                         benchmark_start_timestamp = benchmark_start_timestamp.isoformat(), 
                                         benchmark_end_timestamp = benchmark_end_timestamp.isoformat(),
                                         msg = MSG_ERROR, resource_stats = resource_stats)                                               
    finally:
        if connection is not None:
            connection.close()

        
    timer.stop("total")
    resource_stats = monitor.stop()
    benchmark_end_timestamp = Timer.get_timestamp()
    execution_time = timer.duration("total")
    
    total_rows, query_succeded = get_total_size_dataset(parquet_path = parquet_path, logger = logger)

    if not query_succeded:
        return build_return_dict_queries(slice_name = slice_name, parquet_path = parquet_folder, query_name = query_name, 
                                         operation = STANDARD, ntest = ntest, total_files = total_files, total_rows_in_dataset = total_rows,
                                         rows_returned = rows_returned, execution_time = execution_time, throughput = 0, threads = threads, 
                                         benchmark_start_timestamp = benchmark_start_timestamp.isoformat(), 
                                         benchmark_end_timestamp = benchmark_end_timestamp.isoformat(),
                                         msg = MSG_ERROR)
    
    throughput = total_rows / execution_time if execution_time > 0 else 0.0
    print(f"DuckDB executed the query {query_name} in {execution_time:.2f} seconds. Processed {total_rows} row(s) and returned {rows_returned} row(s)")
    print(f"Throughput: {throughput:.2f} rows/sec")

    return build_return_dict_queries(slice_name = slice_name, parquet_path = parquet_folder, query_name = query_name, 
                                     operation = STANDARD, ntest = ntest, total_files = total_files, total_rows_in_dataset = total_rows,
                                     rows_returned = rows_returned, execution_time = execution_time, throughput = throughput, threads = threads, 
                                     benchmark_start_timestamp = benchmark_start_timestamp.isoformat(), 
                                     benchmark_end_timestamp = benchmark_end_timestamp.isoformat(),
                                     msg = "Query was processed successfully", resource_stats = resource_stats)
if __name__ == "__main__":
    # Create logger
    logger = setup_logger()
    result = benchmark_duckdb_queries(slice_name = "4th-slice", 
                                    query_name = "q2_most_valuable_routes", 
                                    logger = logger, 
                                    ntest = 49, 
                                    threads = 3)

    log_info_msg(logger, result["msg"])
    print(result["msg"])
    timestamp = result['benchmark_start_timestamp'].replace(":", "-")
    save_result_to_csv_and_json(result, 
                                f"./queries/csv/queries_sm.csv",
                                f"./queries/json/queries_{result['test_number']}_{timestamp}.json")
