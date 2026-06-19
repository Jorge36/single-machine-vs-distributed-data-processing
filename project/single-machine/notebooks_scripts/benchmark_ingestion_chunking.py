# Import libraries
import pandas as pd
from pathlib import Path
import gc
from common import setup_logger, log_info_msg, log_error_msg, Timer, ResourceMonitor, compute_dataset_size_mib, CSV_PATTERN, CHUNKING, INGESTION, save_result_to_csv_and_json
from typing import Any
import logging


# Build benchmark return for ingestion step
def build_return_dict_ingestion(
    slice_path: str | Path,
    operation: str,
    ntest: int,
    processed_files: int,
    processed_rows: int,
    execution_time: float,
    throughput: float,
    benchmark_start_timestamp: str,
    benchmark_end_timestamp: str,
    msg: str,
    chunksize: int = 0,
    resource_stats: dict[str, Any] | None = None,
) -> dict[str, Any]:
    
    if resource_stats is None:
        resource_stats = ResourceMonitor.as_dict()

    return {
        # operations:
        "stage": INGESTION,
        # Type of operation
        "operation": operation,
        # Test Number
        "test_number": ntest,        
        # Slice path
        "slice_path": str(slice_path),
        # Performance
        "processed_files": processed_files,
        "processed_rows": processed_rows,
        "execution_time_sec": execution_time,
        "throughput_rows_sec": throughput,
        "chunksize": chunksize,
        "dataset_size_mib": compute_dataset_size_mib(slice_path),
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
        "msg": msg
    }

# Benchmark ingestion performed in chunks using Pandas library, only reading files by iterating
# through all CSV files in the given slice directory, counting rows, measuring execution time,
# throughput and other metrics.
# Formal parameters:
# slice_path: path to the directory containing CSV files for the slice
# ntest: test number
# logger: Logger instance for reporting progress and errors
# chunksize: size of the chunk used to process the data
# Returns:
# benchmark results including slice path, processed files, processed rows, execution time, throughput, timestamps, 
# resource statistics and a message
def benchmark_pandas_ingestion_read_only_chunk(
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
    MSG_ERROR = "Ingestion finished with errors, check pipeline.log"
    slice_path = Path(slice_path)
    files = sorted(slice_path.glob(CSV_PATTERN))

    if len(files) == 0:
        
        log_error_msg(logger, f"No CSV files found in folder: {slice_path}")
        timer.stop("total")
        benchmark_end_timestamp = Timer.get_timestamp()
        resource_stats = monitor.stop()
        execution_time = timer.duration("total")
        return build_return_dict_ingestion(slice_path = slice_path, operation = CHUNKING, ntest = ntest, processed_files = 0, processed_rows = 0, 
                                           execution_time = execution_time, throughput = 0.0, benchmark_start_timestamp =  benchmark_start_timestamp.isoformat(),
                                           benchmark_end_timestamp = benchmark_end_timestamp.isoformat(), resource_stats = resource_stats,
                                           msg = MSG_ERROR, chunksize = chunksize)

    processed_rows = 0
    processed_files = 0

    for f in files:
        print(f"Reading {f}...")
        try:
            chunker_iter = pd.read_csv(
                f,
                chunksize = chunksize,
                low_memory = False
            )

            for chunk in chunker_iter:
                processed_rows += len(chunk)
                del chunk

        except Exception as e:
            log_error_msg(logger, f"Failed to open file {f} as chunked CSV: {type(e).__name__}: {e}")

            timer.stop("total")
            benchmark_end_timestamp = Timer.get_timestamp()
            resource_stats = monitor.stop()
            execution_time = timer.duration("total")
            throughput = processed_rows / execution_time if execution_time > 0 else 0
            return build_return_dict_ingestion(slice_path = slice_path, operation = CHUNKING, ntest = ntest, processed_files = processed_files, processed_rows = processed_rows, 
                                               execution_time = execution_time, throughput = throughput, benchmark_start_timestamp =  benchmark_start_timestamp.isoformat(),
                                               benchmark_end_timestamp = benchmark_end_timestamp.isoformat(), resource_stats = resource_stats,
                                               msg = MSG_ERROR, chunksize = chunksize)
            
        gc.collect()    
        processed_files += 1
    
    timer.stop("total")
    benchmark_end_timestamp = Timer.get_timestamp()
    resource_stats = monitor.stop()
    
    execution_time = timer.duration("total")
    throughput = processed_rows / execution_time if execution_time > 0 else 0.0
    
    print(f"Pandas ingested {processed_files} file(s) and {processed_rows} row(s) in {execution_time:.2f} seconds")
    print(f"Throughput: {throughput:.2f} rows/sec")
    return build_return_dict_ingestion(slice_path = slice_path, operation = CHUNKING, ntest = ntest, processed_files = processed_files, processed_rows = processed_rows, 
                                       execution_time = execution_time, throughput = throughput, benchmark_start_timestamp =  benchmark_start_timestamp.isoformat(),
                                       benchmark_end_timestamp = benchmark_end_timestamp.isoformat(), resource_stats = resource_stats,
                                       msg = "Ingestion finished successfully", chunksize = chunksize)

if __name__ == "__main__":
    # Create logger
    logger = setup_logger()
    result = benchmark_pandas_ingestion_read_only_chunk(slice_path = "../data/CSV/raw-data/4th-slice/", 
                                                        ntest = 45, 
                                                        chunksize = 3_000_000,
                                                        logger = logger)
    log_info_msg(logger, result["msg"])
    print(result["msg"])
    timestamp = result['benchmark_start_timestamp'].replace(":", "-")
    save_result_to_csv_and_json(result, 
                                f"./ingestion/csv/ingestion_sm.csv",
                                f"./ingestion/json/ingestion_{result['test_number']}_{timestamp}.json")
