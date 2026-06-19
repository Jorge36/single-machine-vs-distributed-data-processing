from datetime import datetime
from uuid import uuid4
from common import count_parquet_files, compute_dataset_size_mib, setup_logger, log_info_msg, log_error_msg, Timer, ResourceMonitor, STANDARD, save_result_to_csv_and_json, COMPACTION_TO_PARQUET_FILE
from typing import Any
from pathlib import Path
import duckdb
import logging

# Build benchmark return for compaction to one parquet file per partition step
def build_return_dict_compaction_parquet_lake(
    parquet_path: str | Path,
    compacted_parquet_path: str | Path,
    operation: str,
    ntest: int,
    created_parquet_files: int,
    total_partitions: int,
    compacted_parquet_size: float,
    processed_files: int,
    processed_rows: int,
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
        "stage": COMPACTION_TO_PARQUET_FILE,
        # Type of operation
        "operation": operation,
        # Test Number
        "test_number": ntest,  
        # Parquet paths
        "parquet_path": str(parquet_path),
        "compacted_parquet_path": str(compacted_parquet_path),
        # Performance
        "processed_files": processed_files,
        "processed_rows": processed_rows,
        "execution_time_sec": execution_time,
        "throughput_rows_sec": throughput,
        "threads": threads,
        "parquet_size_mib": compute_dataset_size_mib(parquet_path),
        "compacted_parquet_size_mib": compacted_parquet_size,
        "compacted_parquet_files_created": created_parquet_files,
        "total_partitions_created": total_partitions,
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

# Benchamrk compaction of partitioned parquet lake using duckdb
# Parameters:
# parquet_path: directory partitioned parquet lake
# compacted_parquet_path: output path for compacted parquet lake
# ntest: test number
# threads: number of DuckDB execution threads
# logger: Logger instance for reporting progress and errors
# verify_count_rows: if to verify row counts after compaction
# Returns:
# benchmark results including parquet path, compacted parquet path, processed files, processed rows, execution time, 
# throughput, threads, timestamps, resource statistics and a message
def benchmark_compaction_parquet_lake(
    parquet_path: str | Path, 
    compacted_parquet_path: str | Path, 
    ntest: int,
    threads: int = 0, 
    logger: logging.Logger | None = None,
    verify_count_rows: bool = False
) -> dict[str, Any]:

    if logger is None:
        logger = setup_logger()
                
    timer = Timer()
    monitor = ResourceMonitor()
    benchmark_start_timestamp = Timer.get_timestamp()
    print(benchmark_start_timestamp.isoformat())
    timer.start("total")
    monitor.start()
    
    parquet_path = Path(parquet_path)
    compacted_parquet_path = Path(compacted_parquet_path)
    created_parquet_files = 0
    total_rows = 0
    total_files = 0
    total_partitions = 0
    compacted_parquet_size = 0
    MSG_ERROR = "Compaction step completed with errors, check pipeline.log"
    MIB = 1024 * 1024

    if not parquet_path.exists():
        msg = f"Source parquet path does not exist: {parquet_path}"
        log_error_msg(logger, msg)
        timer.stop("total")
        resource_stats = monitor.stop()
        benchmark_end_timestamp = Timer.get_timestamp()
        execution_time = timer.duration("total")
        throughput = total_rows / execution_time if execution_time > 0 else 0.0
        return build_return_dict_compaction_parquet_lake(parquet_path = parquet_path, compacted_parquet_path = compacted_parquet_path,
                                                         operation = STANDARD, ntest = ntest, processed_files = total_files, processed_rows = total_rows,
                                                         execution_time = execution_time, throughput = throughput, threads = threads,
                                                         benchmark_start_timestamp = benchmark_start_timestamp.isoformat(),
                                                         benchmark_end_timestamp =  benchmark_end_timestamp.isoformat(),
                                                         resource_stats = resource_stats, created_parquet_files = created_parquet_files,
                                                         msg = MSG_ERROR, total_partitions = total_partitions,
                                                         compacted_parquet_size = compacted_parquet_size)
    connection = None

    try:
        connection = duckdb.connect()
        if threads > 0:
            connection.execute(f"PRAGMA threads={threads}")
    except duckdb.Error as e:
        msg = f"Failed to initialize DuckDB connection (threads = {threads}): {type(e).__name__}: {e}"
        log_error_msg(logger, msg)
        if connection is not None:
            connection.close()
            
        timer.stop("total")
        resource_stats = monitor.stop()
        benchmark_end_timestamp = Timer.get_timestamp()
        execution_time = timer.duration("total")
        throughput = total_rows / execution_time if execution_time > 0 else 0.0
        return build_return_dict_compaction_parquet_lake(parquet_path = parquet_path, compacted_parquet_path = compacted_parquet_path,
                                                         operation = STANDARD, ntest = ntest, processed_files = total_files, processed_rows = total_rows,
                                                         execution_time = execution_time, throughput = throughput, threads = threads,
                                                         benchmark_start_timestamp = benchmark_start_timestamp.isoformat(),
                                                         benchmark_end_timestamp =  benchmark_end_timestamp.isoformat(),
                                                         resource_stats = resource_stats, created_parquet_files = created_parquet_files,
                                                         msg = MSG_ERROR, total_partitions = total_partitions,
                                                         compacted_parquet_size = compacted_parquet_size)
            
    try: 

        for year_dir in parquet_path.glob("year=*"):
            for month_dir in year_dir.glob("month=*"):
    
                rel_path = month_dir.relative_to(parquet_path)
                target_dir = compacted_parquet_path / rel_path
                
                existing_files = set(target_dir.rglob("*.parquet"))
                
                target_dir.mkdir(parents = True, exist_ok = True)

                run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

                target_file = target_dir / f"compacted_data_{run_id}_{uuid4().hex}.parquet"
    
                pattern_read = (month_dir / "*.parquet").as_posix()

                total_files += count_parquet_files(parquet_path = pattern_read, use_pattern = True)
        
                nrows = connection.execute(f"SELECT COUNT(*) FROM read_parquet('{pattern_read}')").fetchone()[0]
                
                query = f"""
                COPY (
                    SELECT *
                    FROM read_parquet('{pattern_read}')
                ) 
                TO '{target_file.as_posix()}'
                (FORMAT PARQUET);
                """
            
                connection.execute(query)
                total_partitions += 1
                new_files = set(target_dir.rglob("*.parquet"))
                # This logic was add, because it can be extended to the creation of more than one file
                created_files = new_files - existing_files
                
                if not created_files:
                    msg = f"No new parquet file was created in: {target_dir}"
                    log_error_msg(logger, msg)
                    timer.stop("total")
                    resource_stats = monitor.stop()
                    benchmark_end_timestamp = Timer.get_timestamp()
                    execution_time = timer.duration("total")
                    throughput = total_rows / execution_time if execution_time > 0 else 0.0
                    return build_return_dict_compaction_parquet_lake(parquet_path = parquet_path, compacted_parquet_path = compacted_parquet_path,
                                                                     operation = STANDARD, ntest = ntest, processed_files = total_files, processed_rows = total_rows,
                                                                     execution_time = execution_time, throughput = throughput, threads = threads,
                                                                     benchmark_start_timestamp = benchmark_start_timestamp.isoformat(),
                                                                     benchmark_end_timestamp =  benchmark_end_timestamp.isoformat(),
                                                                     resource_stats = resource_stats, created_parquet_files = created_parquet_files,
                                                                     msg = MSG_ERROR, total_partitions = total_partitions,
                                                                     compacted_parquet_size = compacted_parquet_size)

                created_parquet_files += len(created_files)
                created_paths = [f.as_posix() for f in created_files]
                paths_sql = ", ".join(f"'{p}'" for p in created_paths)

                if verify_count_rows:
                    readback_nrows = connection.execute(f"SELECT COUNT(*) FROM read_parquet([{paths_sql}])").fetchone()[0]
                    if nrows != readback_nrows:
                        msg = f"Row mismatch in {rel_path}: Expected {nrows} rows, but new parquet files contain {readback_nrows}"
                        log_error_msg(logger, msg)
                        timer.stop("total")
                        resource_stats = monitor.stop()
                        benchmark_end_timestamp = Timer.get_timestamp()
                        execution_time = timer.duration("total")
                        throughput = total_rows / execution_time if execution_time > 0 else 0.0
                        return build_return_dict_compaction_parquet_lake(parquet_path = parquet_path, compacted_parquet_path = compacted_parquet_path,
                                                                         operation = STANDARD, ntest = ntest, processed_files = total_files, processed_rows = total_rows,
                                                                         execution_time = execution_time, throughput = throughput, threads = threads,
                                                                         benchmark_start_timestamp = benchmark_start_timestamp.isoformat(),
                                                                         benchmark_end_timestamp =  benchmark_end_timestamp.isoformat(),
                                                                         resource_stats = resource_stats, created_parquet_files = created_parquet_files,
                                                                         msg = MSG_ERROR, total_partitions = total_partitions,
                                                                         compacted_parquet_size = compacted_parquet_size)                

                size_bytes = [f.stat().st_size for f in created_files]
                compacted_parquet_size += (sum(size_bytes) / MIB)
                
                total_rows += nrows
                msg = f"Compacted {rel_path}. ({nrows} row(s))"
                log_info_msg(logger, msg)
                
    except Exception as e:
        msg = f"Compaction failed: {type(e).__name__}: {e}"
        log_error_msg(logger, msg)
        timer.stop("total")
        resource_stats = monitor.stop()
        benchmark_end_timestamp = Timer.get_timestamp()
        execution_time = timer.duration("total")
        throughput = total_rows / execution_time if execution_time > 0 else 0.0
        return build_return_dict_compaction_parquet_lake(parquet_path = parquet_path, compacted_parquet_path = compacted_parquet_path,
                                                         operation = STANDARD, ntest = ntest, processed_files = total_files, processed_rows = total_rows,
                                                         execution_time = execution_time, throughput = throughput, threads = threads,
                                                         benchmark_start_timestamp = benchmark_start_timestamp.isoformat(),
                                                         benchmark_end_timestamp =  benchmark_end_timestamp.isoformat(),
                                                         resource_stats = resource_stats, created_parquet_files = created_parquet_files,
                                                         msg = MSG_ERROR, total_partitions = total_partitions,
                                                         compacted_parquet_size = compacted_parquet_size)

                
    finally:
    	if connection is not None:
        	connection.close()
                               
    timer.stop("total")
    resource_stats = monitor.stop()
    benchmark_end_timestamp = Timer.get_timestamp()
    execution_time = timer.duration("total")
    throughput = total_rows / execution_time if execution_time > 0 else 0
            
    print(f"DuckDB compacted {total_rows} row(s) to a Parquet file in {execution_time:.2f} seconds")
    print(f"Throughput: {throughput:.2f} rows/sec")

    return build_return_dict_compaction_parquet_lake(parquet_path = parquet_path, compacted_parquet_path = compacted_parquet_path,
                                                     operation = STANDARD, ntest = ntest, processed_files = total_files, processed_rows = total_rows,
                                                     execution_time = execution_time, throughput = throughput, threads = threads,
                                                     benchmark_start_timestamp = benchmark_start_timestamp.isoformat(),
                                                     benchmark_end_timestamp =  benchmark_end_timestamp.isoformat(),
                                                     resource_stats = resource_stats, created_parquet_files = created_parquet_files,
                                                     msg = "Compaction step completed successfully", total_partitions = total_partitions,
                                                     compacted_parquet_size = compacted_parquet_size)
if __name__ == "__main__":
    # Create logger
    logger = setup_logger()
    result = benchmark_compaction_parquet_lake(parquet_path = Path("../data/parquet/2nd-slice/"), 
                                            compacted_parquet_path = "../data/parquet/2nd-slice_compacted/", 
                                            logger = logger, 
                                            ntest = 25,
                                            verify_count_rows = True, 
                                            threads = 0)

    log_info_msg(logger, result["msg"])
    print(result["msg"])
    timestamp = result['benchmark_start_timestamp'].replace(":", "-")
    save_result_to_csv_and_json(result, 
                                f"./compaction/csv/compaction_sm.csv",
                                f"./compaction/json/compaction_{result['test_number']}_{timestamp}.json")
