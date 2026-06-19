from pyspark import StorageLevel
from common import ResourceMonitor, build_csv_row_compaction_parquet_lake_spark, count_df, path_exists_fs, list_month_partition_dir_fs, compute_files_size_mib, list_files_fs, COMPACTION_TO_PARQUET_FILE, test_session, save_result_to_csv_and_json, create_spark_session, compute_dataset_size_mib, STANDARD, Timer, SparkMetricsCollector, SparkRestMetricsCollector, setup_logger, log_error_msg, log_info_msg
from pyspark.sql import SparkSession
import uuid
from typing import Any
import logging
from datetime import datetime

# Build benchmark return for parquet persistence step
def build_return_dict_compaction_parquet_lake(
    spark_session: SparkSession,
    parquet_path: str,
    compacted_parquet_path: str,
    operation: str,
    ntest: int,
    created_parquet_files: int,
    total_partitions: int,
    compacted_parquet_size: float,
    dataframe_partitions_parquet: list[int],
    dataframe_partitions_compacted_parquet: list[int],
    processed_files: int,
    processed_rows: int,
    execution_time: float,
    throughput: float,
    benchmark_start_timestamp: str,
    benchmark_end_timestamp: str,
    msg: str,
    spark_metrics:  dict[str, Any],
    spark_rest_metrics: dict[str, Any],
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
        "parquet_path": parquet_path,
        "compacted_parquet_path": compacted_parquet_path,
        # Performance
        "processed_files": processed_files,
        "processed_rows": processed_rows,
        "execution_time_sec": execution_time,
        "throughput_rows_sec": throughput,
        "parquet_size_mib": compute_dataset_size_mib(spark_session = spark_session, path = parquet_path, suffix = ".parquet"),
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
        "net_sent_mib": resource_stats["net_sent_mib"],
        "net_received_mib": resource_stats["net_received_mib"],
        "failed_process_metric_count": resource_stats["failed_process_metric_count"],
        # Message
        "msg": msg,
        # Spark
        "dataframe_partitions_parquet": dataframe_partitions_parquet,
        "dataframe_partitions_compacted_parquet": dataframe_partitions_compacted_parquet,
        "job_ids": spark_metrics["job_ids"],
        "stage_ids": spark_metrics["stage_ids"],
        "num_jobs": spark_metrics["num_jobs"],
        "num_stages": spark_metrics["num_stages"],
        "num_executors": spark_metrics["num_executors"],
        "parallelism": spark_metrics["parallelism"],
        "stage_metrics": spark_metrics["stage_metrics"],
        # Spark Metrics - Rest endpoint
        "spark_app_id": spark_rest_metrics["spark_app_id"],
        "num_jobs": spark_rest_metrics["num_jobs"],
        "num_stages": spark_rest_metrics["num_stages"],
        "num_executors": spark_rest_metrics["num_executors"],
        "total_tasks": spark_rest_metrics["total_tasks"],
        "total_input_mib": spark_rest_metrics["total_input_mib"],
        "total_output_mib": spark_rest_metrics["total_output_mib"],
        "total_shuffle_read_mib": spark_rest_metrics["total_shuffle_read_mib"],
        "total_shuffle_write_mib": spark_rest_metrics["total_shuffle_write_mib"],
        "total_executor_runtime_sec": spark_rest_metrics["total_executor_runtime_sec"],
        "stages": spark_rest_metrics["stages"],
        "executors": spark_rest_metrics["executors"],
        "jobs": spark_rest_metrics["jobs"]
    }

# Benchamrk compaction of partitioned parquet lake using spark
# Parameters:
# spark_session: Spark Session
# parquet_path: directory partitioned in HDFS Hadoop parquet lake
# compacted_parquet_path: output path in HDFS Hadoop for compacted parquet lake
# ntest: test number
# logger: Logger instance for reporting progress and errors
# verify_count_rows: if to verify row counts after compaction
# Returns:
# benchmark results including slice path, processed files, processed rows, execution time, throughput, timestamps, partitions,
# created files, size of dataset/files, resource statistics, spark metrics, a message, etc
def benchmark_compaction_parquet_lake(
    spark_session: SparkSession,
    parquet_path: str,
    compacted_parquet_path: str, 
    ntest: int,
    logger: logging.Logger | None = None,
    verify_count_rows: bool = False,
) -> dict[str, Any]:

    if logger is None:
        logger = setup_logger()

    MIB = 1024 * 1024
    
    timer = Timer()
    monitor = ResourceMonitor()
    collector = SparkMetricsCollector(spark_session)
    rest_collector = SparkRestMetricsCollector(spark_session)
    group_id = f"benchmark-{uuid.uuid4().hex}"
    sc = spark_session.sparkContext
    sc.setJobGroup(group_id, "thesis benchmark", interruptOnCancel = True)
    sc.setLocalProperty("spark.jobGroup.id", group_id)
    before = collector.snapshot()
    benchmark_start_timestamp = Timer.get_timestamp()
    timer.start("total")
    monitor.start()
    
    created_parquet_files = 0
    total_rows = 0
    total_files = 0
    total_partitions = 0
    compacted_parquet_size = 0.0
    MSG_ERROR = "Compaction step completed with errors, check pipeline.log"
    dataframe_partitions_parquet = []
    dataframe_partitions_compacted_parquet = []

    if not path_exists_fs(spark_session, parquet_path):
        msg = f"Source parquet path does not exist: {parquet_path}"
        log_error_msg(logger, msg)
        timer.stop("total")
        resource_stats = monitor.stop()
        benchmark_end_timestamp = Timer.get_timestamp()
        execution_time = timer.duration("total")
        throughput = total_rows / execution_time if execution_time > 0 else 0.0
        after = collector.snapshot()
        metrics = collector.collect_between(before, after)
        rest_metrics = rest_collector.collect()
        sc.setLocalProperty("spark.jobGroup.id", None) 
        return build_return_dict_compaction_parquet_lake(spark_session = spark_session, parquet_path = parquet_path, compacted_parquet_path = compacted_parquet_path,
                                                         operation = STANDARD, ntest = ntest, created_parquet_files = created_parquet_files,
                                                         total_partitions = total_partitions, compacted_parquet_size = compacted_parquet_size,
                                                         dataframe_partitions_parquet = dataframe_partitions_parquet, 
                                                         dataframe_partitions_compacted_parquet = dataframe_partitions_compacted_parquet,
                                                         processed_files = total_files, processed_rows = total_rows,
                                                         execution_time = execution_time, throughput = throughput, 
                                                         benchmark_start_timestamp = benchmark_start_timestamp.isoformat(),
                                                         benchmark_end_timestamp =  benchmark_end_timestamp.isoformat(),
                                                         resource_stats = resource_stats, msg = MSG_ERROR, 
                                                         spark_metrics = metrics,
                                                         spark_rest_metrics = rest_metrics)
    
    try:

        month_dirs = list_month_partition_dir_fs(spark_session, parquet_path)

        for month_dir, rel_path in month_dirs:

            run_id = datetime.now().strftime("%Y%m%d_%H%M%S")

            target_dir = f"{compacted_parquet_path.rstrip('/')}/{rel_path}/compacted_data_{run_id}_{uuid.uuid4().hex}"

            total_files += len(list_files_fs(spark_session, month_dir))

            df_partition = spark_session.read.parquet(month_dir).persist(StorageLevel.MEMORY_AND_DISK)

            dataframe_partitions_parquet.append(df_partition.rdd.getNumPartitions())

            nrows =  count_df(df_partition)

            df_partition_coalesced = df_partition.coalesce(1)

            df_partition_coalesced.write.mode("overwrite").parquet(target_dir)

            df_partition.unpersist(blocking = True)

            dataframe_partitions_compacted_parquet.append(df_partition_coalesced.rdd.getNumPartitions())

            total_partitions += 1
            
            created_files = set(list_files_fs(spark_session, target_dir))

            if not created_files:
                msg = f"No new parquet file was created in: {target_dir}"
                log_error_msg(logger, msg)
                timer.stop("total")
                resource_stats = monitor.stop()
                benchmark_end_timestamp = Timer.get_timestamp()
                execution_time = timer.duration("total")
                throughput = total_rows / execution_time if execution_time > 0 else 0.0
                after = collector.snapshot()
                metrics = collector.collect_between(before, after)
                rest_metrics = rest_collector.collect()
                sc.setLocalProperty("spark.jobGroup.id", None) 
                return build_return_dict_compaction_parquet_lake(spark_session = spark_session, parquet_path = parquet_path, compacted_parquet_path = compacted_parquet_path,
                                                                 operation = STANDARD, ntest = ntest, created_parquet_files = created_parquet_files,
                                                                 total_partitions = total_partitions, compacted_parquet_size = compacted_parquet_size,
                                                                 dataframe_partitions_parquet = dataframe_partitions_parquet, 
                                                                 dataframe_partitions_compacted_parquet = dataframe_partitions_compacted_parquet,
                                                                 processed_files = total_files, processed_rows = total_rows,
                                                                 execution_time = execution_time, throughput = throughput, 
                                                                 benchmark_start_timestamp = benchmark_start_timestamp.isoformat(),
                                                                 benchmark_end_timestamp =  benchmark_end_timestamp.isoformat(),
                                                                 resource_stats = resource_stats, msg = MSG_ERROR, 
                                                                 spark_metrics = metrics,
                                                                 spark_rest_metrics = rest_metrics)

            
            created_parquet_files += len(created_files)
            compacted_parquet_size += compute_files_size_mib(
                spark_session = spark_session,
                files = list(created_files)
            )

            if verify_count_rows:
                readback_nrows = spark_session.read.parquet(*list(created_files)).count()
                if readback_nrows != nrows:
                    msg = f"Row mismatch in {rel_path}: Expected {nrows} rows, but new parquet files contain {readback_nrows}"
                    log_error_msg(logger, msg)
                    timer.stop("total")
                    resource_stats = monitor.stop()
                    benchmark_end_timestamp = Timer.get_timestamp()
                    execution_time = timer.duration("total")
                    throughput = total_rows / execution_time if execution_time > 0 else 0.0
                    after = collector.snapshot()
                    metrics = collector.collect_between(before, after)
                    rest_metrics = rest_collector.collect()
                    sc.setLocalProperty("spark.jobGroup.id", None) 
                    return build_return_dict_compaction_parquet_lake(spark_session = spark_session, parquet_path = parquet_path, compacted_parquet_path = compacted_parquet_path,
                                                                     operation = STANDARD, ntest = ntest, created_parquet_files = created_parquet_files,
                                                                     total_partitions = total_partitions, compacted_parquet_size = compacted_parquet_size,
                                                                     dataframe_partitions_parquet = dataframe_partitions_parquet, 
                                                                     dataframe_partitions_compacted_parquet = dataframe_partitions_compacted_parquet,
                                                                     processed_files = total_files, processed_rows = total_rows,
                                                                     execution_time = execution_time, throughput = throughput, 
                                                                     benchmark_start_timestamp = benchmark_start_timestamp.isoformat(),
                                                                     benchmark_end_timestamp =  benchmark_end_timestamp.isoformat(),
                                                                     resource_stats = resource_stats, msg = MSG_ERROR, 
                                                                     spark_metrics = metrics,
                                                                     spark_rest_metrics = rest_metrics)

            
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
        after = collector.snapshot()
        metrics = collector.collect_between(before, after)
        rest_metrics = rest_collector.collect()
        sc.setLocalProperty("spark.jobGroup.id", None) 
        return build_return_dict_compaction_parquet_lake(spark_session = spark_session, parquet_path = parquet_path, compacted_parquet_path = compacted_parquet_path,
                                                         operation = STANDARD, ntest = ntest, created_parquet_files = created_parquet_files,
                                                         total_partitions = total_partitions, compacted_parquet_size = compacted_parquet_size,
                                                         dataframe_partitions_parquet = dataframe_partitions_parquet, 
                                                         dataframe_partitions_compacted_parquet = dataframe_partitions_compacted_parquet,
                                                         processed_files = total_files, processed_rows = total_rows,
                                                         execution_time = execution_time, throughput = throughput, 
                                                         benchmark_start_timestamp = benchmark_start_timestamp.isoformat(),
                                                         benchmark_end_timestamp =  benchmark_end_timestamp.isoformat(),
                                                         resource_stats = resource_stats, msg = MSG_ERROR, 
                                                         spark_metrics = metrics,
                                                         spark_rest_metrics = rest_metrics)
   
    timer.stop("total")
    resource_stats = monitor.stop()
    benchmark_end_timestamp = Timer.get_timestamp()
    execution_time = timer.duration("total")
    throughput = total_rows / execution_time if execution_time > 0 else 0.0
    after = collector.snapshot()
    metrics = collector.collect_between(before, after)
    rest_metrics = rest_collector.collect()
    sc.setLocalProperty("spark.jobGroup.id", None) 
            
    print(f"Spark compacted {total_rows} row(s) to a Parquet folder in {execution_time:.2f} seconds")
    print(f"Throughput: {throughput:.2f} rows/sec")

    return build_return_dict_compaction_parquet_lake(spark_session = spark_session, parquet_path = parquet_path, compacted_parquet_path = compacted_parquet_path,
                                                     operation = STANDARD, ntest = ntest, created_parquet_files = created_parquet_files,
                                                     total_partitions = total_partitions, compacted_parquet_size = compacted_parquet_size,
                                                     dataframe_partitions_parquet = dataframe_partitions_parquet, 
                                                     dataframe_partitions_compacted_parquet = dataframe_partitions_compacted_parquet,
                                                     processed_files = total_files, processed_rows = total_rows,
                                                     execution_time = execution_time, throughput = throughput, 
                                                     benchmark_start_timestamp = benchmark_start_timestamp.isoformat(),
                                                     benchmark_end_timestamp =  benchmark_end_timestamp.isoformat(),
                                                     resource_stats = resource_stats, msg = "Compaction step completed successfully", 
                                                     spark_metrics = metrics,
                                                     spark_rest_metrics = rest_metrics)


if __name__ == "__main__":
    # Create logger
    logger = setup_logger()
    spark_session = create_spark_session(logger)
    result = benchmark_compaction_parquet_lake(spark_session = spark_session, 
                                               parquet_path = "hdfs:///data/parquet/1st-slice",
                                               compacted_parquet_path = "hdfs:///data/parquet/1st-slice_compacted",
                                               ntest = 1,
                                               logger = logger,
                                               verify_count_rows = True)

    spark_session.stop()
    log_info_msg(logger, result["msg"])
    print(result["msg"])
    timestamp = result['benchmark_start_timestamp'].replace(":", "-")
    save_result_to_csv_and_json(result, 
                                f"./compaction/csv/compaction_ce.csv",
                                f"./compaction/json/compaction_{result['test_number']}_{timestamp}.json",
                                build_csv_row_compaction_parquet_lake_spark)
