from common import ResourceMonitor, INGESTION, build_csv_row_ingestion_spark, test_session, save_result_to_csv_and_json, create_spark_session, compute_dataset_size_mib, CSV_PATTERN, count_files_fs, STANDARD, Timer, SparkMetricsCollector, SparkRestMetricsCollector, setup_logger, log_error_msg, log_info_msg
from pyspark.sql import SparkSession
import uuid
from typing import Any
import logging


# Build benchmark return for ingestion step
def build_return_dict_ingestion(
    spark_session: SparkSession,
    slice_path: str,
    operation: str,
    ntest: int,
    processed_files: int,
    processed_rows: int,
    execution_time: float,
    throughput: float,
    benchmark_start_timestamp: str,
    benchmark_end_timestamp: str,
    msg: str,
    dataframe_partitiones: int,
    spark_metrics:  dict[str, Any],
    spark_rest_metrics: dict[str, Any],
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
        "slice_path": slice_path,
        # Performance
        "processed_files": processed_files,
        "processed_rows": processed_rows,
        "execution_time_sec": execution_time,
        "throughput_rows_sec": throughput,
        "dataset_size_mib": compute_dataset_size_mib(spark_session, slice_path),
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
        # Spark Metrics
        "dataframe_partitions": dataframe_partitiones,
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

# Benchmark CSV ingestion using Spark by calling read.csv function to read files from a S3 bucket,
# counting rows, measuring execution time,
# throughput and other metrics. Only reading data.
# Formal parameters:
# spark_session: Spark Session 
# slice_path: path to the directory containing CSV files for the slice
# logger: Logger instance for reporting progress and errors.
# ntest: test number
# Returns:
# benchmark results including slice path, processed files, processed rows, execution time, throughput, timestamps, partitions
# resource statistics, a message and spark metrics
def benchmark_spark_ingestion_read_only(
    spark_session: SparkSession,
    slice_path: str, 
    ntest: int,
    logger: logging.Logger | None = None
) -> dict[str, Any]:

    if logger is None:
        logger = setup_logger()

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
    MSG_ERROR = "Ingestion finished with errors, check pipeline.log"
    processed_rows = 0
    processed_files = 0
    dataframe_partitiones = 0
    
    try:
    
        processed_files = count_files_fs(spark_session = spark_session, path = slice_path)
    
        if processed_files == 0:
            log_error_msg(logger, f"No CSV files found in path: {slice_path}")
            timer.stop("total")
            benchmark_end_timestamp = Timer.get_timestamp()
            resource_stats = monitor.stop()
            execution_time = timer.duration("total")
            after = collector.snapshot()
            metrics = collector.collect_between(before, after)
            rest_metrics = rest_collector.collect()
            sc.setLocalProperty("spark.jobGroup.id", None) 
            return build_return_dict_ingestion(slice_path = slice_path, operation = STANDARD, ntest = ntest, processed_files = processed_files, processed_rows = processed_rows, 
                                               execution_time = execution_time, throughput = 0.0, benchmark_start_timestamp =  benchmark_start_timestamp.isoformat(),
                                               benchmark_end_timestamp = benchmark_end_timestamp.isoformat(), resource_stats = resource_stats,
                                               msg = MSG_ERROR, spark_session = spark_session, spark_metrics = metrics, 
                                               spark_rest_metrics = rest_metrics, dataframe_partitiones = dataframe_partitiones)
            
        log_info_msg(logger, "NYC Yellow trip taxi dataset")
        print(f"Reading {slice_path}...")
        
        csv_path = f"{str(slice_path).rstrip('/')}/{CSV_PATTERN}"
        
        df = (
                spark_session.read
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(csv_path)
        )
        processed_rows = df.count()
        dataframe_partitiones = df.rdd.getNumPartitions()
        
    except Exception as e:
        log_error_msg(logger, f"Unexpected Spark ingestion error for {slice_path}: {type(e).__name__}: {e}")
        timer.stop("total")
        benchmark_end_timestamp = Timer.get_timestamp()
        resource_stats = monitor.stop()
        execution_time = timer.duration("total")
        throughput = processed_rows / execution_time if execution_time > 0 else 0.0
        sc.cancelJobGroup(group_id)
        after = collector.snapshot()
        metrics = collector.collect_between(before, after)
        rest_metrics = rest_collector.collect()
        sc.setLocalProperty("spark.jobGroup.id", None) 
        return build_return_dict_ingestion(slice_path = slice_path, operation = STANDARD, ntest = ntest, processed_files = processed_files, processed_rows = processed_rows, 
                                           execution_time = execution_time, throughput = throughput, benchmark_start_timestamp =  benchmark_start_timestamp.isoformat(),
                                           benchmark_end_timestamp = benchmark_end_timestamp.isoformat(), resource_stats = resource_stats,
                                           msg = MSG_ERROR, spark_session = spark_session, 
                                           spark_metrics = metrics, spark_rest_metrics = rest_metrics, dataframe_partitiones = dataframe_partitiones)
    
    timer.stop("total")
    benchmark_end_timestamp = Timer.get_timestamp()
    resource_stats = monitor.stop()
    

    execution_time = timer.duration("total")
    throughput = processed_rows / execution_time if execution_time > 0 else 0

    after = collector.snapshot()
    metrics = collector.collect_between(before, after)
    rest_metrics = rest_collector.collect()
    sc.setLocalProperty("spark.jobGroup.id", None) 
    
    print(f"Spark ingested {processed_files} file(s) and {processed_rows} row(s) in {execution_time:.2f} seconds")
    print(f"Throughput: {throughput:.2f} rows/sec")
    return build_return_dict_ingestion(slice_path = slice_path, operation = STANDARD, ntest = ntest, processed_files = processed_files, processed_rows = processed_rows, 
                                       execution_time = execution_time, throughput = throughput, benchmark_start_timestamp =  benchmark_start_timestamp.isoformat(),
                                       benchmark_end_timestamp = benchmark_end_timestamp.isoformat(), resource_stats = resource_stats,
                                       msg = "Ingestion finished successfully", spark_session = spark_session, 
                                       spark_metrics = metrics, spark_rest_metrics = rest_metrics, dataframe_partitiones = dataframe_partitiones)


if __name__ == "__main__":
    # Create logger
    logger = setup_logger()
    spark_session = create_spark_session(logger)
    result = benchmark_spark_ingestion_read_only(spark_session = spark_session, 
                                                 slice_path = "hdfs:///data/CSV/raw-data/4th-slice/", 
                                                 ntest = 4, 
                                                 logger = logger)
    
    spark_session.stop()
    log_info_msg(logger, result["msg"])
    print(result["msg"])
    timestamp = result['benchmark_start_timestamp'].replace(":", "-")
    save_result_to_csv_and_json(result, 
                                f"./ingestion/csv/ingestion_ce.csv",
                                f"./ingestion/json/ingestion_{result['test_number']}_{timestamp}.json",
                                build_csv_row_ingestion_spark)
