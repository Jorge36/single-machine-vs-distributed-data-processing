from pyspark import StorageLevel
from common import ResourceMonitor, is_empty, count_df, build_csv_row_parquet_persistence_spark, compute_files_size_mib, list_files_fs, PERSISTENCE_TO_PARQUET, test_session, save_result_to_csv_and_json_cleaning, create_spark_session, compute_dataset_size_mib, CSV_PATTERN, count_files_fs, STANDARD, Timer, SparkMetricsCollector, SparkRestMetricsCollector, setup_logger, log_error_msg, log_info_msg
from pyspark.sql import SparkSession, DataFrame, functions as F, Row
import uuid
from typing import Any
import logging
from pyspark.sql.functions import input_file_name
from benchmark_cleaning_transf_profiling import apply_master_schema, cleaning_transformation, profile_missing_values, profile_vendor_specific_differences

# Persist a DataFrame to partitioned parquet files using Spark
# Data is partitioned by year and month
# parquet_path/year=YYYY/month=MM/
# Parameters:
# spark_session: SparkSession
# df: input DataFrame to persist
# parquet_path: output directory in Hadoop HDFS for parquet files
# logger: Logger instance for reporting progress and errors
# verify_count_rows: if to validate written parquet files by counting rows
# Returns:
# True if persistence completed successfully, false otherwise
# The number of created files
# Total size of the parquet files
def persist_partitioned_parquet_spark(
    spark_session: SparkSession,
    df: DataFrame, 
    parquet_path: str, 
    logger: logging.Logger | None = None, 
    verify_count_rows: bool = False,
) -> tuple[bool, int, float]:

    MIB = 1024 * 1024
    
    if logger is None:
        logger = setup_logger()

    created_files = set()
    parquet_files_size_mib = 0.0
    
    try: 
        
        validation_row = df.agg(
            F.count("*").alias("nrows"),
            F.sum(
                F.when(
                 F.col("year").isNotNull() & F.col("month").isNotNull(),
                 1
                ).otherwise(0)
            ).alias("expected_rows")
        ).collect()[0]
    
        nrows = validation_row["nrows"] or 0    
        expected_rows = validation_row["expected_rows"] or 0
    
        if expected_rows != nrows:    
            log_error_msg(logger, "Some rows have null year/month values, parquet persistence aborted")
            return False, 0, 0.0
    
        # Files before writing 
        existing_files = set(list_files_fs(spark_session = spark_session, path = parquet_path))
    
        # Persist to partitioned parquet
        df.write.mode("append").partitionBy("year", "month").parquet(parquet_path)
    
        # Files after writing
        new_files = set(list_files_fs(spark_session = spark_session, path = parquet_path))
    
        created_files = new_files - existing_files
    
        if not created_files:
            msg = f"No new parquet files were created in: {parquet_path}"
            log_error_msg(logger, msg)
            return False, 0, 0.0
    
        # Compute size of created parquet files
        parquet_files_size_mib = compute_files_size_mib(spark_session, list(created_files))
    
        if verify_count_rows:
            readback_nrows = (
                spark_session.read.parquet(*list(created_files)).count()
            )
    
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
    
# Build benchmark return for parquet persistence step
def build_return_dict_parquet_persistence(
    spark_session: SparkSession,
    slice_path: str,
    parquet_path: str,
    operation: str,
    ntest: int, 
    processed_files: int,
    processed_rows: int,
    rows_after_cleaning: int,
    execution_time: float,
    throughput: float,
    parquet_files_size_mib: float,
    created_parquet_files: int,
    benchmark_start_timestamp: str,
    benchmark_end_timestamp: str,
    msg: str,
    dataframe_partitions: int,
    dataframe_partitions_parquet: int,
    summary: list[Row] | None,
    missing_values: list[Row] | None,
    spark_metrics:  dict[str, Any],
    spark_rest_metrics: dict[str, Any],
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
        "slice_path": slice_path,
        # Parquet path:
        "parquet_path": parquet_path,
        # Performance
        "processed_files": processed_files,
        "processed_rows": processed_rows,
        "rows_after_cleaning": rows_after_cleaning,
        "execution_time_sec": execution_time,
        "throughput_rows_sec": throughput,
        "dataset_size_mib": compute_dataset_size_mib(spark_session, slice_path),
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
        "net_sent_mib": resource_stats["net_sent_mib"],
        "net_received_mib": resource_stats["net_received_mib"],
        "failed_process_metric_count": resource_stats["failed_process_metric_count"],
        # Message
        "msg": msg,
        # Profiling
        "summary": summary,
        "missing_values": missing_values,
        # Spark Metrics
        "dataframe_partitions": dataframe_partitions,
        "dataframe_partitions_parquet": dataframe_partitions_parquet,
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

# Benchmark CSV ingestion, cleaning and transformation using Spark by calling read.csv function to read files from a S3 bucket,
# counting rows, measuring execution time,
# throughput and other metrics. Only reading data.
# Formal parameters:
# spark_session: Spark Session 
# slice_path: path to the directory in S3 containing CSV files for the slice
# parquet_path: output path in HDFS Hadoop where parquet files are saved
# removed_slice_path: output path in S3 to save the removed rows from the DataFrame
# logger: Logger instance for reporting progress and errors.
# ntest: test number
# verify_count_rows: if to verify row counts after compaction
# Returns:
# benchmark results including slice path, processed files, processed rows, execution time, throughput, timestamps, partitions, 
# resource statistics, spark metrics, a message, etc
def benchmark_spark_parquet_persistence(
    spark_session: SparkSession,
    slice_path: str, 
    parquet_path: str, 
    removed_slice_path: str,
    ntest: int,
    logger: logging.Logger | None = None,
    verify_count_rows: bool = False
) -> tuple[DataFrame, dict[str, Any]]:

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
    MSG_ERROR = "Ingestion, cleaning, transformation, profiling and persistence to parquet finished with errors, check pipeline.log"
    processed_rows = 0
    processed_files = 0
    dataframe_partitions = 0
    dataframe_partitions_parquet = 0
    rows_after_cleaning = 0
    created_parquet_files = 0
    parquet_files_size_mib = 0.0
    msg = MSG_ERROR
    
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
            
            return build_return_dict_parquet_persistence(slice_path = slice_path, operation = STANDARD, ntest = ntest, processed_files = processed_files, processed_rows = processed_rows, 
                                                         execution_time = execution_time, throughput = 0.0, benchmark_start_timestamp =  benchmark_start_timestamp.isoformat(),
                                                         benchmark_end_timestamp = benchmark_end_timestamp.isoformat(), resource_stats = resource_stats,
                                                         msg = MSG_ERROR, spark_session = spark_session, spark_metrics = metrics,
                                                         spark_rest_metrics = rest_metrics, dataframe_partitions = dataframe_partitions,
                                                         summary = None, missing_values = None, rows_after_cleaning = rows_after_cleaning,
                                                         dataframe_partitions_parquet = dataframe_partitions_parquet,
                                                         parquet_path = parquet_path,
                                                         created_parquet_files = created_parquet_files, parquet_files_size_mib = parquet_files_size_mib)

            
        log_info_msg(logger, "NYC Yellow trip taxi dataset")
        print(f"Reading {slice_path}...")
        
        csv_path = f"{str(slice_path).rstrip('/')}/{CSV_PATTERN}"
        
        df = (
                spark_session.read
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(csv_path)
        )
        df = df.withColumn("filename", input_file_name())
        df = df.withColumn("currency", F.lit("USD"))
        df = df.withColumn("trip_distance_unit", F.lit("Miles"))
        df = df.withColumn("year", F.regexp_extract("filename", r"yellow_tripdata_(\d{4})-(\d{2})\.csv$", 1).cast("short"))
        df = df.withColumn("month", F.regexp_extract("filename", r"yellow_tripdata_(\d{4})-(\d{2})\.csv$", 2).cast("byte"))
        df = df.drop("filename")
        df = df.withColumn("suspicious", F.lit(False))
        
        dataframe_partitions = df.rdd.getNumPartitions()

        if is_empty(df):
            
            log_error_msg(logger, f"No valid CSV files were ingested in {slice_path}")
            timer.stop("total")
            benchmark_end_timestamp = Timer.get_timestamp()
            resource_stats = monitor.stop()
            execution_time = timer.duration("total")
            throughput = processed_rows / execution_time if execution_time > 0 else 0
            after = collector.snapshot()
            metrics = collector.collect_between(before, after)
            rest_metrics = rest_collector.collect()
            sc.setLocalProperty("spark.jobGroup.id", None) 
            
            return build_return_dict_parquet_persistence(slice_path = slice_path, operation = STANDARD, ntest = ntest, processed_files = processed_files, processed_rows = processed_rows, 
                                                         execution_time = execution_time, throughput = throughput, benchmark_start_timestamp =  benchmark_start_timestamp.isoformat(),
                                                         benchmark_end_timestamp = benchmark_end_timestamp.isoformat(), resource_stats = resource_stats,
                                                         msg = MSG_ERROR, spark_session = spark_session, spark_metrics = metrics,
                                                         spark_rest_metrics = rest_metrics, dataframe_partitions = dataframe_partitions,
                                                         summary = None, missing_values = None, rows_after_cleaning = rows_after_cleaning,
                                                         dataframe_partitions_parquet = dataframe_partitions_parquet,
                                                         parquet_path = parquet_path,
                                                         created_parquet_files = created_parquet_files, parquet_files_size_mib = parquet_files_size_mib)
                        

        log_info_msg(logger, "Ingestion finished successfully")
        processed_rows = df.count()
        
        df_normalised = apply_master_schema(df = df, logger = logger)
                
        df_cleaned = df_normalised.limit(0)
        if not is_empty(df_normalised):
            df_cleaned = cleaning_transformation(df = df_normalised, removed_slice_path = removed_slice_path,
                                                logger = logger, header = True)
            del df_normalised

        summary = None
        missing_values = None

        if not is_empty(df_cleaned):
            df_cleaned = df_cleaned.persist(StorageLevel.DISK_ONLY)
            dataframe_partitions_parquet = df_cleaned.rdd.getNumPartitions()
            rows_after_cleaning = count_df(df_cleaned)
            summary = profile_vendor_specific_differences(df_cleaned).collect()
            missing_values = profile_missing_values(df_cleaned).collect()
            persisted = False
            persisted, created_parquet_files, parquet_files_size_mib = persist_partitioned_parquet_spark(
                spark_session = spark_session,
                df = df_cleaned,
                parquet_path = parquet_path,
                logger = logger,
                verify_count_rows = verify_count_rows
            )
            if persisted:
                msg = "Ingestion, cleaning, transformation, profiling and persistence to parquet completed successfully"
            
            df_cleaned.unpersist(blocking = True)

    except Exception as e:
        log_error_msg(logger, f"Unexpected Spark ingestion error for {slice_path}: {type(e).__name__}: {e}")
        timer.stop("total")
        benchmark_end_timestamp = Timer.get_timestamp()
        resource_stats = monitor.stop()
        execution_time = timer.duration("total")
        throughput = processed_rows / execution_time if execution_time > 0 else 0
        after = collector.snapshot()
        metrics = collector.collect_between(before, after)
        rest_metrics = rest_collector.collect()
        sc.setLocalProperty("spark.jobGroup.id", None) 
        
        return build_return_dict_parquet_persistence(slice_path = slice_path, operation = STANDARD, ntest = ntest, processed_files = processed_files, processed_rows = processed_rows, 
                                                     execution_time = execution_time, throughput = throughput, benchmark_start_timestamp =  benchmark_start_timestamp.isoformat(),
                                                     benchmark_end_timestamp = benchmark_end_timestamp.isoformat(), resource_stats = resource_stats,
                                                     msg = MSG_ERROR, spark_session = spark_session, spark_metrics = metrics,
                                                     spark_rest_metrics = rest_metrics, dataframe_partitions = dataframe_partitions,
                                                     summary = None, missing_values = None, rows_after_cleaning = rows_after_cleaning,
                                                     dataframe_partitions_parquet = dataframe_partitions_parquet,
                                                     parquet_path = parquet_path,
                                                     created_parquet_files = created_parquet_files, parquet_files_size_mib = parquet_files_size_mib)
                        

                
    timer.stop("total")
    benchmark_end_timestamp = Timer.get_timestamp()
    resource_stats = monitor.stop()
    

    execution_time = timer.duration("total")
    throughput = processed_rows / execution_time if execution_time > 0 else 0
    after = collector.snapshot()
    metrics = collector.collect_between(before, after)
    rest_metrics = rest_collector.collect()
    sc.setLocalProperty("spark.jobGroup.id", None) 

    
    print(f"Spark persisted {processed_files} file(s) and {processed_rows} row(s) in {execution_time:.2f} seconds")
    print(f"Throughput: {throughput:.2f} rows/sec")
    

    return build_return_dict_parquet_persistence(slice_path = slice_path, operation = STANDARD, ntest = ntest, processed_files = processed_files, processed_rows = processed_rows, 
                                                 execution_time = execution_time, throughput = throughput, benchmark_start_timestamp =  benchmark_start_timestamp.isoformat(),
                                                 benchmark_end_timestamp = benchmark_end_timestamp.isoformat(), resource_stats = resource_stats,
                                                 msg = msg, spark_session = spark_session, spark_metrics = metrics,
                                                 spark_rest_metrics = rest_metrics, dataframe_partitions = dataframe_partitions,
                                                 summary = summary, missing_values = missing_values, rows_after_cleaning = rows_after_cleaning,
                                                 dataframe_partitions_parquet = dataframe_partitions_parquet,
                                                 parquet_path = parquet_path,
                                                 created_parquet_files = created_parquet_files, parquet_files_size_mib = parquet_files_size_mib)

if __name__ == "__main__":
    # Create logger
    logger = setup_logger()
    spark_session = create_spark_session(logger)
    result = benchmark_spark_parquet_persistence(spark_session = spark_session, 
                                                 slice_path = "hdfs:///data/CSV/raw-data/1st-slice/", 
                                                 ntest = 1, logger = logger,
                                                 parquet_path = "hdfs:///data/parquet/1st-slice",
                                                 removed_slice_path = "hdfs:///data/CSV/removed/1st-slice",
                                                 verify_count_rows = True)
   
    spark_session.stop()	
    log_info_msg(logger, result["msg"])
    print(result["msg"])
    timestamp = result['benchmark_start_timestamp'].replace(":", "-")
    save_result_to_csv_and_json_cleaning(result, 
                                         f"./persistence/csv/persistence_ce.csv",
                                         f"./persistence/json/persistence_{result['test_number']}_{timestamp}.json",
                                         build_csv_row_parquet_persistence_spark)
    

                                	 
