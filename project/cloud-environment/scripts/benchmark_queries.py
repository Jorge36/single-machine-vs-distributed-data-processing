from common import ResourceMonitor, count_df, build_csv_row_queries_spark, PARQUET_FOLDERS, list_files_fs, QUERIES, test_session, save_result_to_csv_and_json, create_spark_session, compute_dataset_size_mib, STANDARD, Timer, SparkMetricsCollector, SparkRestMetricsCollector, setup_logger, log_error_msg, log_info_msg
from pyspark.sql import SparkSession
import uuid
from typing import Any
import logging

SPARKSQL_QUERIES = {
    
    "q1_monthly_payment_statistics": """
            SELECT year, month, payment_type_id, COUNT(*) AS trip_count, AVG(trip_distance) AS avg_trip_distance,
                   trip_distance_unit, AVG(fare_amount) AS avg_fare_amount, AVG(tip_amount) AS avg_tip_amount,
                   SUM(total_amount) AS sum_total_amount, currency
            FROM parquet.`{parquet_path}`
            WHERE suspicious = FALSE
         GROUP BY year, month, payment_type_id, trip_distance_unit, currency
         ORDER BY year, month, payment_type_id
    """,

    "q2_most_valuable_routes": """
            SELECT pickup_location_id, dropoff_location_id, COUNT(*) AS trip_count, AVG(trip_distance) AS avg_trip_distance,
                   trip_distance_unit, AVG(total_amount) AS avg_total_amount, SUM(total_amount) AS sum_total_amount,
                   currency
              FROM parquet.`{parquet_path}`
          GROUP BY pickup_location_id, dropoff_location_id, trip_distance_unit, currency
            HAVING COUNT(*) > 100
          ORDER BY sum_total_amount DESC, trip_count DESC
             LIMIT 100
    """,

    "q3_hourly_demand_and_speed": """
            WITH base AS (
                SELECT HOUR(pickup_datetime) AS pickup_hour, trip_distance, trip_distance_unit, fare_amount, total_amount,
                       currency,
                       (unix_timestamp(dropoff_datetime) - unix_timestamp(pickup_datetime)) / 60.0 AS trip_duration_min
                  FROM parquet.`{parquet_path}`
                 WHERE trip_distance > 0
            )
            SELECT pickup_hour, COUNT(*) AS trip_count, AVG(trip_distance) AS avg_trip_distance, trip_distance_unit,
                   AVG(fare_amount) AS avg_fare_amount, AVG(total_amount) AS avg_total_amount, currency,
                   AVG(
                    CASE
                        WHEN trip_duration_min = 0 THEN NULL
                        ELSE trip_distance / (trip_duration_min / 60.0)
                    END) AS avg_speed_mph
             FROM base
         GROUP BY pickup_hour, trip_distance_unit, currency
         ORDER BY pickup_hour, trip_distance_unit, currency
    """
}

FULL_DATASET_SIZE_QUERY = "SELECT COUNT(*) FROM parquet.`{parquet_path}`"

# Build benchmark return for queries step
def build_return_dict_queries(
    spark_session: SparkSession,
    slice_name: str,
    compacted_parquet_path: str,
    operation: str,
    ntest: int,
    query_name: str,
    total_files: int,
    total_rows_in_dataset: int,
    rows_returned: int,
    execution_time: float,
    throughput: float,
    benchmark_start_timestamp: str,
    benchmark_end_timestamp: str,
    msg: str,
    dataframe_partitions_parquet: int,
    spark_metrics:  dict[str, Any] | None = None,
    spark_rest_metrics: dict[str, Any] | None = None,
    resource_stats: dict[str, Any] | None = None,
) -> dict[str, Any]:

    if resource_stats is None:
        resource_stats = ResourceMonitor.as_dict()

    if spark_metrics is None:
        spark_metrics = SparkMetricsCollector.as_dict()

    if spark_rest_metrics is None:
        spark_rest_metrics = SparkRestMetricsCollector.as_dict()

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
            "parquet_path": compacted_parquet_path,
            # Query name
            "query_name": query_name,
            # Performance
            "total_files": total_files,
            "total_rows_in_dataset": total_rows_in_dataset,
            "rows_returned": rows_returned,
            "execution_time_sec": execution_time,
            "throughput_rows_sec": throughput,
            "parquet_files_size_mib": compute_dataset_size_mib(spark_session = spark_session, path = compacted_parquet_path, suffix = ".parquet"),
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
            "msg":  msg,
            # Spark
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

# Get the total number of rows in a parquet lake using Spark SQL
def get_total_size_dataset(
    spark_session: SparkSession,
    parquet_path: str,
    logger: logging.Logger | None = None,
) -> tuple[int, bool]:

    if logger is None:
        logger = setup_logger()

    if "{parquet_path}" not in FULL_DATASET_SIZE_QUERY:
        msg = f"Query to get full dataset size does not contain '{{parquet_path}}': '{parquet_path}'"
        log_error_msg(logger, msg)        
        return 0, False

    try:
        query = FULL_DATASET_SIZE_QUERY.format(parquet_path = parquet_path)
    except Exception as e:
        msg = f"Error formatting query to get full dataset size: {type(e).__name__}: {e}"
        log_error_msg(logger, msg)
        return 0, False

    try:
        df_result = spark_session.sql(query)
        row = df_result.collect()[0]
        total_rows = int(row[0] or 0)  
    except Exception as e:
        msg = f"Query to get full dataset size could not be executed due to the following error: {type(e).__name__}: {e}"
        log_error_msg(logger, msg)
        return 0, False

    return total_rows, True

# Benchmark Spark SQL queries against a parquet dataset
# Parameters:
# spark_session: Spark Session
# slice_name: key used to resolve the parquet dataset path from PARQUET_PATHS
# query_name: key used to resolve the query template from SPARKSQL_QUERIES
# ntest: test number
# logger: Logger instance for reporting progress and errors
# Returns:
# benchmark results including parquet path, processed files, processed rows, execution time, throughput, timestamps, partitions, 
# resource statistics, spark metrics, a message, etc
def benchmark_sparksql_queries(
    spark_session: SparkSession,
    slice_name: str, 
    query_name: str, 
    ntest: int,
    logger: logging.Logger | None = None,
) -> dict[str, Any]:

    if logger is None:
        logger = setup_logger()


    timer = Timer()
    benchmark_start_timestamp = Timer.get_timestamp()
    MSG_ERROR = "Query processed with errors, check pipeline.log"

    query_template = SPARKSQL_QUERIES[query_name]
    parquet_folder = PARQUET_FOLDERS[slice_name]    

    total_files = 0
    total_rows = 0
    rows_returned = 0
    dataframe_partitions = 0

    total_files = len(list_files_fs(spark_session, parquet_folder))

    if total_files == 0:
        msg = f"No parquet file(s) were found for that path: {parquet_folder} OR {parquet_folder} does not exist"
        log_error_msg(logger, msg)        
        benchmark_end_timestamp = Timer.get_timestamp()
        return build_return_dict_queries(spark_session = spark_session, slice_name = slice_name, compacted_parquet_path = parquet_folder,
                                         operation = STANDARD, ntest = ntest, query_name = query_name, total_files = total_files,
                                         total_rows_in_dataset = 0, rows_returned = 0, execution_time = 0.0, throughput = 0.0,
                                         benchmark_start_timestamp =  benchmark_start_timestamp.isoformat(),
                                         benchmark_end_timestamp = benchmark_end_timestamp.isoformat(),
                                         msg = MSG_ERROR, dataframe_partitions_parquet = 0)


    if "{parquet_path}" not in query_template:
        msg = f"Query does not contain '{{parquet_path}}': '{parquet_folder}'"
        log_error_msg(logger, msg)        
        benchmark_end_timestamp = Timer.get_timestamp()
        return build_return_dict_queries(spark_session = spark_session, slice_name = slice_name, compacted_parquet_path = parquet_folder,
                                         operation = STANDARD, ntest = ntest, query_name = query_name, total_files = total_files,
                                         total_rows_in_dataset = 0, rows_returned = 0, execution_time = 0.0, throughput = 0.0,
                                         benchmark_start_timestamp =  benchmark_start_timestamp.isoformat(),
                                         benchmark_end_timestamp = benchmark_end_timestamp.isoformat(),
                                         msg = MSG_ERROR, dataframe_partitions_parquet = 0)

    try:
        query = query_template.format(parquet_path = parquet_folder)
        print(f"Query: \n {query}")
    except Exception as e:
        msg = f"Error formatting query {query_name}: {e}"
        log_error_msg(logger, msg)
        benchmark_end_timestamp = Timer.get_timestamp()
        return build_return_dict_queries(spark_session = spark_session, slice_name = slice_name, compacted_parquet_path = parquet_folder,
                                         operation = STANDARD, ntest = ntest, query_name = query_name, total_files = total_files,
                                         total_rows_in_dataset = 0, rows_returned = 0, execution_time = 0.0, throughput = 0.0,
                                         benchmark_start_timestamp =  benchmark_start_timestamp.isoformat(),
                                         benchmark_end_timestamp = benchmark_end_timestamp.isoformat(),
                                         msg = MSG_ERROR, dataframe_partitions_parquet = 0)

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

    try:
        df_result = spark_session.sql(query)
        rows_returned = count_df(df_result)        
    except Exception as e:
        msg = f"Query could not te executed due to the following error: {type(e).__name__}: {e}"
        log_error_msg(logger, msg)
        timer.stop("total")
        benchmark_end_timestamp = Timer.get_timestamp()
        resource_stats = monitor.stop()
        execution_time = timer.duration("total")
        after = collector.snapshot()
        metrics = collector.collect_between(before, after)
        rest_metrics = rest_collector.collect()
        sc.setLocalProperty("spark.jobGroup.id", None) 
        return build_return_dict_queries(spark_session = spark_session, slice_name = slice_name, compacted_parquet_path = parquet_folder,
                                         operation = STANDARD, ntest = ntest, query_name = query_name, total_files = total_files,
                                         total_rows_in_dataset = 0, rows_returned = rows_returned, execution_time = execution_time, throughput = 0.0,
                                         benchmark_start_timestamp =  benchmark_start_timestamp.isoformat(),
                                         benchmark_end_timestamp = benchmark_end_timestamp.isoformat(),
                                         msg = MSG_ERROR, dataframe_partitions_parquet = dataframe_partitions, spark_metrics = metrics,
                                         spark_rest_metrics = rest_metrics, resource_stats = resource_stats)
    timer.stop("total")
    benchmark_end_timestamp = Timer.get_timestamp()
    resource_stats = monitor.stop()
    execution_time = timer.duration("total")
    after = collector.snapshot()
    metrics = collector.collect_between(before, after)
    rest_metrics = rest_collector.collect()
    sc.setLocalProperty("spark.jobGroup.id", None) 
    
    dataframe_partitions = df_result.rdd.getNumPartitions()

    total_rows, query_succeded = get_total_size_dataset(spark_session = spark_session, parquet_path = parquet_folder, logger = logger)

    if not query_succeded:
        msg = MSG_ERROR
    else:
        msg = "Query was processed successfuly"

    throughput = total_rows / execution_time if execution_time > 0 else 0.0
    print(f"Spark executed the query {query_name} in {execution_time:.2f} seconds. Processed {total_rows} row(s) and returned {rows_returned} row(s)")
    print(f"Throughput: {throughput:.2f} rows/sec")

    return build_return_dict_queries(spark_session = spark_session, slice_name = slice_name, compacted_parquet_path = parquet_folder,
                                     operation = STANDARD, ntest = ntest, query_name = query_name, total_files = total_files,
                                     total_rows_in_dataset = total_rows, rows_returned = rows_returned, execution_time = execution_time, throughput = throughput,
                                     benchmark_start_timestamp =  benchmark_start_timestamp.isoformat(),
                                     benchmark_end_timestamp = benchmark_end_timestamp.isoformat(),
                                     msg = msg, dataframe_partitions_parquet = dataframe_partitions, spark_metrics = metrics,
                                     spark_rest_metrics = rest_metrics, resource_stats = resource_stats)

if __name__ == "__main__":
    # Create logger
    logger = setup_logger()
    spark_session = create_spark_session(logger)
    result = benchmark_sparksql_queries(spark_session = spark_session, slice_name = "4th-slice",
                                        query_name = "q2_most_valuable_routes", ntest = 8,
                                        logger = logger)
    spark_session.stop()
    log_info_msg(logger, result["msg"])
    print(result["msg"])
    timestamp = result['benchmark_start_timestamp'].replace(":", "-")
    save_result_to_csv_and_json(result, 
                                f"./queries/csv/queries_ce.csv",
                                f"./queries/json/queries_{result['test_number']}_{timestamp}.json",
                                build_csv_row_queries_spark)
    
