from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import SparkSession, DataFrame, functions as F
from common import ResourceMonitor, PARQUET_FOLDERS, is_empty, count_df, build_csv_row_ml_spark, list_files_fs, MACHINE_LEARNING, test_session, save_result_to_csv_and_json, create_spark_session, compute_dataset_size_mib, STANDARD, Timer, SparkMetricsCollector, SparkRestMetricsCollector, setup_logger, log_error_msg, log_info_msg
import logging
from typing import Any
import uuid
from pyspark import StorageLevel
from pyspark.sql.types import DoubleType

# Transform features and creates new features based on pickup_datetime and
# dropoff_datetime
def transform_ml_features(
    df: DataFrame
) -> DataFrame:

    df = (
        df
        # pickup_datetime
	.withColumn("pickup_datetime", F.col("pickup_datetime").cast("timestamp"))
	.withColumn("dropoff_datetime", F.col("dropoff_datetime").cast("timestamp"))
        .withColumn("pickup_hour", F.hour("pickup_datetime").cast("byte"))
        .withColumn("pickup_day", F.dayofmonth("pickup_datetime").cast("byte"))
        .withColumn("pickup_weekday", F.dayofweek("pickup_datetime").cast("byte"))
        # dropoff_datetime
        .withColumn("dropoff_hour", F.hour("dropoff_datetime").cast("byte"))
        .withColumn("dropoff_day", F.dayofmonth("dropoff_datetime").cast("byte"))
        .withColumn("dropoff_weekday", F.dayofweek("dropoff_datetime").cast("byte"))
        # Trip duration sec
        .withColumn(
            "trip_duration_sec",
            (F.col("dropoff_datetime").cast("long") - F.col("pickup_datetime").cast("long")).cast("long")
        )
        # Trip speed
        .withColumn(
            "trip_speed",
            F.when(
                F.col("trip_duration_sec") > 0,
                F.col("trip_distance") / (F.col("trip_duration_sec") / F.lit(3600.0))
            ).otherwise(F.lit(None).cast("double"))
        )
        .drop("pickup_datetime", "dropoff_datetime")
    )

    return df

# Remove negative monetary values and outliers for ml
def remove_negative_monetary_value_and_outliers_for_ml(
    df: DataFrame
) -> DataFrame:

    return df.filter(
        (F.col("fare_amount") >= 0) &
        (F.col("tip_amount") >= 0) &
        (F.col("fare_amount") < 500) &
        (F.col("tip_amount") < 200)
    )

# Build benchmark return for machine learning step
def build_return_dict_ml(
    spark_session: SparkSession,
    slice_name: str,
    parquet_path: str,
    operation: str,
    ntest: int,
    total_files: int,
    total_rows_in_dataset: int,
    total_rows_used_ml: int,
    execution_time: float,
    execution_time_train: float,
    execution_time_pred: float,
    throughput: float,
    throughput_ml: float, 
    benchmark_start_timestamp: str,
    benchmark_end_timestamp: str,
    msg: str,
    mae: float,
    rmse: float,
    r2: float,
    dataframe_partitions_parquet: int,
    spark_metrics:  dict[str, Any],
    spark_rest_metrics: dict[str, Any],
    resource_stats: dict[str, Any] | None = None,
) -> dict[str, Any]:

    if resource_stats is None:
        resource_stats = ResourceMonitor.as_dict()

    return {
            # operations:
            "stage": MACHINE_LEARNING,
            # Type of operation
            "operation": operation,
            # Test Number
            "test_number": ntest, 
            #Slice name
            "slice_name": slice_name,
            # Slice path
            "parquet_path": parquet_path,
            # Performance
            "total_files": total_files,
            "total_rows_in_dataset": total_rows_in_dataset,
            "total_rows_used_ml": total_rows_used_ml,
            "execution_time_sec": execution_time,
            "throughput_total_rows_sec": throughput,
            "throughput_rows_used_ml_sec": throughput_ml,
            "execution_time_sec_train": execution_time_train,
            "execution_time_sec_pred": execution_time_pred,
            "parquet_files_size_mib": compute_dataset_size_mib(spark_session = spark_session, path = parquet_path, suffix = ".parquet"),
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
            # Quality metrics
            "mae": mae,
            "rmse": rmse,
            "r2": r2,
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

# Benchmark machine learning steps using linear regression from Spark MLlib to predict tip amount
# Parameters:
# spark_session: Spark Session
# slice_name: key used to resolve the parquet dataset path from PARQUET_PATHS
# ntest: test number
# logger: Logger instance for reporting progress and errors
# test_size: proportion of the dataset to allocate to the test set
# random_state: seed used to ensure reproducible train/test splits and model behaviour
# Returns:
# benchmark results including parquet path, processed files, processed rows, execution time, throughput, timestamps, partitions, 
# resource statistics, spark metrics, a message, quality metrics, etc.
def benchmark_predict_tip_amount(
    spark_session: SparkSession,
    slice_name: str,
    ntest: int,
    logger: logging.Logger | None = None,
    test_size: float = 0.2,
    random_state: int = 42,
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

    MSG_ERROR = "Machine learning algorithm completed with errors, check pipeline.log" 
    parquet_folder = PARQUET_FOLDERS[slice_name]

    total_files = len(list_files_fs(spark_session, parquet_folder))

    if total_files == 0:
        msg = f"No parquet file(s) were found for path: {parquet_folder} OR {parquet_folder} does not exist"
        log_error_msg(logger, msg)
        timer.stop("total")
        benchmark_end_timestamp = Timer.get_timestamp()
        resource_stats = monitor.stop()
        execution_time = timer.duration("total")
        after = collector.snapshot()
        metrics = collector.collect_between(before, after)
        rest_metrics = rest_collector.collect()
        sc.setLocalProperty("spark.jobGroup.id", None) 
        return build_return_dict_ml(spark_session = spark_session, slice_name = slice_name, parquet_path = parquet_folder,
                                    operation = STANDARD, ntest = ntest, total_files = total_files, total_rows_in_dataset = 0,
                                    total_rows_used_ml = 0, execution_time = execution_time, execution_time_train = 0.0,
                                    execution_time_pred = 0.0, throughput = 0.0, throughput_ml = 0.0, 
                                    benchmark_start_timestamp = benchmark_start_timestamp.isoformat(),
                                    benchmark_end_timestamp = benchmark_end_timestamp.isoformat(),
                                    msg = MSG_ERROR, mae = 0.0, rmse = 0.0, r2 = 0.0, dataframe_partitions_parquet = 0,
                                    spark_metrics = metrics, spark_rest_metrics = rest_metrics,
                                    resource_stats = resource_stats)

    total_rows = 0
    total_rows_used_ml = 0
    dataframe_partitions_parquet = 0
    execution_time_train = 0.0
    execution_time_pred = 0.0
    mae = 0.0
    rmse = 0.0
    r2 = 0.0
    
    try:
        df_result = spark_session.read.parquet(parquet_folder).select("pickup_datetime", "dropoff_datetime", "trip_distance", "pickup_location_id", 
                                                                    "dropoff_location_id", "payment_type_id", "fare_amount", "tip_amount", "year", 
                                                                    "month")

        if is_empty(df_result):
            msg = f"No rows found in parquet path: {parquet_folder}"
            log_error_msg(logger, msg)
            timer.stop("total")
            benchmark_end_timestamp = Timer.get_timestamp()
            resource_stats = monitor.stop()
            execution_time = timer.duration("total")
            after = collector.snapshot()
            metrics = collector.collect_between(before, after)
            rest_metrics = rest_collector.collect()
            sc.setLocalProperty("spark.jobGroup.id", None) 
            return build_return_dict_ml(spark_session = spark_session, slice_name = slice_name, parquet_path = parquet_folder,
                                        operation = STANDARD, ntest = ntest, total_files = total_files, total_rows_in_dataset = total_rows,
                                        total_rows_used_ml = total_rows_used_ml, execution_time = execution_time, execution_time_train = 0.0,
                                        execution_time_pred = 0.0, throughput = 0.0, throughput_ml = 0.0, 
                                        benchmark_start_timestamp = benchmark_start_timestamp.isoformat(),
                                        benchmark_end_timestamp = benchmark_end_timestamp.isoformat(),
                                        msg = MSG_ERROR, mae = 0.0, rmse = 0.0, r2 = 0.0, dataframe_partitions_parquet = dataframe_partitions_parquet,
                                        spark_metrics = metrics, spark_rest_metrics = rest_metrics,
                                        resource_stats = resource_stats)
        
        df_result = df_result.persist(StorageLevel.DISK_ONLY)
        total_rows = count_df(df_result)
        dataframe_partitions_parquet = df_result.rdd.getNumPartitions()
        
        df_ml = transform_ml_features(df_result)
        df_ml = remove_negative_monetary_value_and_outliers_for_ml(df_ml)

        target = "tip_amount"

        features = [c for c in df_ml.columns if c != target]

        # Remove rows with target null
        condition = None
        allowed_cols = set(features + [target])

        for field in df_ml.schema.fields:
            if field.name not in allowed_cols:
                continue

            if isinstance(field.dataType, DoubleType):
                c_cond = F.col(field.name).isNotNull() & (~F.isnan(F.col(field.name)))
            else:
                c_cond = F.col(field.name).isNotNull()
        
            condition = c_cond if condition is None else (condition & c_cond)
            
        df_ml = df_ml.filter(condition)

        total_rows_used_ml = count_df(df_ml)

        if total_rows_used_ml == 0:
            log_error_msg(logger, "No valid rows remain after preprocessing")
            timer.stop("total")
            benchmark_end_timestamp = Timer.get_timestamp()
            resource_stats = monitor.stop()
            execution_time = timer.duration("total")
            after = collector.snapshot()
            metrics = collector.collect_between(before, after)
            rest_metrics = rest_collector.collect()
            sc.setLocalProperty("spark.jobGroup.id", None) 
            throughput = total_rows / execution_time if execution_time > 0 else 0.0
            return build_return_dict_ml(spark_session = spark_session, slice_name = slice_name, parquet_path = parquet_folder,
                                        operation = STANDARD, ntest = ntest, total_files = total_files, total_rows_in_dataset = total_rows,
                                        total_rows_used_ml = total_rows_used_ml, execution_time = execution_time, execution_time_train = 0.0,
                                        execution_time_pred = 0.0, throughput = throughput, throughput_ml = 0.0, 
                                        benchmark_start_timestamp = benchmark_start_timestamp.isoformat(),
                                        benchmark_end_timestamp = benchmark_end_timestamp.isoformat(),
                                        msg = MSG_ERROR, mae = 0.0, rmse = 0.0, r2 = 0.0, dataframe_partitions_parquet = dataframe_partitions_parquet,
                                        spark_metrics = metrics, spark_rest_metrics = rest_metrics,
                                        resource_stats = resource_stats)
        
        assembler = VectorAssembler(
            inputCols = features,
            outputCol = "features_raw",
            handleInvalid = "error"
        )

        assembled_df = assembler.transform(df_ml).select(
            F.col(target).alias("label"),
            "features_raw"
        )

        train_df, test_df = assembled_df.randomSplit(
            [1.0 - test_size, test_size],
            seed = random_state
        )

        scaler = StandardScaler(
            inputCol = "features_raw",
            outputCol = "features",
            withMean = True,
            withStd = True
        )

        scaler_model = scaler.fit(train_df)
        # select clean ML format, drop features_raw and the original columns
        train_scaled = scaler_model.transform(train_df).select("label", "features")
        test_scaled = scaler_model.transform(test_df).select("label", "features")

        # Model and train benchmark
        timer.start("train")
        lr_model = LinearRegression(
                        featuresCol = "features",
                        labelCol = "label",
                        predictionCol = "prediction"
                    )

        lr_model = lr_model.fit(train_scaled)
        timer.stop("train")

        # Prediction benchmark
        timer.start("pred")
        predictions = lr_model.transform(test_scaled)
        _ = predictions.count()
        timer.stop("pred")

        timer.stop("total")
        benchmark_end_timestamp = Timer.get_timestamp()
        resource_stats = monitor.stop()
        execution_time = timer.duration("total")
        execution_time_train = timer.duration("train")
        execution_time_pred = timer.duration("pred")
        after = collector.snapshot()
        metrics = collector.collect_between(before, after)
        rest_metrics = rest_collector.collect()
        sc.setLocalProperty("spark.jobGroup.id", None) 


        
        # Quality metrics
        mae_evaluator = RegressionEvaluator(
            labelCol = "label",
            predictionCol = "prediction",
            metricName = "mae"
        )

        rmse_evaluator = RegressionEvaluator(
            labelCol = "label",
            predictionCol = "prediction",
            metricName = "rmse"
        )

        r2_evaluator = RegressionEvaluator(
            labelCol = "label",
            predictionCol = "prediction",
            metricName = "r2"
        )
        
        mae = mae_evaluator.evaluate(predictions)
        rmse = rmse_evaluator.evaluate(predictions)
        r2 = r2_evaluator.evaluate(predictions)

        df_result.unpersist(blocking = True)
        
    except Exception as e:
        msg = f"Spark ML algorithm failed: {type(e).__name__}: {e}"
        log_error_msg(logger, msg)
        timer.stop("total")
        benchmark_end_timestamp = Timer.get_timestamp()
        resource_stats = monitor.stop()
        execution_time = timer.duration("total")
        after = collector.snapshot()
        metrics = collector.collect_between(before, after)
        rest_metrics = rest_collector.collect()
        sc.setLocalProperty("spark.jobGroup.id", None) 
        throughput = total_rows / execution_time if execution_time > 0 else 0.0
        throughput_ml = total_rows_used_ml / execution_time if execution_time > 0 else 0.0
        return build_return_dict_ml(spark_session = spark_session, slice_name = slice_name, parquet_path = parquet_folder,
                                    operation = STANDARD, ntest = ntest, total_files = total_files, total_rows_in_dataset = total_rows,
                                    total_rows_used_ml = total_rows_used_ml, execution_time = execution_time, execution_time_train = execution_time_train,
                                    execution_time_pred = execution_time_pred, throughput = throughput, throughput_ml = throughput_ml, 
                                    benchmark_start_timestamp = benchmark_start_timestamp.isoformat(),
                                    benchmark_end_timestamp = benchmark_end_timestamp.isoformat(),
                                    msg = MSG_ERROR, mae = mae, rmse = rmse, r2 = r2, dataframe_partitions_parquet = dataframe_partitions_parquet,
                                    spark_metrics = metrics, spark_rest_metrics = rest_metrics,
                                    resource_stats = resource_stats)        



    throughput = total_rows / execution_time if execution_time > 0 else 0.0
    throughput_ml = total_rows_used_ml / execution_time if execution_time > 0 else 0.0

    print(f"Spark and Spark MLlib run the training algorithm in {execution_time_train:.2f} seconds.")
    print(f"Spark and Spark MLlib run the prediction algorithm in {execution_time_pred:.2f} seconds.")    
    print(f"Spark and Spark MLlib run the machine learning algorithm in {execution_time:.2f} seconds. Processed {total_rows} row(s)")
    print("The reported training and prediction stage timings do not represent strictly isolated"
          "execution times. Due to Spark’s lazy evaluation model, transformations are only executed upon an action," 
          "which may trigger computation from preceding stages. Consequently, some operations may be attributed across stages," 
          "and the measured timings may be either over- or under-estimated. These values are therefore indicative rather than exact.")
    print(f"Throughput: {throughput:.2f} rows/sec")

    return build_return_dict_ml(spark_session = spark_session, slice_name = slice_name, parquet_path = parquet_folder,
                                operation = STANDARD, ntest = ntest, total_files = total_files, total_rows_in_dataset = total_rows,
                                total_rows_used_ml = total_rows_used_ml, execution_time = execution_time, execution_time_train = execution_time_train,
                                execution_time_pred = execution_time_pred, throughput = throughput, throughput_ml = throughput_ml, 
                                benchmark_start_timestamp = benchmark_start_timestamp.isoformat(),
                                benchmark_end_timestamp = benchmark_end_timestamp.isoformat(),
                                msg = "Machine learning algorithm completed", mae = mae, rmse = rmse, r2 = r2, 
                                dataframe_partitions_parquet = dataframe_partitions_parquet, spark_metrics = metrics, spark_rest_metrics = rest_metrics,
                                resource_stats = resource_stats)      

if __name__ == "__main__":
    # Create logger
    logger = setup_logger()
    spark_session = create_spark_session(logger)
    result = benchmark_predict_tip_amount(spark_session = spark_session, slice_name = "4th-slice",
                                          ntest = 4, logger = logger)

    spark_session.stop()
    log_info_msg(logger, result["msg"])
    print(result["msg"])
    timestamp = result['benchmark_start_timestamp'].replace(":", "-")
    save_result_to_csv_and_json(result, 
                                f"./ml/csv/ml_ce.csv",
                                f"./ml/json/ml_{result['test_number']}_{timestamp}.json",
                                build_csv_row_ml_spark)
