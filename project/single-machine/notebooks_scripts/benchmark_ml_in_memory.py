from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.preprocessing import StandardScaler
from pathlib import Path
import logging
from common import count_parquet_files, setup_logger, log_info_msg, log_error_msg, Timer, ResourceMonitor, IN_MEMORY, save_result_to_csv_and_json, PARQUET_FOLDERS, PARQUET_PATHS
from machine_learning import transform_ml_features, remove_negative_monetary_value_and_outliers_for_ml, build_return_dict_ml
import duckdb
from typing import Any

# Benchmark machine learning steps using linear regression to predict tip amount
# Parameters:
# slice_name: key used to resolve the parquet dataset path from PARQUET_PATHS
# ntest: test number
# threads: number of DuckDB execution threads
# logger: Logger instance for reporting progress and errors
# test_size: proportion of the dataset to allocate to the test set
# random_state: seed used to ensure reproducible train/test splits and model behaviour
# Returns:
# benchmark results including slice name, parquet path, total files, total rows in dataset, execution time,
# execution time training, execution time prediction, throughput, threads, timestamps, resource statistics, a message and
# quality metrics
def benchmark_predict_tip_amount(
    slice_name: str | Path, 
    ntest: int,
    threads: int = 0, 
    logger: logging.Logger | None = None, 
    test_size: float = 0.2, 
    random_state: int = 42
) -> dict[str, Any]:

    if logger is None:
        logger = setup_logger()

    timer = Timer()
    monitor = ResourceMonitor()
    benchmark_start_timestamp = Timer.get_timestamp()
    print(benchmark_start_timestamp.isoformat())

    timer.start("total")
    monitor.start()

    MSG_ERROR = "Machine learning algorithm completed with errors, check pipeline.log" 
    parquet_path = PARQUET_PATHS[slice_name]
    parquet_folder = PARQUET_FOLDERS[slice_name]

    total_files = count_parquet_files(parquet_path = parquet_path, use_pattern = True)
    
    if total_files == 0:
        msg = f"No parquet file(s) were found for path: {parquet_folder} OR {parquet_folder} does not exist"
        log_error_msg(logger, msg)
        timer.stop("total")
        resource_stats = monitor.stop()
        benchmark_end_timestamp = Timer.get_timestamp()
        execution_time = timer.duration("total")
        return build_return_dict_ml(slice_name = slice_name, parquet_path = parquet_folder, 
                                    operation = IN_MEMORY, ntest = ntest, total_files = total_files, total_rows_in_dataset = 0,
                                    execution_time = execution_time, execution_time_train = 0.0, execution_time_pred = 0.0, throughput = 0.0,
                                    total_rows_used_ml = 0, throughput_ml = 0.0,
                                    threads = threads, benchmark_start_timestamp = benchmark_start_timestamp.isoformat(), 
                                    benchmark_end_timestamp = benchmark_end_timestamp.isoformat(), msg = MSG_ERROR, mae = 0.0,
                                    rmse = 0.0, r2 = 0.0, resource_stats = resource_stats)

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
        return build_return_dict_ml(slice_name = slice_name, parquet_path = parquet_folder, 
                                    operation = IN_MEMORY, ntest = ntest, total_files = total_files, total_rows_in_dataset = 0,
                                    execution_time = execution_time, execution_time_train = 0.0, execution_time_pred = 0.0, throughput = 0.0,
                                    total_rows_used_ml = 0, throughput_ml = 0.0,
                                    threads = threads, benchmark_start_timestamp = benchmark_start_timestamp.isoformat(), 
                                    benchmark_end_timestamp = benchmark_end_timestamp.isoformat(), msg = MSG_ERROR, mae = 0.0,
                                    rmse = 0.0, r2 = 0.0, resource_stats = resource_stats)
    try:
        query = f"""
                 SELECT pickup_datetime, dropoff_datetime, trip_distance, pickup_location_id, 
                        dropoff_location_id, payment_type_id, fare_amount, tip_amount, 
                        year, month FROM read_parquet ('{parquet_path}')
                 """
        df_result = connection.execute(query).fetchdf()
        
    except Exception as e:
        msg = f"Query could not be executed due to the following error: {type(e).__name__}: {e}"
        log_error_msg(logger, msg)
        timer.stop("total")
        resource_stats = monitor.stop()
        benchmark_end_timestamp = Timer.get_timestamp()
        execution_time = timer.duration("total")
        return build_return_dict_ml(slice_name = slice_name, parquet_path = parquet_folder, 
                                    operation = IN_MEMORY, ntest = ntest, total_files = total_files, total_rows_in_dataset = 0,
                                    execution_time = execution_time, execution_time_train = 0.0, execution_time_pred = 0.0, throughput = 0.0,
                                    total_rows_used_ml = 0, throughput_ml = 0.0,
                                    threads = threads, benchmark_start_timestamp = benchmark_start_timestamp.isoformat(), 
                                    benchmark_end_timestamp = benchmark_end_timestamp.isoformat(), msg = MSG_ERROR, mae = 0.0,
                                    rmse = 0.0, r2 = 0.0, resource_stats = resource_stats)
          
    finally:
        connection.close()

    if df_result.empty:
        msg = f"No rows found in parquet path: {parquet_path}"
        log_error_msg(logger, msg)
        timer.stop("total")
        resource_stats = monitor.stop()
        benchmark_end_timestamp = Timer.get_timestamp()
        execution_time = timer.duration("total")
        return build_return_dict_ml(slice_name = slice_name, parquet_path = parquet_folder, 
                                    operation = IN_MEMORY, ntest = ntest, total_files = total_files, total_rows_in_dataset = 0,
                                    execution_time = execution_time, execution_time_train = 0.0, execution_time_pred = 0.0, throughput = 0.0,
                                    total_rows_used_ml = 0, throughput_ml = 0.0,
                                    threads = threads, benchmark_start_timestamp = benchmark_start_timestamp.isoformat(), 
                                    benchmark_end_timestamp = benchmark_end_timestamp.isoformat(), msg = MSG_ERROR, mae = 0.0,
                                    rmse = 0.0, r2 = 0.0, resource_stats = resource_stats)
        
    df_result = transform_ml_features(df_result)
    total_rows = len(df_result)
    target = "tip_amount"
    features = [c for c in df_result.columns if c != target]
    df_result = remove_negative_monetary_value_and_outliers_for_ml(df_result)
    df_result = df_result.dropna(subset = features + [target])
    total_rows_used_ml = len(df_result)

    if total_rows_used_ml == 0:
        log_error_msg(logger, "No valid rows remain after preprocessing")
        timer.stop("total")
        resource_stats = monitor.stop()
        benchmark_end_timestamp = Timer.get_timestamp()
        execution_time = timer.duration("total")
        throughput = total_rows / execution_time if execution_time > 0 else 0
        return build_return_dict_ml(slice_name = slice_name, parquet_path = parquet_folder, 
                                    operation = IN_MEMORY, ntest = ntest, total_files = total_files, total_rows_in_dataset = total_rows,
                                    execution_time = execution_time, execution_time_train = 0.0, execution_time_pred = 0.0, throughput = throughput,
                                    total_rows_used_ml = total_rows_used_ml, throughput_ml = 0.0,
                                    threads = threads, benchmark_start_timestamp = benchmark_start_timestamp.isoformat(), 
                                    benchmark_end_timestamp = benchmark_end_timestamp.isoformat(), msg = MSG_ERROR, mae = 0.0,
                                    rmse = 0.0, r2 = 0.0, resource_stats = resource_stats)
    
    X = df_result[features]
    y = df_result[target]

    # Split data
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size= test_size, random_state = random_state 
    )
 
    # Scaler
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    # Train benchmark
    timer.start("train")
    model = LinearRegression()
    model.fit(X_train_scaled, y_train)
    timer.stop("train")

    # Prediction benchmark
    timer.start("pred")
    y_pred = model.predict(X_test_scaled)
    timer.stop("pred")
    
    timer.stop("total")
    resource_stats = monitor.stop()
    benchmark_end_timestamp = Timer.get_timestamp()
    
    # Performance metrics
    execution_time = timer.duration("total")
    execution_time_train = timer.duration("train")
    execution_time_pred = timer.duration("pred")
    throughput = total_rows / execution_time if execution_time > 0 else 0.0
    throughput_ml = total_rows_used_ml / execution_time if execution_time > 0 else 0.0

    # Quality metrics
    mae = mean_absolute_error(y_test, y_pred)
    rmse = mean_squared_error(y_test, y_pred) ** 0.5
    r2 = r2_score(y_test, y_pred)

    print(f"Pandas and Scikit-learn run the training algorithm in {execution_time_train:.2f} seconds.")
    print(f"Pandas and Scikit-learn run the prediction algorithm in {execution_time_pred:.2f} seconds.")    
    print(f"Pandas and Scikit-learn run the machine learning algorithm in {execution_time:.2f} seconds. Processed {total_rows} row(s)")
    print(f"Throughput: {throughput:.2f} rows/sec")

    return build_return_dict_ml(slice_name = slice_name, parquet_path = parquet_folder, 
                                operation = IN_MEMORY, ntest = ntest, total_files = total_files, total_rows_in_dataset = total_rows,
                                execution_time = execution_time, execution_time_train = execution_time_train, execution_time_pred = execution_time_pred, 
                                total_rows_used_ml = total_rows_used_ml, throughput_ml = throughput_ml,
                                throughput = throughput, threads = threads, benchmark_start_timestamp = benchmark_start_timestamp.isoformat(), 
                                benchmark_end_timestamp = benchmark_end_timestamp.isoformat(), msg = "Machine learning algorithm completed", mae = mae,
                                rmse = rmse, r2 = r2, resource_stats = resource_stats)  

if __name__ == "__main__":
    # Create logger
    logger = setup_logger()
    result = benchmark_predict_tip_amount(slice_name = "2nd-slice", logger = logger, ntest = 7, threads = 3)

    log_info_msg(logger, result["msg"])
    print(result["msg"])
    timestamp = result['benchmark_start_timestamp'].replace(":", "-")
    save_result_to_csv_and_json(result, 
                                f"./ml/csv/ml_sm.csv",
                                f"./ml/json/ml_{result['test_number']}_{timestamp}.json")
