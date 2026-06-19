from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import SGDRegressor
import numpy as np
from pathlib import Path
import logging
from common import count_parquet_files, setup_logger, log_info_msg, log_error_msg, Timer, ResourceMonitor, CHUNKING, save_result_to_csv_and_json, PARQUET_FOLDERS, PARQUET_PATHS
from machine_learning import transform_ml_features, remove_negative_monetary_value_and_outliers_for_ml, build_return_dict_ml
import duckdb
import gc
from typing import Any

# Benchmark machine learning steps using incremental learning with SGDRRegressor
# Parameters:
# slice_name: key used to resolve the parquet dataset path from PARQUET_PATHS
# ntest: test number
# approx_rows_per_chunk: approximate number of rows to process per chunk during incremental learning
# threads: number of DuckDB execution threads
# logger: Logger instance for reporting progress and errors
# test_size: proportion of the dataset to allocate to the test set
# random_state: seed used to ensure reproducible train/test splits and model behaviour
# Returns:
# benchmark results including slice name, parquet path, total files, total rows in dataset, execution time,
# execution time training, execution time prediction, throughput, threads, timestamps, resource statistics, a message and
# quality metrics
def benchmark_predict_tip_amount_incremental_learning(
    slice_name: str | Path, 
    ntest: int,
    approx_rows_per_chunk: int = 100_000, 
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
    timer.start("train")
    monitor.start()

    MSG_ERROR = "Machine learning algorithm completed with errors, check pipeline.log" 
    
    parquet_path = PARQUET_PATHS[slice_name]
    parquet_folder = PARQUET_FOLDERS[slice_name]
    DUCKDB_VECTOR_SIZE = 2048
    vector_multiple = max(1, approx_rows_per_chunk // DUCKDB_VECTOR_SIZE)
    chunksize = vector_multiple * DUCKDB_VECTOR_SIZE

    total_files = count_parquet_files(parquet_path,  use_pattern = True)
    
    if total_files == 0:
        msg = f"No parquet file(s) were found for path: {parquet_folder} OR {parquet_folder} does not exist"
        log_error_msg(logger, msg)
        timer.stop("train")
        timer.stop("total")
        resource_stats = monitor.stop()
        benchmark_end_timestamp = Timer.get_timestamp()
        execution_time_train = timer.duration("train")
        execution_time = timer.duration("total")
        return build_return_dict_ml(slice_name = slice_name, parquet_path = parquet_folder, 
                                    operation = CHUNKING, ntest = ntest, total_files = total_files, total_rows_in_dataset = 0,
                                    execution_time = execution_time, execution_time_train = execution_time_train, execution_time_pred = 0.0, throughput = 0.0,
                                    total_rows_used_ml = 0, throughput_ml = 0.0,
                                    threads = threads, benchmark_start_timestamp = benchmark_start_timestamp.isoformat(), 
                                    benchmark_end_timestamp = benchmark_end_timestamp.isoformat(), msg = MSG_ERROR, mae = 0.0,
                                    chunksize = chunksize, rmse = 0.0, r2 = 0.0, 
                                    resource_stats = resource_stats)

    connection = None
    
    try:
        connection = duckdb.connect()
        if threads and threads > 0:
            connection.execute(f"PRAGMA threads={threads}")
    except duckdb.Error as e:
        msg = f"Failed to initialize DuckDB connection (threads = {threads}): {type(e).__name__}: {e}"
        log_error_msg(logger, msg)
        if connection is not None:
            connection.close()
        timer.stop("train")
        timer.stop("total")
        resource_stats = monitor.stop()
        benchmark_end_timestamp = Timer.get_timestamp()
        execution_time_train = timer.duration("train")
        execution_time = timer.duration("total")
        return build_return_dict_ml(slice_name = slice_name, parquet_path = parquet_folder, 
                                    operation = CHUNKING, ntest = ntest, total_files = total_files, total_rows_in_dataset = 0,
                                    execution_time = execution_time, execution_time_train = execution_time_train, execution_time_pred = 0.0, throughput = 0.0,
                                    total_rows_used_ml = 0, throughput_ml = 0.0,
                                    threads = threads, benchmark_start_timestamp = benchmark_start_timestamp.isoformat(), 
                                    benchmark_end_timestamp = benchmark_end_timestamp.isoformat(), msg = MSG_ERROR, mae = 0.0,
                                    chunksize = chunksize, rmse = 0.0, r2 = 0.0, 
                                    resource_stats = resource_stats)

    total_rows = 0
    total_rows_used_ml = 0
    X_test_scaled_parts = []
    y_test_parts = []

    try:
        
        query = f"""
                 SELECT pickup_datetime, dropoff_datetime, trip_distance, pickup_location_id, 
                        dropoff_location_id, payment_type_id, fare_amount, tip_amount,
                        year, month FROM read_parquet ('{parquet_path}')
                 """

        df_result = connection.execute(query)
        model = SGDRegressor(random_state = random_state)
        scaler = StandardScaler()
    
        target = "tip_amount"

        chunk_counter = 0

        while True:
            
            chunk = df_result.fetch_df_chunk(vector_multiple)
            
            if chunk is None or chunk.empty:
                break

            chunk_counter += 1
            total_rows += len(chunk) 

            chunk = transform_ml_features(chunk)
            features = [c for c in chunk.columns if c != target]
            chunk = remove_negative_monetary_value_and_outliers_for_ml(chunk)
            chunk = chunk.dropna(subset = features + [target])
            
            if chunk.empty:
                del X
                del y
                del X_train
                del X_test
                del y_train
                del y_test
                del chunk
                continue

            total_rows_used_ml += len(chunk)
            
            X = chunk[features]
            y = chunk[target]    

            X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = test_size, random_state = random_state)

            if len(X_train) == 0:
                continue
            
            scaler.partial_fit(X_train)

            X_train_scaled = scaler.transform(X_train)
            X_test_scaled = scaler.transform(X_test)

            model.partial_fit(X_train_scaled, y_train)

            if len(X_test_scaled) > 0:
                X_test_scaled_parts.append(X_test_scaled)
                y_test_parts.append(y_test.to_numpy())

            del X
            del y
            del X_train
            del X_test
            del y_train
            del y_test
            del chunk
            del X_train_scaled
            del X_test_scaled

            if chunk_counter % 20 == 0:
                gc.collect()

        del df_result

    except Exception as e:
        msg = f"Training step failed: {type(e).__name__}: {e}"
        log_error_msg(logger, msg)
        timer.stop("train")
        timer.stop("total")
        resource_stats = monitor.stop()
        benchmark_end_timestamp = Timer.get_timestamp()
        execution_time_train = timer.duration("train")
        execution_time = timer.duration("total")
        throughput = total_rows / execution_time if execution_time > 0 else 0.0
        throughput_ml = total_rows_used_ml / execution_time if execution_time > 0 else 0.0
        return build_return_dict_ml(slice_name = slice_name, parquet_path = parquet_folder, 
                                    operation = CHUNKING, ntest = ntest, total_files = total_files, total_rows_in_dataset = 0,
                                    execution_time = execution_time, execution_time_train = execution_time_train, execution_time_pred = 0.0, throughput = throughput,
                                    total_rows_used_ml = total_rows_used_ml, throughput_ml = throughput_ml,
                                    threads = threads, benchmark_start_timestamp = benchmark_start_timestamp.isoformat(), 
                                    benchmark_end_timestamp = benchmark_end_timestamp.isoformat(), msg = MSG_ERROR, mae = 0.0,
                                    chunksize = chunksize, rmse = 0.0, r2 = 0.0, 
                                    resource_stats = resource_stats)
            
    finally:
        connection.close()

    timer.stop("train")

    # Prediction benchmark
    if not X_test_scaled_parts or not y_test_parts:
        log_error_msg(logger, "Prediction step could be not performed: no test data collected")
        timer.stop("total")
        resource_stats = monitor.stop()
        benchmark_end_timestamp = Timer.get_timestamp()
        execution_time_train = timer.duration("train")
        execution_time = timer.duration("total")
        throughput = total_rows / execution_time if execution_time > 0 else 0.0
        throughput_ml = total_rows_used_ml / execution_time if execution_time > 0 else 0.0
        return build_return_dict_ml(slice_name = slice_name, parquet_path = parquet_folder, 
                                    operation = CHUNKING, ntest = ntest, total_files = total_files, total_rows_in_dataset = 0,
                                    execution_time = execution_time, execution_time_train = execution_time_train, execution_time_pred = 0.0, throughput = throughput,
                                    total_rows_used_ml = total_rows_used_ml, throughput_ml = throughput_ml,
                                    threads = threads, benchmark_start_timestamp = benchmark_start_timestamp.isoformat(), 
                                    benchmark_end_timestamp = benchmark_end_timestamp.isoformat(), msg = MSG_ERROR, mae = 0.0,
                                    chunksize = chunksize, rmse = 0.0, r2 = 0.0, 
                                    resource_stats = resource_stats)
         
    timer.start("pred")
    X_test_final = np.vstack(X_test_scaled_parts)
    y_test_final = np.concatenate(y_test_parts)
    y_pred = model.predict(X_test_final)
    timer.stop("pred")
    timer.stop("total")
    resource_stats = monitor.stop()
    benchmark_end_timestamp = Timer.get_timestamp()
    
    # Performance metrics
    execution_time_train = timer.duration("train")
    execution_time_pred = timer.duration("pred")
    execution_time = timer.duration("total")  
    throughput = total_rows / execution_time if execution_time > 0 else 0.0
    throughput_ml = total_rows_used_ml / execution_time if execution_time > 0 else 0.0
    
    # Quality metrics
    mae = mean_absolute_error(y_test_final, y_pred)
    rmse = mean_squared_error(y_test_final, y_pred) ** 0.5
    r2 = r2_score(y_test_final, y_pred)
    
    print(f"Pandas and Scikit-learn run the training algorithm in {execution_time_train:.2f} seconds.")
    print(f"Pandas and Scikit-learn run the prediction algorithm in {execution_time_pred:.2f} seconds.")    
    print(f"Pandas and Scikit-learn run the machine learning algorithm in {execution_time:.2f} seconds. Processed {total_rows} row(s)")
    print(f"Throughput: {throughput:.2f} rows/sec")
    
    return build_return_dict_ml(slice_name = slice_name, parquet_path = parquet_folder, 
                                operation = CHUNKING, ntest = ntest, total_files = total_files, total_rows_in_dataset = total_rows,
                                execution_time = execution_time, execution_time_train = execution_time_train, execution_time_pred = execution_time_pred, 
                                throughput = throughput, total_rows_used_ml = total_rows_used_ml, throughput_ml = throughput_ml,
                                threads = threads, benchmark_start_timestamp = benchmark_start_timestamp.isoformat(), 
                                benchmark_end_timestamp = benchmark_end_timestamp.isoformat(), msg = "Machine learning algorithm completed", mae = mae,
                                chunksize = chunksize, rmse = rmse, r2 = r2, 
                                resource_stats = resource_stats)
if __name__ == "__main__":
    # Create logger
    logger = setup_logger()
    result = benchmark_predict_tip_amount_incremental_learning(slice_name = "4th-slice", 
                                                            logger = logger, 
                                                            ntest = 19, 
                                                            approx_rows_per_chunk = 25_000, 
                                                            threads = 0)

    log_info_msg(logger, result["msg"])
    print(result["msg"])
    timestamp = result['benchmark_start_timestamp'].replace(":", "-")
    save_result_to_csv_and_json(result, 
                                f"./ml/csv/ml_sm.csv",
                                f"./ml/json/ml_{result['test_number']}_{timestamp}.json")
