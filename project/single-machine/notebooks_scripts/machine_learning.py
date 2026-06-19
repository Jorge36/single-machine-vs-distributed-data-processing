import pandas as pd
from pathlib import Path
from typing import Any
from common import ResourceMonitor, MACHINE_LEARNING, compute_dataset_size_mib

# Transform features and creates new features based on pickup_datetime and dropoff_datetime
def transform_ml_features(
    df: pd.DataFrame
) -> pd.DataFrame:

    df = df.copy()

    # Pickup datetime
    df["pickup_hour"] = df["pickup_datetime"].dt.hour.astype("UInt8")
    df["pickup_day"] = df["pickup_datetime"].dt.day.astype("UInt8")
    df["pickup_weekday"] = df["pickup_datetime"].dt.weekday.astype("UInt8")
            
    # Dropoff datetime
    df["dropoff_hour"] = df["dropoff_datetime"].dt.hour.astype("UInt8")
    df["dropoff_day"] = df["dropoff_datetime"].dt.day.astype("UInt8")
    df["dropoff_weekday"] = df["dropoff_datetime"].dt.weekday.astype("UInt8")
            
    # Trip duration sec
    df["trip_duration_sec"] = (df["dropoff_datetime"] - df["pickup_datetime"]).dt.total_seconds().astype("UInt32")

    # Trip speed
    df["trip_speed"] = df["trip_distance"] / (df["trip_duration_sec"] / 3600)

    # Drop no numerical columns
    df = df.drop(columns = ["pickup_datetime", "dropoff_datetime"])

    return df

# Remove negative monetary values and outliers for ml
def remove_negative_monetary_value_and_outliers_for_ml(
    df: pd.DataFrame
) -> pd.DataFrame:

    mask = (
        df["fare_amount"].ge(0) &
        df["tip_amount"].ge(0) &
        df["fare_amount"].lt(500) & # remove outliers
        df["tip_amount"].lt(200)
    )

    return df[mask].copy()

# Build benchmark return for machine learning step
def build_return_dict_ml(
    slice_name: str,
    operation: str,
    parquet_path: str | Path,
    ntest: int,
    total_files: int,
    total_rows_in_dataset: int,
    total_rows_used_ml: int,
    execution_time: float,
    execution_time_train: float,
    execution_time_pred: float,
    throughput: float,
    throughput_ml: float, 
    threads: int,
    benchmark_start_timestamp: str,
    benchmark_end_timestamp: str,
    msg: str,
    mae: float,
    rmse: float,
    r2: float,
    chunksize: int = 0,
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
            "parquet_path": str(parquet_path),
            # Performance
            "total_files": total_files,
            "total_rows_in_dataset": total_rows_in_dataset,
            "total_rows_used_ml": total_rows_used_ml,
            "execution_time_sec": execution_time,
            "throughput_total_rows_sec": throughput,
            "throughput_rows_used_ml_sec": throughput_ml,
            "execution_time_sec_train": execution_time_train,
            "execution_time_sec_pred": execution_time_pred,
            "threads": threads,
            "chunksize": chunksize,
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
            # Quality metrics
            "mae": mae,
            "rmse": rmse,
            "r2": r2,
            # Message
            "msg":  msg
    }
