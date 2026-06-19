from functools import reduce
from pyspark.sql.types import (
    ByteType, ShortType, DoubleType, StringType, BooleanType, TimestampType
)
from pyspark import StorageLevel
from common import ResourceMonitor, is_empty, count_df, build_csv_row_cleaning_transformation_spark, CLEANING_TRANSFORMATION, test_session, save_result_to_csv_and_json_cleaning, create_spark_session, compute_dataset_size_mib, CSV_PATTERN, count_files_fs, STANDARD, Timer, SparkMetricsCollector, SparkRestMetricsCollector, setup_logger, log_error_msg, log_info_msg
from pyspark.sql import SparkSession, DataFrame, functions as F, Row
import uuid
from typing import Any
import logging
from pyspark.sql.functions import input_file_name

# Rename columns of a DataFrame according to a predefined mapping
def rename_columns(df: DataFrame) -> tuple[DataFrame, str]:

    COLUMN_RENAME_MAP = {
        "VendorID": "vendor_id",
        "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
        "passenger_count": "passenger_count",
        "trip_distance": "trip_distance",
        "RatecodeID": "rate_code_id",
        "store_and_fwd_flag": "store_and_forward_flag",
        "PULocationID": "pickup_location_id",
        "DOLocationID": "dropoff_location_id",
        "payment_type": "payment_type_id",
        "fare_amount": "fare_amount",
        "extra": "extra_charge",
        "mta_tax": "metered_rate_tax",
        "tip_amount": "tip_amount",
        "tolls_amount": "tolls_amount",
        "improvement_surcharge": "improvement_surcharge",
        "total_amount": "total_amount",
        "congestion_surcharge": "congestion_surcharge",
        "Airport_fee": "airport_fee",
        "cbd_congestion_fee": "cbd_congestion_fee",
        "year": "year",
        "month": "month",
        # New fields
        "currency": "currency", 
        "trip_distance_unit": "trip_distance_unit",
        "suspicious": "suspicious"
    }

    for old, new in COLUMN_RENAME_MAP.items():
        if old in df.columns:
            df = df.withColumnRenamed(old, new)
        
    return df, "Rename was performed successfully"


# Builds a set of column names from a DataFrame
def build_columns_schema(df: DataFrame) -> tuple[set[str], str]:
    cols = set(df.columns)
    return cols, "Schema was built successfully"

# Align a DataFrame to a predefined master schema and reports missing and unexpected columns.
# Adds missing columns with null values, drops unexpected columns and reorders columns to match
# the mater schema
# Parameters:
# df: Input DataFrame
# cols: set of column names present in the DataFrame
# Returns:
# aligned DataFrame and message describing missing and unexpected columns
def align_schema(
    df: DataFrame, 
    cols: set[str]
) -> tuple[DataFrame, str]:

    MASTER_SCHEMA = [
        "vendor_id",
        "pickup_datetime",
        "dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "rate_code_id",
        "store_and_forward_flag",
        "pickup_location_id",
        "dropoff_location_id",
        "payment_type_id",
        "fare_amount",
        "extra_charge",
        "metered_rate_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
        "airport_fee",
        "cbd_congestion_fee",
        # New fields
        "year",
        "month",
        "currency", 
        "trip_distance_unit",
        "suspicious"
    ]

    unexpected_cols = set()
    missing_cols = set()
    
    for c in cols:
        if c not in MASTER_SCHEMA:
            unexpected_cols.add(c)

    for c in MASTER_SCHEMA:
        if c not in cols:
            df = df.withColumn(c, F.lit(None))
            missing_cols.add(c)

    df = df.select(*MASTER_SCHEMA)

    if missing_cols:
        msg_missing_cols = f"Missing columns added as nulls: {sorted(missing_cols)}"
    else:
        msg_missing_cols = "No missing columns detected"

    if unexpected_cols:
        msg_unexpected_cols = f"Unexpected columns detected: {sorted(unexpected_cols)}"
    else:
        msg_unexpected_cols = "No unexpected columns detected"
        
    msg = f"{msg_missing_cols}\n{msg_unexpected_cols}"
    
    return df, msg

# Compare DataFrame column dtypes against the master schema
# Parameters:
# df: input DataFrame
# cols: set of column names present in the DataFrame
# Returns:
# status message containing the Dictionary of mismatches 
def get_type_mismatches(
    df: DataFrame, 
    cols: set[str]
) -> str:
    
    MASTER_SCHEMA_TYPES = {
        "vendor_id": ByteType,
        "pickup_datetime": TimestampType,
        "dropoff_datetime": TimestampType,
        "passenger_count": ByteType,
        "trip_distance": DoubleType,
        "rate_code_id": ByteType,
        "store_and_forward_flag": StringType,
        "pickup_location_id": ShortType,
        "dropoff_location_id": ShortType,
        "payment_type_id": ByteType,
        "fare_amount": DoubleType,
        "extra_charge": DoubleType,
        "metered_rate_tax": DoubleType,
        "tip_amount": DoubleType,
        "tolls_amount": DoubleType,
        "improvement_surcharge": DoubleType,
        "total_amount": DoubleType,
        "congestion_surcharge": DoubleType,
        "airport_fee": DoubleType,
        "cbd_congestion_fee": DoubleType,
        "year": ShortType,
        "month": ByteType,
        "currency": StringType,
        "trip_distance_unit": StringType,
        "suspicious": BooleanType
    }
    
    mismatches = {}

    actual_types = {f.name: type(f.dataType) for f in df.schema.fields}
    
    for col, expected_type in MASTER_SCHEMA_TYPES.items():
        if col in cols:
            actual_type = actual_types.get(col)
            if actual_type != expected_type:
                mismatches[col] = {
                    "expected": expected_type.__name__,
                    "actual": actual_type.__name__ if actual_type else None
                }

    if mismatches:
        msg = f"Type mismatches detected in {len(mismatches)} column(s): {mismatches}"
    else:
        msg = "No type mismatches detected"

    return msg

# Enforce the master schema for Yellow Taxi data by cleaning,
# and casting columns to their expected types
# Parameters:
# df: input DataFrame
# cols: set of column names present in the DataFrame
# Returns:
# DataFrame with enforced schema and status message
def enforce_master_schema(
    df: DataFrame, 
    cols: set[str]
) -> tuple[DataFrame, str]:

    # Integer columns
    for c in ["vendor_id", "passenger_count", "rate_code_id", "payment_type_id", "month"]:
        if c in cols:
            df = df.withColumn(c, F.col(c).cast("byte"))  

    # Greater Integer columns
    for c in ["pickup_location_id", "dropoff_location_id", "year"]:
        if c in cols:
            df = df.withColumn(c, F.col(c).cast("short")) 

    # Float columns
    for c in ["trip_distance", "fare_amount", "extra_charge", "metered_rate_tax", "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount", 
              "congestion_surcharge", "airport_fee", "cbd_congestion_fee"]:
        if c in cols:
            df = df.withColumn(c, F.col(c).cast("double")) 

    # Datetime columns
    for c in ["pickup_datetime", "dropoff_datetime"]:
        if c in cols:
            df = df.withColumn(c, F.to_timestamp(F.col(c))) 

    # Categorical columns
    if "store_and_forward_flag" in cols:
        df = df.withColumn("store_and_forward_flag", F.upper(F.trim(F.col("store_and_forward_flag"))))
        df = df.withColumn("store_and_forward_flag", 
                         F.when(
                             F.col("store_and_forward_flag").isin("Y", "N"),
                             F.col("store_and_forward_flag")
                         ).otherwise(F.lit(None).cast("string"))
            )
        
    if "currency" in cols:
        df = df.withColumn("currency", F.col("currency").cast("string"))

    if "trip_distance_unit" in cols:
        df = df.withColumn("trip_distance_unit", F.col("trip_distance_unit").cast("string")) 

    if "suspicious" in cols:
        df = df.withColumn("suspicious", F.col("suspicious").cast("boolean"))

    return df, "Master schema enforcement completed"

# Applies the master schema to a DataFrame by:
# - renaming columns
# - building and logging schema before alignment
# - aligning columns to the master schema
# - checking type mismatches before enforcement
# - enforcing master schema
# - checking type mismatches after enforcement
def apply_master_schema(
    df: DataFrame, 
    logger: logging.Logger | None = None
) -> DataFrame:
    
    if logger is None:
        logger = setup_logger()

    # Rename the columns
    df, msg = rename_columns(df)
    log_info_msg(logger, msg)
    
    # schema before alignment
    original_cols, msg = build_columns_schema(df)
    
    log_info_msg(logger, f"Schema before alignment:\n{msg}")
    
    # align columns
    df, msg = align_schema(df, original_cols)

    log_info_msg(logger, msg)
    
    # schema after alignment
    aligned_cols, msg = build_columns_schema(df)

    log_info_msg(logger, f"Schema after alignment:\n{msg}")
    
    # check types before enforcement
    msg = get_type_mismatches(df, aligned_cols)

    log_info_msg(logger, f"Type mismatches before enforcement:\n{msg}")
    
    # enforce schema
    df, msg = enforce_master_schema(df, aligned_cols)

    log_info_msg(logger, msg)
    
    # check types after enforcement
    msg = get_type_mismatches(df, aligned_cols)

    log_info_msg(logger, f"Type mismatches after enforcement:\n{msg}")

    return df

# Missing value imputation transformations for selected columns:
# - tip_amount -> 0
# - passenger_count -> 1
# - store_and_forward_flag -> "UNKNOWN"
def impute_missing_values(
    df: DataFrame
) -> DataFrame:

    cols = set(df.columns)

    if "tip_amount" in cols:
        df = df.withColumn(
            "imputed_tip_amount",
            F.col("tip_amount").isNull().cast("int")
        )
        df = df.withColumn(
            "tip_amount",
            F.coalesce(F.col("tip_amount"), F.lit(0.0).cast("float"))
        )

    if "passenger_count" in cols:
        df = df.withColumn(
            "imputed_passenger_count",
            F.col("passenger_count").isNull().cast("int")
        )
        df = df.withColumn(
            "passenger_count",
            F.coalesce(F.col("passenger_count"), F.lit(1).cast("short"))
        )        
                          
    if "store_and_forward_flag" in cols:
        df = df.withColumn(
            "imputed_store_and_forward_flag",
            F.col("store_and_forward_flag").isNull().cast("int")
        )
        df = df.withColumn(
            "store_and_forward_flag",
            F.coalesce(F.col("store_and_forward_flag"), F.lit("UNKNOWN"))
        )
        
    return df

# Missing value imputation transformations for total amount:
# A row is imputed when:
# 1. total_amount is missing (NaN) and
# 2. at least one of the individual monetary fields is present.
def impute_missing_total_amount(
    df: DataFrame
) -> DataFrame:

    AMOUNT_COLS = ["fare_amount", "extra_charge", "metered_rate_tax", "tip_amount", "tolls_amount",                    
                     "improvement_surcharge", "congestion_surcharge", "airport_fee",               
                     "cbd_congestion_fee"]

    # at least one amount column is not null
    any_amount_present = reduce(
        lambda a, b: a | b,
        [F.col(c).isNotNull() for c in AMOUNT_COLS]
    )

    fill_mask = (
        F.col("total_amount").isNull() &
        any_amount_present
    )

    expected_total = reduce(
        lambda a, b: a + b,
        [F.coalesce(F.col(c), F.lit(0.0)) for c in AMOUNT_COLS]
    )

    df = df.withColumn(
        "imputed_total_amount",
        F.col("total_amount").isNull().cast("int")
    )
    
    df = df.withColumn(
        "total_amount",
        F.when(fill_mask, expected_total).otherwise(F.col("total_amount"))
    )
    
    return df

# Filter to identify and separate rows where all columns are missing
# Parameters:
# df: input DataFrame
# Returns:
# filtered DataFrame containing only valid row(s), filtered DataFrame containing invalid row(s) 
def remove_rows_all_columns_missing(
    df: DataFrame
) -> tuple[DataFrame, DataFrame]:

    null_expression = [F.col(c).isNull() for c in df.columns]
    invalid_mask = reduce(lambda a, b: a & b, null_expression)
    
    df_removed_rows = df.filter(invalid_mask)
    df_valid = df.filter(~invalid_mask)

    return df_valid, df_removed_rows

# Filter to identify and separete rows where all specified critical columns are missing
# Parameters:
# df: input DataFrame
# critical_cols: columns considered critical. 
# Returns:
# filtered DataFrame containing oly valid row(s), filtered DataFrame containing invalid row(s) 
# and a message with an error in case critical columns list is empty
# or empty if the function was executed successfully
def remove_rows_with_missing_critical_columns(
    df: DataFrame, 
    critical_cols: list[str] | None = None,
) -> tuple[DataFrame, DataFrame, str]:
    
    if critical_cols is None:
        return df, df.limit(0), "No critical columns provided, no rows will be removed"

    null_expression = [F.col(c).isNull() for c in critical_cols]
    invalid_mask = reduce(lambda a, b: a & b, null_expression)

    df_removed_rows = df.filter(invalid_mask)
    df_valid = df.filter(~invalid_mask)

    return df_valid, df_removed_rows, ""

# Filter to identify and separate rows with invalid timestamps (Spark DataFrame version)
# A row is considered valid if:
# - pickup_datetime and dropoff_datetime are not null
# - pickup_datetime < dropoff_dateimte
# Parameters:
# df: input DataFrame
# Returns:
# filtered DataFrame containing only valid rows, filtered DataFrame containing rows with invalid or null timestamps
def remove_invalid_timestamps(
    df: DataFrame
) -> tuple[DataFrame, DataFrame]:


    valid_mask = (
        F.col("pickup_datetime").isNotNull() &
        F.col("dropoff_datetime").isNotNull() &
        (F.col("pickup_datetime") < F.col("dropoff_datetime"))
    )

    df_removed_rows = df.filter(~valid_mask)
    df_valid = df.filter(valid_mask)
        
    return df_valid, df_removed_rows

# Filter to identify and separate row that has a year and/or month which are not equal to the year and/or month
# in the filename
# Checking that pickup_datetime matches the single year-month pair declared in the row
# A row is removed when:
# - pickup_datetime.year != year 
# - pickup_datetime.month != month 
# - pickup_datetime, year, month is missing 
# Parameters:
# df: input DataFrame
# Returns:
# filtered DataFrame containing only valid rows, leaked DataFrame containing rows with invalid years and/or months
# and status message describing the filtering operation
def validate_year_month_leakage(
    df: DataFrame
) -> tuple[DataFrame, DataFrame]:

    valid_mask = (
        F.col("pickup_datetime").isNotNull() &
        F.col("year").isNotNull() &
        F.col("month").isNotNull() &
        (F.year(F.col("pickup_datetime")) == F.col("year")) &
        (F.month(F.col("pickup_datetime")) == F.col("month"))
    )
    
    df_leaked_rows = df.filter(~valid_mask)
    df_valid = df.filter(valid_mask)

    return df_valid, df_leaked_rows

#  filter to identify and separate rows with negative trip_distance values (Spark DataFrame version)
def remove_negative_trip_distance(
    df: DataFrame
) -> tuple[DataFrame, DataFrame]:

    valid_mask = (F.col("trip_distance") >= 0) | (F.col("trip_distance").isNull())

    df_removed_rows = df.filter(~valid_mask)
    df_valid = df.filter(valid_mask)
    
    return df_valid, df_removed_rows

# filter to identify and separate rows with negative passenger_count values (Spark DataFrame version)
def remove_unrealistic_passenger_count(
    df: DataFrame
) -> tuple[DataFrame, DataFrame]:

    valid_mask = (F.col("passenger_count") >= 0) | (F.col("passenger_count").isNull())

    df_removed_rows = df.filter(~valid_mask)
    df_valid = df.filter(valid_mask)

    return df_valid, df_removed_rows

# This function evaluates several conditions to identify rows where monetary
# values appear inconsistent or incomplete. A row is flagged as suspicious if:
# 1. The total_amount differs from the sum of all individual monetary fields/columns
#    by more than the specified tolerance
# 2. All monetary fields/columns are missing 
# 3. The total_amount field/column is missing
# 4. The tip_amount field/column is missing
# Parameters:
# df: input DataFrame
# tolerance: tolerance to compare with the difference between total amount and all the other fields//columns
# Returns:
# Original DataFrame containing the new column suspicious flag, filtered DataFrame containing suspicious rows 
# and status message describing the filtering operation
def add_suspicious_flag_amount_values_currency(
    df: DataFrame, 
    tolerance: float = 2.0
) -> tuple[DataFrame, DataFrame]:

    def is_missing(col_name: str):
        return F.col(col_name).isNull() | F.isnan(F.col(col_name))

    def zero_if_missing(col_name: str):
        return F.when(is_missing(col_name), F.lit(0.0)).otherwise(F.col(col_name))

    AMOUNT_COLS = ["fare_amount", "extra_charge", "metered_rate_tax", "tip_amount", "tolls_amount",                    
                    "improvement_surcharge", "congestion_surcharge", "airport_fee",               
                    "cbd_congestion_fee"]

    # expected total, treat nulls as 0
    expected_total = reduce(
        lambda a, b: a + b,
        [zero_if_missing(c) for c in AMOUNT_COLS]
    )    

    diff = F.abs(F.col("total_amount") - expected_total)

    inconsistent_total_amount_mask = (
        (~is_missing("total_amount")) &
        (diff > F.lit(tolerance))
    )

    # All monetary fields null
    all_null_mask = reduce(
        lambda a, b: a & b,
        [is_missing(c) for c in AMOUNT_COLS]
    )

    # Missing total_amount
    missing_total_amount_mask = F.col("total_amount").isNull()

    # Missing tip amount and payment type is card
    missing_tip_mask = (
        (F.col("payment_type_id") == 1) &
        is_missing("tip_amount")
    )

    suspicious_mask = (
        inconsistent_total_amount_mask |
        all_null_mask |
        missing_total_amount_mask |
        missing_tip_mask
    )

    df = df.withColumn("suspicious", suspicious_mask)

    df_suspicious_rows = df.filter(F.col("suspicious") == True)

    return df, df_suspicious_rows

# filter to identify and separate rows with trips with unrealistic speed values (Spark DataFrame version)
# Parameters:
# df: input DataFrame
# max_speed_mph: maximum allowed speed in mph
# Results:
# filtered DataFrame containing only valid rows, filtered DataFrame containing rows with unrealistic speed values
# and status message describing the filtering operation
def remove_unrealistic_trip_speed(
    df: DataFrame, 
    max_speed_mph: float = 120.0
) -> tuple[DataFrame, DataFrame]:

    # Trip duration in minutes
    df_with_metrics = df.withColumn(
        "trip_duration_min",
        (F.col("dropoff_datetime").cast("long") - F.col("pickup_datetime").cast("long")) / F.lit(60.0)  
    )

    # Average speed in mph, only when duration > 0
    df_with_metrics = df_with_metrics.withColumn(
        "avg_speed_mph",
        F.when(
            F.col("trip_duration_min") > 0,
            F.floor(F.col("trip_distance") / (F.col("trip_duration_min") / F.lit(60.0)))
        ).otherwise(F.lit(None).cast("double"))
    )

    valid_mask = (
        F.col("avg_speed_mph").isNull() |
        (F.col("avg_speed_mph") < F.lit(max_speed_mph))
    )
    
    df_removed_rows = df_with_metrics.filter(~valid_mask)
    df_valid = df_with_metrics.filter(valid_mask).drop("trip_duration_min", "avg_speed_mph")

    return df_valid, df_removed_rows

# Profiles by computing missing value count and percentage per column
# and returning a DataFrame (only transformations) sorted in descending order of the number of missing values
def profile_missing_values(
    df: DataFrame
) -> DataFrame:

    total_rows_expr = F.count(F.lit(1))

    agg_expressions = [total_rows_expr.alias("total_rows")]

    for c in df.columns:
        agg_expressions.append(
            F.sum(F.col(c).isNull().cast("long")).alias(c)
        )

    df_columns = df.agg(*agg_expressions)

    df_result = None

    for c in df.columns:
        df_col = df_columns.select(
            F.lit(c).alias("column_name"),
            F.col(c).cast("long").alias("missing_count"),
            F.round((F.col(c) / F.col("total_rows")) * 100, 2).alias("missing_percent")
        )
        df_result = df_col if df_result is None else df_result.unionByName(df_col)

    return df_result.orderBy(F.col("missing_count").desc())

# Profiles vendor-specific differences by computing descriptive statistics
# (count, mean, median, std, min, max) for numeric fields grouped by vendor_id.
def profile_vendor_specific_differences(
    df: DataFrame
) -> DataFrame:
    
    cols = ["trip_distance", "fare_amount", "tip_amount", "total_amount"]

    agg_expressions = []

    for c in cols:
        agg_expressions.extend([
            F.count(F.col(c)).alias(f"{c}_count"),
            F.mean(F.col(c)).alias(f"{c}_mean"),
            F.percentile_approx(F.col(c), 0.5).alias(f"{c}_median"),
            F.stddev(F.col(c)).alias(f"{c}_stddev"),
            F.min(F.col(c)).alias(f"{c}_min"),
            F.max(F.col(c)).alias(f"{c}_max")
        ])

    return df.groupby("vendor_id").agg(*agg_expressions)

# Count removed rows to trigger an action and processing the DAG
def count_removed_rows_and_optionally_write(
    df_removed_rows: DataFrame,
    output_path: str,
    header: bool,
    review_msg: str, 
    logger: logging.Logger | None = None, 
) -> int:

    if logger is None:
        logger = setup_logger()
    
    removed = count_df(df_removed_rows)

    if removed > 0:
        log_info_msg(logger, review_msg)
        df_removed_rows.write.mode("append").option("header", header).csv(output_path)
    
    return removed

# Count suspicious rows, optionally sample, and write to CSV folder if non-empty
# Returns exact suspicious row count
def count_suspicious_rows_and_optionally_write(
    df_suspicious: DataFrame,
    output_path: str,
    header: bool,
    logger: logging.Logger | None = None, 
) -> int:

    if logger is None:
        logger = setup_logger()

    MAX_SUSPICIOUS_ROWS = 50
    
    suspicious = count_df(df_suspicious)

    if suspicious > 0:

        if suspicious > MAX_SUSPICIOUS_ROWS:
            fraction = MAX_SUSPICIOUS_ROWS / suspicious
            df_to_write = df_suspicious.sample(withReplacement = False, fraction = fraction, seed = 42)
            log_info_msg(
                logger,
                f"{MAX_SUSPICIOUS_ROWS} suspicious rows were chosen using random sampling to be saved in the file: {output_path}"
            )

        else:
            df_to_write = df_suspicious
            log_info_msg(logger, f"All {suspicious} suspicious rows were saved in the file: {output_path}")

        df_to_write.write.mode("append").option("header", header).csv(output_path)
    
    return suspicious
    
# Take a list of imputation flag columns and sum each flag column
# and return a Python dictionary with the counts
def aggregate_imputation_flags(
    df: DataFrame,
    flag_cols: list[str]
) -> dict[str, int]:

    existing_flag_cols = [c for c in df.columns if c in flag_cols]

    if not existing_flag_cols:
        return {}

    agg_expressions = [
        F.sum(F.coalesce(F.col(c), F.lit(0))).cast("long").alias(c)
        for c in existing_flag_cols
    ]

    row = df.agg(*agg_expressions).collect()[0]
    
    return {c: int(row[c] or 0) for c in existing_flag_cols}

# Drop columns from a Spark DataFrame lazily
def drop_columns_df(
    df: DataFrame,
    cols_to_drop: list[str]
) -> DataFrame:

    existing_cols = [c for c in cols_to_drop if c in df.columns]
    return df.drop(*existing_cols)

# Applies the full cleaning and transformation pipeline to a slice of data.
# The pipeline sequentially:
# - removes rows with all columns missing
# - removes rows wit all critical columns missing
# - removes invalid timestamps
# - removes year/month leakage
# - removes negative trip distances
# - removes unrealistic passenger counts
# - removes unrealistic trip speeds
# - flags suspicious monetary rows and save a sample
# - imputes selected missing values
# - imputes missing total amount values
# Removed rows are written to CSV files under removed_slice_path
# Processing stops early if the DataFrame becomes empty
# Parameters:
# df: input DataFrame
# removed_slice_path: directory where removed-rows and suspicious-rows will be saved
# logger: Logger instance for reporting progress and errors
# header: if to write CSV headers when appending output to files
# Returns:
# cleaned DataFrame (only transformations)
def cleaning_transformation(
    df: DataFrame, 
    removed_slice_path: str, 
    logger: logging.Logger | None = None, 
    header: bool = False
) -> DataFrame:
    
    if logger is None:
        logger = setup_logger()

    removed_slice_path = removed_slice_path.rstrip("/")
    msg = ""
    
    # Remove rows with all column missing
    df, df_removed_rows = remove_rows_all_columns_missing(df)

    removed = count_removed_rows_and_optionally_write(
        df_removed_rows = df_removed_rows, output_path = f"{removed_slice_path}/rows_all_columns_missing", 
        header = header, review_msg = f"Review file: {removed_slice_path}/rows_all_columns_missing to check the removed rows",
        logger = logger
    )
    if removed == 0:
        msg = "0 rows were dropped because no row had all columns null"
    elif removed == 1:
        msg = "1 row was dropped because all its columns were null"
    else: 
        msg = f"{removed} were dropped because all their columns were null" 

    log_info_msg(logger, msg)

    if is_empty(df):
        return df
    
    # Remove rows with all critical columns missing
    df, df_removed_rows, msg = remove_rows_with_missing_critical_columns(df, ["pickup_datetime", "dropoff_datetime", "trip_distance", "pickup_location_id",  
                                                      "dropoff_location_id", "fare_amount"])

    if msg:
        log_error_msg(logger, msg)

    removed = count_removed_rows_and_optionally_write(
        df_removed_rows = df_removed_rows, output_path = f"{removed_slice_path}/rows_missing_critical_columns",
        header = header, review_msg = f"Review file: {removed_slice_path}/rows_missing_critical_columns to check the removed rows",
        logger = logger
    )

    if removed == 0:
        msg = "No rows were dropped, because all rows had at least one critical field present"
    elif removed == 1:
        msg = "1 row was dropped because all critical fields were missing"
    else:
        msg = f"{removed} rows were dropped because all critical fields were missing"

    log_info_msg(logger, msg)

    if is_empty(df):
        return df

    # Remove rows with invalid timestamps
    df, df_removed_rows = remove_invalid_timestamps(df)
    
    removed = count_removed_rows_and_optionally_write(
        df_removed_rows = df_removed_rows, output_path = f"{removed_slice_path}/invalid_timestamps",
        header = header, review_msg = f"Review file: {removed_slice_path}/invalid_timestamps to check the removed rows",
        logger = logger
    )

    if removed == 0:
        msg = "No invalid timestamps were found"
    elif removed == 1:
        msg = "1 row had invalid timestamp(s)"
    else:
        msg = f"{removed} rows had invalid timestamps"

    log_info_msg(logger, msg)

    if is_empty(df):
        return df

    # Validate year/month leakage and remove rows
    df, df_removed_rows = validate_year_month_leakage(df)

    leaked_count = count_removed_rows_and_optionally_write(
        df_removed_rows = df_removed_rows, output_path = f"{removed_slice_path}/month_leaked_rows",
        header = header, review_msg = f"Review file: {removed_slice_path}/month_leaked_rows to check the removed rows",
        logger = logger
    )

    if leaked_count == 0:
        msg = "No leaked rows were detected"
    elif leaked_count == 1:
        msg = "1 leaked row was detected"
    else:
        msg = f"{leaked_count} leaked rows were detected"

    log_info_msg(logger, msg)

    if is_empty(df):
        return df

    # Remove rows with negative trip distance
    df, df_removed_rows = remove_negative_trip_distance(df)

    removed = count_removed_rows_and_optionally_write(
        df_removed_rows = df_removed_rows, output_path = f"{removed_slice_path}/negative_trip_distance",
        header = header, review_msg = f"Review file: {removed_slice_path}/negative_trip_distance to check the removed rows",
        logger = logger
    )

    if removed == 0:
        msg = "No rows were removed due to negative trip distance"
    elif removed == 1:
        msg = "1 row was removed because it had a negative trip distance"
    else:
        msg = f"{removed} rows were removed because they had negative trip distances"

    log_info_msg(logger, msg)

    if is_empty(df):
        return df

    # Remove rows with unrealistic passenger count
    df, df_removed_rows = remove_unrealistic_passenger_count(df)
    
    removed = count_removed_rows_and_optionally_write(
        df_removed_rows = df_removed_rows, output_path = f"{removed_slice_path}/unrealistic_passenger_count",
        header = header, review_msg = f"Review file: {removed_slice_path}/unrealistic_passenger_count to check the removed rows",
        logger = logger
    )

    if removed == 0:
        msg = "No rows were removed due to negative passenger count"
    elif removed == 1:
        msg = "1 row was removed because it had a negative passenger count"
    else:
        msg = f"{removed} rows were removed because they had negative passenger counts"

    log_info_msg(logger, msg)

    if is_empty(df):
        return df

    # Remove rows with unrealistic trip speed
    df, df_removed_rows = remove_unrealistic_trip_speed(df)

    removed = count_removed_rows_and_optionally_write(
        df_removed_rows = df_removed_rows, output_path = f"{removed_slice_path}/unrealistic_trip_speed",
        header = header, review_msg = f"Review file: {removed_slice_path}/unrealistic_trip_speed to check the removed rows",
        logger = logger
    )

    if removed == 0:
        msg = "No rows were removed due to unrealistic trip speeds"
    elif removed == 1:
        msg = "1 row was removed due to unrealistic trip speed"
    else:
        msg = f"{removed} rows were removed due to unrealistic trip speeds"

    log_info_msg(logger, msg)

    if is_empty(df):
        return df

    # Add suspicious flag and save sample if needed
    df, df_suspicious = add_suspicious_flag_amount_values_currency(df)

    suspicious = count_suspicious_rows_and_optionally_write(
            df_suspicious = df_suspicious, output_path = f"{removed_slice_path}/suspicious_rows(NO_REMOVED_ROWS)",
            header = header, logger = logger
    )

    if suspicious == 0:
        msg = "No suspicious monetary rows were flagged"
    elif suspicious == 1:
        msg = "1 suspicious row was flagged as suspicious"
    else:
        msg = f"{suspicious} rows were flagged as suspicious"
    
    log_info_msg(logger, msg)

    if is_empty(df):
        return df
    
    # Impute missing values
    df = impute_missing_values(df)

    imputation_counts = aggregate_imputation_flags(df, ["imputed_tip_amount", "imputed_passenger_count", "imputed_store_and_forward_flag"])

    tip_missing = imputation_counts.get("imputed_tip_amount", 0)
    passenger_missing = imputation_counts.get("imputed_passenger_count", 0)
    store_and_forward_flag_missing = imputation_counts.get("imputed_store_and_forward_flag", 0)

    msgs = []

    if tip_missing == 1:
        msgs.append("1 missing tip_amount value was imputed with 0")
    elif tip_missing > 1:
        msgs.append(f"{tip_missing} missing tip_amount values were imputed with 0")

    if passenger_missing == 1:
        msgs.append("1 missing passenger_count value was imputed with 1")
    elif passenger_missing > 1:
        msgs.append(f"{passenger_missing} missing passenger_count values were imputed with 1")

    if store_and_forward_flag_missing == 1:
        msgs.append("1 missing store_and_forward_flag value was imputed with UNKNOWN")
    elif store_and_forward_flag_missing > 1:
        msgs.append(f"{store_and_forward_flag_missing} missing store_and_forward_flag values were imputed with UNKNOWN")

    if not msgs:
        msg = "No missing values were imputed"
    else:
        msg = ", ".join(msgs)

    log_info_msg(logger, msg)

    df = impute_missing_total_amount(df)

    total_amount_counts = aggregate_imputation_flags(df, ["imputed_total_amount"])
    total_amount_missing = total_amount_counts.get("imputed_total_amount", 0)

    if total_amount_missing == 0:
        msg = "No total amount values were imputed"
    elif total_amount_missing == 1:
        msg = "1 total amount value was imputed summing all the monetary values"
    else:
        msg = f"{total_amount_missing} total amount values were imputed summing all the currency values"

    log_info_msg(logger, msg)

    df = drop_columns_df(df, ["imputed_tip_amount", "imputed_passenger_count", "imputed_store_and_forward_flag", "imputed_total_amount"])
    
    return df

# Build benchmark return for cleaning and transformation step
def build_return_dict_cleaning_transformation(
    spark_session: SparkSession,
    slice_path: str,
    operation: str,
    ntest: int, 
    processed_files: int,
    processed_rows: int,
    rows_after_cleaning: int,
    execution_time: float,
    throughput: float,
    benchmark_start_timestamp: str,
    benchmark_end_timestamp: str,
    msg: str,
    summary: list[Row] | None,
    missing_values: list[Row] | None,
    dataframe_partitions: int,
    spark_metrics:  dict[str, Any],
    spark_rest_metrics: dict[str, Any],
    resource_stats: dict[str, Any] | None = None,
) -> dict[str, Any]:
    
    if resource_stats is None:
        resource_stats = ResourceMonitor.as_dict()

    return {
        # operations:
        "stage": CLEANING_TRANSFORMATION,
        # Type of operation
        "operation": operation,
        # Test Number
        "test_number": ntest,   
        # Slice path
        "slice_path": slice_path,
        # Performance
        "processed_files": processed_files,
        "processed_rows": processed_rows,
        "rows_after_cleaning": rows_after_cleaning,
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
        # Profiling
        "summary": summary,
        "missing_values": missing_values,
        # Spark Metrics
        "dataframe_partitions": dataframe_partitions,
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
# spark_session: spark session 
# slice_path: path to the directory containing CSV files for the slice
# logger: Logger instance for reporting progress and errors.
# ntest: test number
# Returns:
# benchmark results including slice path, processed files, processed rows, execution time, throughput, timestamps, 
# resource statistics, spark metrics and a message
def benchmark_spark_cleaning_transformation(
    spark_session: SparkSession,
    slice_path: str, 
    removed_slice_path: str, 
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
    MSG_ERROR = "Ingestion, cleaning, transformation and profiling finished with errors, check pipeline.log"
    processed_rows = 0
    processed_files = 0
    dataframe_partitions = 0
    rows_after_cleaning = 0
    
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
            
            return build_return_dict_cleaning_transformation(slice_path = slice_path, operation = STANDARD, ntest = ntest, processed_files = processed_files, processed_rows = processed_rows, 
                                                             execution_time = execution_time, throughput = 0.0, benchmark_start_timestamp =  benchmark_start_timestamp.isoformat(),
                                                             benchmark_end_timestamp = benchmark_end_timestamp.isoformat(), resource_stats = resource_stats,
                                                             msg = MSG_ERROR, spark_session = spark_session, spark_metrics = metrics,
                                                             spark_rest_metrics = rest_metrics, dataframe_partitions = dataframe_partitions,
                                                             summary = None, missing_values = None, rows_after_cleaning = rows_after_cleaning)

            
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
            
            return build_return_dict_cleaning_transformation(slice_path = slice_path, operation = STANDARD, ntest = ntest, processed_files = processed_files, processed_rows = processed_rows, 
                                                             execution_time = execution_time, throughput = throughput, benchmark_start_timestamp =  benchmark_start_timestamp.isoformat(),
                                                             benchmark_end_timestamp = benchmark_end_timestamp.isoformat(), resource_stats = resource_stats,
                                                             msg = MSG_ERROR, spark_session = spark_session, spark_metrics = metrics,
                                                             spark_rest_metrics = rest_metrics, dataframe_partitions = dataframe_partitions,
                                                             summary = None, missing_values = None, rows_after_cleaning = rows_after_cleaning)

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
            rows_after_cleaning = count_df(df_cleaned)
            summary = profile_vendor_specific_differences(df_cleaned).collect()
            missing_values = profile_missing_values(df_cleaned).collect()
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
        
        return build_return_dict_cleaning_transformation(slice_path = slice_path, operation = STANDARD, ntest = ntest, processed_files = processed_files, processed_rows = processed_rows, 
                                                         execution_time = execution_time, throughput = throughput, benchmark_start_timestamp =  benchmark_start_timestamp.isoformat(),
                                                         benchmark_end_timestamp = benchmark_end_timestamp.isoformat(), resource_stats = resource_stats,
                                                         msg = MSG_ERROR, spark_session = spark_session, spark_metrics = metrics,
                                                         spark_rest_metrics = rest_metrics, dataframe_partitions = dataframe_partitions,
                                                         summary = None, missing_values = None, rows_after_cleaning = rows_after_cleaning)

                
    timer.stop("total")
    benchmark_end_timestamp = Timer.get_timestamp()
    resource_stats = monitor.stop()
    

    execution_time = timer.duration("total")
    throughput = processed_rows / execution_time if execution_time > 0 else 0
    after = collector.snapshot()
    metrics = collector.collect_between(before, after)
    rest_metrics = rest_collector.collect()
    sc.setLocalProperty("spark.jobGroup.id", None) 

    
    print(f"Spark cleaned and transformed {processed_files} file(s) and {processed_rows} row(s) in {execution_time:.2f} seconds")
    print(f"Throughput: {throughput:.2f} rows/sec")
    

    return build_return_dict_cleaning_transformation(slice_path = slice_path, operation = STANDARD, ntest = ntest, processed_files = processed_files, processed_rows = processed_rows, 
                                                     execution_time = execution_time, throughput = throughput, benchmark_start_timestamp =  benchmark_start_timestamp.isoformat(),
                                                     benchmark_end_timestamp = benchmark_end_timestamp.isoformat(), resource_stats = resource_stats,
                                                     msg = "Ingestion, cleaning, transformation and profiling finished successfully", spark_session = spark_session, spark_metrics = metrics,
                                                     spark_rest_metrics = rest_metrics, dataframe_partitions = dataframe_partitions,
                                                     summary = summary, missing_values = missing_values, rows_after_cleaning = rows_after_cleaning)

if __name__ == "__main__":
    # Create logger
    logger = setup_logger()
    spark_session = create_spark_session(logger)
    result = benchmark_spark_cleaning_transformation(spark_session = spark_session, 
                                                     slice_path = "hdfs:///data/CSV/raw-data/3rd-slice/", 
                                                     ntest = 3,
                                                     logger = logger,
                                                     removed_slice_path = "hdfs:///data/CSV/removed/3rd-slice/")
    spark_session.stop()
    log_info_msg(logger, result["msg"])
    print(result["msg"])
    timestamp = result['benchmark_start_timestamp'].replace(":", "-")
    save_result_to_csv_and_json_cleaning(result, 
                                         f"./cleaning/csv/cleaning_ce.csv",
                                	     f"./cleaning/json/cleaning_{result['test_number']}_{timestamp}.json",
                                         build_csv_row_cleaning_transformation_spark)
