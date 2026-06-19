import pandas as pd
from pathlib import Path
from common import setup_logger, log_info_msg, IN_MEMORY, CHUNKING, is_empty
from typing import Any, Literal
import logging
import re
from common import compute_dataset_size_mib, ResourceMonitor, CLEANING_TRANSFORMATION

# Extract year and month from a filename using the pattern YYYY-MM
def extract_year_month_from_filename(
    filename: str | Path
) -> tuple[int | None, int | None, str]:
    filename = str(filename)
    msg = ""
    match = re.search(r"(\d{4})-(\d{2})", filename)
    if not match:
        msg = f"Could not extract year-month from filename: {filename}"
        return None, None, msg
        
    return int(match.group(1)), int(match.group(2)), msg

# Rename columns of a DataFrame according to a predefined mapping
def rename_columns(df: pd.DataFrame) -> tuple[pd.DataFrame, str]:

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
    
    df = df.rename(columns = COLUMN_RENAME_MAP)
    
    return df, "Rename was performed successfully"

# Builds a set of column names from a DataFrame
def build_columns_schema(df: pd.DataFrame) -> tuple[set[str], str]:
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
    df: pd.DataFrame, 
    cols: set[str]
) -> tuple[pd.DataFrame, str]:

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
            df[c] = pd.NA
            missing_cols.add(c)

    df = df[MASTER_SCHEMA].copy()

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
    df: pd.DataFrame, 
    cols: set[str]
) -> str:
    
    MASTER_SCHEMA_TYPES = {
        "vendor_id": "UInt8",
        "pickup_datetime": "datetime64[ns]",
        "dropoff_datetime": "datetime64[ns]",
        "passenger_count": "UInt8",
        "trip_distance": "Float32",
        "rate_code_id": "UInt8",
        "store_and_forward_flag": "category",
        "pickup_location_id": "Int16",
        "dropoff_location_id": "Int16",
        "payment_type_id": "UInt8",
        "fare_amount": "Float32",
        "extra_charge": "Float32",
        "metered_rate_tax": "Float32",
        "tip_amount": "Float32",
        "tolls_amount": "Float32",
        "improvement_surcharge": "Float32",
        "total_amount": "Float32",
        "congestion_surcharge": "Float32",
        "airport_fee": "Float32",
        "cbd_congestion_fee": "Float32",
        # New fields
        "year": "Int16",
        "month": "UInt8",
        "currency": "category", 
        "trip_distance_unit": "category",
        "suspicious": "boolean"
    }

    mismatches = {}
    
    for col, expected_type in MASTER_SCHEMA_TYPES.items():
        if col in cols:
            actual_type = str(df[col].dtype)
            if actual_type != expected_type:
                mismatches[col] = {
                    "expected": expected_type,
                    "actual": actual_type
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
    df: pd.DataFrame, 
    cols: set[str]
) -> tuple[pd.DataFrame, str]:

    # Integer columns
    for c in ["vendor_id", "passenger_count", "rate_code_id", "payment_type_id", "month"]:
        if c in cols:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("UInt8")

    # Greater Integer columns
    for c in ["pickup_location_id", "dropoff_location_id", "year"]:
        if c in cols:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int16") 

    # Float columns
    for c in ["trip_distance", "fare_amount", "extra_charge", "metered_rate_tax", "tip_amount", "tolls_amount", "improvement_surcharge", "total_amount", 
              "congestion_surcharge", "airport_fee", "cbd_congestion_fee"]:
        if c in cols:
            df[c] = pd.to_numeric(df[c], errors="coerce").astype("Float32")

    # Datetime columns
    for c in ["pickup_datetime", "dropoff_datetime"]:
        if c in cols:
            df[c] = pd.to_datetime(df[c], errors="coerce")

    # Categorical columns
    if "store_and_forward_flag" in cols:
        df["store_and_forward_flag"] = df["store_and_forward_flag"].str.strip().str.upper()
        df["store_and_forward_flag"] = pd.Categorical(df["store_and_forward_flag"], categories = ["Y", "N", "UNKNOWN"])

    if "currency" in cols:
        df["currency"] = pd.Categorical(df["currency"], categories = ["USD"])

    if "trip_distance_unit" in cols:
        df["trip_distance_unit"] = pd.Categorical(df["trip_distance_unit"], categories = ["Miles"])

    if "suspicious" in cols:
        df["suspicious"] = df["suspicious"].astype("boolean")

    return df, "Master schema enforcement completed"

# Applies the master schema to a DataFrame by:
# - renaming columns
# - building and logging schema before alignment
# - aligning columns to the master schema
# - checking type mismatches before enforcement
# - enforcing master schema
# - checking type mismatches after enforcement
def apply_master_schema(
    df: pd.DataFrame, 
    logger: logging.Logger | None = None
) -> pd.DataFrame:
    
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

# Impute missing values for selected columns:
# - tip_amount -> 0
# - passenger_count -> 1
# - store_and_forward_flag -> "UNKNOWN"
def impute_missing_values(
    df: pd.DataFrame
) -> tuple[pd.DataFrame, str]:

    df = df.copy()
    
    tip_missing = 0
    passenger_missing = 0
    store_and_forward_flag_missing = 0
    
    if "tip_amount" in df.columns:
        tip_missing = df["tip_amount"].isna().sum()
        df["tip_amount"] = df["tip_amount"].fillna(0)

    if "passenger_count" in df.columns:
        passenger_missing = df["passenger_count"].isna().sum()
        df["passenger_count"] = df["passenger_count"].fillna(1)
        
    if "store_and_forward_flag" in df.columns:
        store_and_forward_flag_missing = df["store_and_forward_flag"].isna().sum()
        df["store_and_forward_flag"] = df["store_and_forward_flag"].fillna("UNKNOWN")

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

    return df, msg

# Imputes missing total_amount values by summing available monetary fields.
# A row is imputed when:
# 1. total_amount is missing (NaN) and
# 2. at least one of the individual monetary fields is present.
def impute_missing_total_amount(
    df: pd.DataFrame
) -> tuple[pd.DataFrame, str]:

    df = df.copy()

    AMOUNT_COLS = ["fare_amount", "extra_charge", "metered_rate_tax", "tip_amount", "tolls_amount",                    
                     "improvement_surcharge", "congestion_surcharge", "airport_fee",               
                     "cbd_congestion_fee"]

    valid_cols = [c for c in AMOUNT_COLS]

    fill_mask = (
        df["total_amount"].isna() &
        df[valid_cols].notna().any(axis = 1)
    )
    
    expected_total = df.loc[fill_mask, valid_cols].fillna(0).sum(axis = 1)

    df.loc[fill_mask, "total_amount"] = expected_total

    filled = int(fill_mask.sum())

    if filled == 0:
        msg = "No total amount values were imputed"
    elif filled == 1:
        msg = "1 total amount value was imputed summing all the monetary values"
    else:
        msg = f"{filled} total amount values were imputed summing all the currency values"

    return df, msg

# Remove rows where all columns are missing
# Parameters:
# df: input DataFrame
# Returns:
# Clean DataFrame, DataFrame containing removed row(s) and status message
def remove_rows_all_columns_missing(
    df: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, str] :

    invalid_mask = df.isna().all(axis = 1)
    df_removed_rows = df[invalid_mask].copy()
    
    df = df[~invalid_mask].copy()

    removed = len(df_removed_rows)
    
    if removed == 0:
        msg = "0 rows were dropped because no row had all columns null"
    elif removed == 1:
        msg = "1 row was dropped because all its columns were null"
    else: 
        msg = f"{removed - len(df)} were dropped because all their columns were null" 

    return df, df_removed_rows, msg    

# Remove rows where all specified critical columns are missing
# Parameters:
# df: input DataFrame
# critical_cols: columns considered critical. 
# Returns:
# Cleaned DataFrame, DataFrame of removed row(s) and status message
def remove_rows_with_missing_critical_columns(
    df: pd.DataFrame, 
    critical_cols: list[str] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    
    if critical_cols is None:
        return df, pd.DataFrame(), "No critical columns provided, no rows were removed"

    invalid_mask = df[critical_cols].isna().all(axis = 1)
    df_removed_rows = df[invalid_mask].copy()
    
    df = df[~invalid_mask].copy()
    removed = len(df_removed_rows)

    if removed == 0:
        msg = "No rows were dropped, because all rows had at least one critical field present"
    elif removed == 1:
        msg = "1 row was dropped because all critical fields were missing"
    else:
        msg = f"{removed} rows were dropped because all critical fields were missing"
    
    return df, df_removed_rows, msg

# Remove rows with invalid timestamps
# A row is considered valid if:
# - pickup_datetime and dropoff_datetime are not null
# - pickup_datetime < dropoff_dateimte
# Parameters:
# df: input DataFrame
# Returns:
# Cleaned DataFrame, DataFrame of removed row(s) and status message
def remove_invalid_timestamps(
    df: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, str]:

    not_nat_mask = (df["pickup_datetime"].notna() & df["dropoff_datetime"].notna())  

    order_mask = df["pickup_datetime"].lt(df["dropoff_datetime"])

    valid_mask = not_nat_mask & order_mask

    df_removed_rows = df[~valid_mask].copy()
    
    df = df[valid_mask].copy()

    removed = len(df_removed_rows)
    
    if removed == 0:
        msg = "No invalid timestamps were found"
    elif removed == 1:
        msg = "1 row had invalid timestamp(s)"
    else:
        msg = f"{removed} rows had invalid timestamps"
        
    return df, df_removed_rows, msg

# Validate year/month leakage in a chunk DataFrame by checking that pickup_datetime
# matches the single year-month pair declared in the chunk
# A row is removed when:
# - pickup_datetime.year != year 
# - pickup_datetime.month != month 
# - pickup_datetime, year, month is missing 
# Parameters:
# df: input DataFrame
# Returns:
# Cleaned DataFrame, DataFrame of leaked row(s) and status message
def validate_year_month_leakage_chunk(
    df: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, str]:

    if df["year"].isna().all() or df["month"].isna().all():
        empty = pd.DataFrame(columns = df.columns)
        return empty, empty.copy(), f"Cannot determine expected year/month pair (all year or/and month values are null)"

    year_values = df["year"].dropna().unique()
    month_values = df["month"].dropna().unique()

    if len(year_values) != 1 or len(month_values) != 1:
        empty = pd.DataFrame(columns = df.columns)
        return empty, empty.copy(), "Chunk has inconsistent year and month values"

    expected_year = year_values[0]
    expected_month = month_values[0]

    mask = (
        df["pickup_datetime"].notna() &
        df["year"].notna() &
        df["month"].notna() &
        df["pickup_datetime"].dt.year.eq(expected_year) &
        df["pickup_datetime"].dt.month.eq(expected_month)
    )

    leaked_rows = df[~mask].copy()
    valid_rows = df[mask].copy()

    nleaked = len(leaked_rows)

    if nleaked == 0:
        leak_msg = "No leaked rows were detected"
    elif nleaked == 1:
        leak_msg = "1 leaked row was detected"
    else:
        leak_msg = f"{nleaked} leaked rows were detected"
    
    msg = f"Perform leakage validation for: {int(expected_year)}-{int(expected_month):02d}\n{leak_msg}"
    
    return valid_rows, leaked_rows, msg

# Validate year/month leakage in a DataFrame by checking that pickup_datetime
# matches the single year-month pair declared in the chunk
# A row is removed when:
# - pickup_datetime.year != year 
# - pickup_datetime.month != month 
# - pickup_datetime, year, month is missing 
# Parameters:
# df: input DataFrame
# Returns:
# Cleaned DataFrame, DataFrame of leaked row(s) and status message
def validate_year_month_leakage_in_memory(
    df: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    
    if df["year"].isna().all() or df["month"].isna().all():
        empty = pd.DataFrame(columns = df.columns)
        return empty, empty.copy(), f"Cannot determine expected year/month pairs (all year or/and month values are null)"
        
    mask = (
        df["pickup_datetime"].notna() &
        df["year"].notna() &
        df["month"].notna() &
        (df["pickup_datetime"].dt.year == df["year"]) &
        (df["pickup_datetime"].dt.month == df["month"])
    )

    leaked_rows = df[~mask].copy()
    valid_rows = df[mask].copy()

    expected_years_months = sorted(set(zip(df["year"].dropna(), df["month"].dropna())))

    header_msg = "Perform leakage validation for the following year-month pairs:\n"

    pairs = "\n".join(f"- {int(y)}-{int(m):02d}" for y, m in expected_years_months)

    nleaked = len(leaked_rows)

    if nleaked == 0:
        leak_msg = "No leaked rows were detected"
    elif nleaked == 1:
        leak_msg = "1 leaked row was detected"
    else:
        leak_msg = f"{nleaked} leaked rows were detected"

    msg = f"{header_msg}{pairs}\n{leak_msg}"

    return valid_rows, leaked_rows, msg

# Removes rows with negative trip_distance values
def remove_negative_trip_distance(
    df: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, str]:

    valid_mask = df["trip_distance"].ge(0) | df["trip_distance"].isna()

    df_removed_rows = df[~valid_mask].copy()
    
    df = df[valid_mask].copy()

    removed = len(df_removed_rows)
    
    if removed == 0:
        msg = "No rows were removed due to negative trip distance"
    elif removed == 1:
        msg = "1 row was removed because it had a negative trip distance"
    else:
        msg = f"{removed} rows were removed because they had negative trip distances"
    
    return df, df_removed_rows, msg    

# Removes rows with negative passenger_count values
def remove_unrealistic_passenger_count(
    df: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, str]:

    valid_mask = df["passenger_count"].ge(0) | df["passenger_count"].isna()
    df_removed_rows = df[~valid_mask].copy()

    df = df[valid_mask].copy()
    
    removed = len(df_removed_rows)
    
    if removed == 0:
        msg = "No rows were removed due to negative passenger count"
    elif removed == 1:
        msg = "1 row was removed because it had a negative passenger count"
    else:
        msg = f"{removed} rows were removed because they had negative passenger counts"
    
    return df, df_removed_rows, msg  


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
# Dataframe with flag, suspicious row(s) and message
def add_suspicious_flag_amount_values_currency(
    df: pd.DataFrame, 
    tolerance: float = 2.0
) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    
    df = df.copy()
    
    suspicious_mask = pd.Series(False, index = df.index)

    # Inconsistent total amount
    AMOUNT_COLS = ["fare_amount", "extra_charge", "metered_rate_tax", "tip_amount", "tolls_amount",                    
                    "improvement_surcharge", "congestion_surcharge", "airport_fee",               
                    "cbd_congestion_fee"]

    expected_total = df[AMOUNT_COLS].fillna(0).sum(axis = 1)
    diff = (df["total_amount"] - expected_total).abs()
    suspicious_mask = suspicious_mask | (df["total_amount"].notna() & diff.gt(tolerance))

    # All monetary fields are null
    all_null_mask = df[AMOUNT_COLS].isna().all(axis = 1)
    suspicious_mask = suspicious_mask | all_null_mask

    # Missing total amount
    missing_total_mask = df["total_amount"].isna()
    suspicious_mask = suspicious_mask | missing_total_mask

    # Missing tip amount and payment type is card
    missing_tip_mask = (df["payment_type_id"] == 1) & df["tip_amount"].isna()
    suspicious_mask = suspicious_mask | missing_tip_mask
    
    # Add column to the original dataframe
    df["suspicious"] = suspicious_mask

    # Check how many suspicious rows there are
    df_suspicious_rows = df[df["suspicious"]].copy()

    nsuspicious = len(df_suspicious_rows)

    if nsuspicious == 0:
        msg = "No suspicious monetary rows were flagged"
    elif nsuspicious == 1:
        msg = "1 suspicious row was flagged as suspicious"
    else:
        msg = f"{nsuspicious} rows were flagged as suspicious"

    return df, df_suspicious_rows, msg

# Identifies trips with unrealistic speeds by computing trip duration in minutes,
# deriving speed in mph, and removing rows where the speed exceeds a
# specified maximum threshold.
# Parameters:
# df: input DataFrame
# max_speed_mph: maximum allowed speed in mph
# Results:
# cleaned DataFrame, removed row(s) and status message
def remove_unrealistic_trip_speed(
    df: pd.DataFrame, 
    max_speed_mph: float = 120
) -> tuple[pd.DataFrame, pd.DataFrame, str]:

    df = df.copy()
    
    df["trip_duration_min"] = (
        df["dropoff_datetime"] - df["pickup_datetime"]
    ).dt.total_seconds() / 60 # converted to minutes

    valid_duration = df["trip_duration_min"].gt(0)

    df["avg_speed_mph"] = pd.NA

    df.loc[valid_duration, "avg_speed_mph"] =  (
        df.loc[valid_duration, "trip_distance"] / 
        (df.loc[valid_duration, "trip_duration_min"] / 60)) # converted to hours

    valid_mask = (
        df["avg_speed_mph"].isna() |
        (df["avg_speed_mph"] <= max_speed_mph))

    df_removed_rows = df[~valid_mask].copy() 
    
    df = df[valid_mask].copy()

    df = df.drop(columns = ["trip_duration_min", "avg_speed_mph"], errors = "ignore")
    
    removed = len(df_removed_rows)
    
    if removed == 0:
        msg = "No rows were removed due to unrealistic trip speeds"
    elif removed == 1:
        msg = "1 row was removed due to unrealistic trip speed"
    else:
        msg = f"{removed} rows were removed due to unrealistic trip speeds"
    
    return df, df_removed_rows, msg

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
# process_type: leakage validation mode
# header: if to write CSV headers when appending output to files
# Returns:
# cleaned DataFrame
def cleaning_transformation(
    df: pd.DataFrame, 
    removed_slice_path: str | Path, 
    logger: logging.Logger | None = None, 
    process_type: Literal[IN_MEMORY, CHUNKING] = IN_MEMORY, 
    header: bool = False
) -> pd.DataFrame:
    
    if logger is None:
        logger = setup_logger()

    removed_slice_path = Path(removed_slice_path)
    df, df_removed_rows, msg = remove_rows_all_columns_missing(df)
    log_info_msg(logger, msg)
    if not is_empty(df_removed_rows):
        path = removed_slice_path / "rows_all_columns_missing.csv"
        log_info_msg(logger, f"Review file: {path} to check the removed rows")
        df_removed_rows.to_csv(path, mode = "a", index = False, header = header)
    
    if not is_empty(df):
        df, df_removed_rows, msg = remove_rows_with_missing_critical_columns(df, ["pickup_datetime", "dropoff_datetime", "trip_distance", "pickup_location_id",  
                                                      "dropoff_location_id", "fare_amount"])
        log_info_msg(logger, msg)
        if not is_empty(df_removed_rows):
            path = removed_slice_path / "rows_missing_critical_columns.csv"
            log_info_msg(logger, f"Review file: {path} to check the removed rows")
            df_removed_rows.to_csv(path, mode = "a", index = False, header = header)
        
    if not is_empty(df):
        df, df_removed_rows, msg =  remove_invalid_timestamps(df)
        log_info_msg(logger, msg)
        if not is_empty(df_removed_rows):
            path = removed_slice_path / "invalid_timestamps.csv"
            log_info_msg(logger, f"Review file: {path} to check the removed rows")
            df_removed_rows.to_csv(path, mode = "a", index = False, header = header)

    if not is_empty(df):

        if process_type == IN_MEMORY:
            df, leaked_df, msg = validate_year_month_leakage_in_memory(df)
            log_info_msg(logger, msg)
            if not is_empty(leaked_df):
                path = removed_slice_path / "month_leaked_rows.csv"
                log_info_msg(logger, f"Review file: {path} to check the leaked rows")
                leaked_df.to_csv(path, mode = "a", index = False, header = header)
            
        elif process_type == CHUNKING:
            df, leaked_df, msg = validate_year_month_leakage_chunk(df)
            log_info_msg(logger, msg)
            if not is_empty(leaked_df):
                path = removed_slice_path / "month_leaked_rows.csv"
                log_info_msg(logger, f"Review file: {path} to check the leaked rows")
                leaked_df.to_csv(path, mode = "a", index = False, header = header)

        else: log_info_msg(logger, f"Process type {process_type} does not exist")
    
    if not is_empty(df):
        df, df_removed_rows, msg = remove_negative_trip_distance(df)
        log_info_msg(logger, msg)
        if not is_empty(df_removed_rows):
            path = removed_slice_path / "negative_trip_distance.csv"
            log_info_msg(logger, f"Review file: {path} to check the removed rows")
            df_removed_rows.to_csv(path, mode = "a", index = False, header = header)

    if not is_empty(df):
        df, df_removed_rows, msg = remove_unrealistic_passenger_count(df)
        log_info_msg(logger, msg)
        if not is_empty(df_removed_rows):
            path = removed_slice_path / "unrealistic_passenger_count.csv"
            log_info_msg(logger, f"Review file: {path} to check the removed rows")
            df_removed_rows.to_csv(path, mode = "a", index = False, header = header)

    if not is_empty(df):
        df, df_removed_rows, msg = remove_unrealistic_trip_speed(df)
        log_info_msg(logger, msg)
        if not is_empty(df_removed_rows):
            path = removed_slice_path / "unrealistic_trip_speed.csv"
            log_info_msg(logger, f"Review file: {path} to check the removed rows")
            df_removed_rows.to_csv(path, mode = "a", index = False, header = header)

    if not is_empty(df):
        MAX_SUSPICIOUS_ROWS = 50
        df, df_suspicious, msg = add_suspicious_flag_amount_values_currency(df)
        log_info_msg(logger, msg)
        if not is_empty(df_suspicious):
            path = removed_slice_path / "suspicious_rows(NO_REMOVED_ROWS).csv"
            if len(df_suspicious) > MAX_SUSPICIOUS_ROWS:
                df_suspicious = df_suspicious.sample(MAX_SUSPICIOUS_ROWS, random_state = 42)
                log_info_msg(logger, f"{MAX_SUSPICIOUS_ROWS} suspicious rows were chosen using random sampling to be saved in the file: {path}")
            else:
                log_info_msg(logger, f"All {len(df_suspicious)} suspicious rows were saved in the file: {path}")
                
            df_suspicious.to_csv(path, mode = "a", index = False, header = header)

    if not is_empty(df):
        df, msg = impute_missing_values(df)
        log_info_msg(logger, msg) 

    if not is_empty(df):
        df, msg = impute_missing_total_amount(df)
        log_info_msg(logger, msg) 
    
    return df

# Build benchmark return for ingestion step
def build_return_dict_cleaning_transformation(
    slice_path: str | Path,
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
    summary: pd.DataFrame,
    missing_values: pd.DataFrame,
    chunksize: int = 0,
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
        "slice_path": str(slice_path),
        # Performance
        "processed_files": processed_files,
        "processed_rows": processed_rows,
        "rows_after_cleaning": rows_after_cleaning,
        "execution_time_sec": execution_time,
        "throughput_rows_sec": throughput,
        "chunksize": chunksize,
        "dataset_size_mib": compute_dataset_size_mib(slice_path),
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
        "msg": msg,
        # Profiling
        "summary": summary,
        "missing_values": missing_values
    } 

