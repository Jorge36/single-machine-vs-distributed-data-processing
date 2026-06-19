import psutil
import time
import os
from datetime import datetime
import threading
from pathlib import Path
import logging
import pandas as pd
import json
from collections.abc import Callable

# Constant variables
CSV_PATTERN = "*.csv"
IN_MEMORY = "in-memory"
CHUNKING = "chunking"
STANDARD = "standard"

INGESTION = "ingestion"
CLEANING_TRANSFORMATION = "ingestion, cleaning, transformation and profiling"
PERSISTENCE_TO_PARQUET = "ingestion, cleaning, transformation, profiling and persistence to parquet"
COMPACTION_TO_PARQUET_FILE = "compaction to one parquet file per partition"
QUERIES = "queries"
MACHINE_LEARNING = "machine learning"

PARQUET_PATHS = {
    "1st-slice": "../data/parquet/1st-slice_compacted/**/*.parquet",
    "2nd-slice": "../data/parquet/2nd-slice_compacted/**/*.parquet",
    "3rd-slice": "../data/parquet/3rd-slice_compacted/**/*.parquet",
    "4th-slice": "../data/parquet/4th-slice_compacted/**/*.parquet",
}

PARQUET_FOLDERS = {
    "1st-slice": "../data/parquet/1st-slice_compacted/",
    "2nd-slice": "../data/parquet/2nd-slice_compacted/",
    "3rd-slice": "../data/parquet/3rd-slice_compacted/",
    "4th-slice": "../data/parquet/4th-slice_compacted/",
}

# Setup Logger

# Initialises and returns a configured logger
def setup_logger(
    log_path: str | Path = "logs/pipeline.log"
) -> logging.Logger:
    # Ensure the log directory exists
    Path(log_path).parent.mkdir(parents = True, exist_ok = True)

    logger = logging.getLogger("pipeline_logger")
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        file_handler = logging.FileHandler(log_path, mode = "a", encoding = "utf-8")
        formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger

# Logs an informational message using the provided logger
def log_info_msg(
    logger: logging.Logger, 
    msg: str
) -> None:
    logger.info(msg)

# Logs an error message using the provided logger
def log_error_msg(
    logger: logging.Logger, 
    msg: str
) -> None:
    logger.error(msg)

# Resource Monitore class and Timer class

# Background monitor class for RSS (memory), CPU usage, per core load and I/O throughput
class ResourceMonitor:

    # Initialise monitor class
    def __init__(
        self, 
        interval: float = 0.1, 
        include_children: bool = True, 
        scale: int = 1024 * 1024
    ):
        self.interval = interval
        self.include_children = include_children
        self.process = psutil.Process(os.getpid())
        self._stop_monitoring = threading.Event()
        self._thread = None
        self._logical_io_initial_rw = None
        self._physical_io_initial_rw = None
        self._scale = scale

        self.samples = {
            "rss_mib": [],
            "process_cpu_percent": [],
            "system_cpu_per_core_percent": [],
            "logical_read_mib": [],
            "logical_write_mib": [],
            "physical_read_mib": [],
            "physical_write_mib": [],
                        
        }

    # Return child processes of the monitored process
    def _get_children(
        self
    ) -> list[psutil.Process]:
        try:
            return self.process.children(recursive = True)
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess) as e:
            print(f"Failed get children: {type(e).__name__}: {e}")
        return []

    # Apply a metric function to process + children and return the aggregated value
    def _sum_process_metric(
        self, 
        function: Callable[[psutil.Process], float]
    ) -> float:
        total = 0.0
        procs = [self.process]
        if self.include_children:
            procs.extend(self._get_children())

        for p in procs:
            try:
                total += function(p)
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess) as e:
                print(f"Failed to add metric: {type(e).__name__}: {e}")
        return total

    # Return current RSS memory usage in MiB
    def _get_rss_mib(self) -> float:
        rss_bytes = self._sum_process_metric(lambda p: p.memory_info().rss)
        return rss_bytes / self._scale 

    # Return aggregated read/write I/O in MiB
    def _get_logical_io_mib(self) -> tuple[float, float]:

        def read_bytes(p):
            return p.io_counters().read_bytes

        def write_bytes(p):
            return p.io_counters().write_bytes

        read_b = self._sum_process_metric(read_bytes)
        write_b = self._sum_process_metric(write_bytes)
        return read_b / self._scale, write_b / self._scale

    # Return system physical disk read/write I/O in MiB
    def _get_physical_disk_io_mib(self) -> tuple[float, float]:
        try:
            io_stats = psutil.disk_io_counters()
            if io_stats is None:
                return 0.0, 0.0
            return io_stats.read_bytes / self._scale, io_stats.write_bytes / self._scale
        except Exception as e:
            print(f"Failed to return physical disk I/O counters: {type(e).__name__}: {e}")
            return 0.0, 0.0

    # Initialise CPU counters
    def _initialise_cpu_utilisation(self) -> None:
        try:
            self.process.cpu_percent(interval = None)
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess) as e:
            print(f"Failed to return metric (cpu utilisation) per cpu: {type(e).__name__}: {e}")

        if self.include_children:
            for p in self._get_children():
                try:
                    p.cpu_percent(interval = None)
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess) as e:
                    print(f"Failed to return cpu metric (utilisation) of the children processes: {type(e).__name__}: {e}")

        try:
            psutil.cpu_percent(interval = None, percpu = True)
        except Exception as e:
            print(f"Failed to return cpu metric (utilisation) per cpu: {type(e).__name__}: {e}")

    # Return CPU utilization percent for process + children
    def _get_process_cpu_utilisation_percent(self) -> float:
        return self._sum_process_metric(lambda p: p.cpu_percent(interval = None))

    # Background loop to collect metrics
    def _run(self) -> None:
        self._initialise_cpu_utilisation()
        logical_read_initial, logical_write_initial = self._get_logical_io_mib()
        self._logical_io_initial_rw = (logical_read_initial, logical_write_initial)

        physical_read_initial, physical_write_initial = self._get_physical_disk_io_mib()
        self._physical_io_initial_rw = (physical_read_initial, physical_write_initial)



        while not self._stop_monitoring.is_set():
            rss_mib = self._get_rss_mib()
            process_cpu_utilisation = self._get_process_cpu_utilisation_percent()
            
            try:
                system_per_core_utilisation = psutil.cpu_percent(interval = None, percpu = True)
            except Exception as e:
                print(f"Failed to return metric (cpu utilisation) per cpu: {type(e).__name__}: {e}")
                system_per_core_utilisation = []
    
            logical_read_mib, logical_write_mib = self._get_logical_io_mib()
            physical_read_mib, physical_write_mib = self._get_physical_disk_io_mib()

            self.samples["rss_mib"].append(rss_mib)
            self.samples["process_cpu_percent"].append(process_cpu_utilisation)
            if len(system_per_core_utilisation) > 0:
                self.samples["system_cpu_per_core_percent"].append(system_per_core_utilisation)
            self.samples["logical_read_mib"].append(logical_read_mib - self._logical_io_initial_rw[0])
            self.samples["logical_write_mib"].append(logical_write_mib - self._logical_io_initial_rw[1])

            self.samples["physical_read_mib"].append(physical_read_mib - self._physical_io_initial_rw[0])
            self.samples["physical_write_mib"].append(physical_write_mib - self._physical_io_initial_rw[1])

            time.sleep(self.interval)

    # Start background resource monitoring
    def start(self) -> None:
        self._stop_monitoring.clear()
        self._thread = threading.Thread(target = self._run, daemon = True)
        self._thread.start()

    # Stop monitoring and return aggregated statistics
    def stop(self) -> dict[str, float | list[float]]:
        self._stop_monitoring.set()
        if self._thread is not None:
            self._thread.join()

        def avg(l):
            return sum(l) / len(l) if l else 0.0

        def peak(l):
            return max(l) if l else 0.0

        per_core_samples = self.samples["system_cpu_per_core_percent"]
        ncores = len(per_core_samples[0]) if per_core_samples else psutil.cpu_count(logical = True) or 1

        avg_per_core = [0.0] * ncores
        peak_per_core = [0.0] * ncores

        if per_core_samples:
            for core_idx in range(ncores):
                vals = [sample[core_idx] for sample in per_core_samples]
                avg_per_core[core_idx] = sum(vals) / len(vals)
                peak_per_core[core_idx] = max(vals)
                
        peak_rss_mib = peak(self.samples["rss_mib"])
        total_ram_mib = psutil.virtual_memory().total / self._scale
        memory_percent_of_total = 100.0 * peak_rss_mib / total_ram_mib if total_ram_mib > 0 else 0.0

        return {
            "avg_rss_mib": avg(self.samples["rss_mib"]),
            "peak_rss_mib": peak_rss_mib,
            "memory_percent_of_total": memory_percent_of_total,
            "avg_process_cpu_percent": avg(self.samples["process_cpu_percent"]),
            "peak_process_cpu_percent": peak(self.samples["process_cpu_percent"]),
            "avg_system_cpu_per_core_percent": avg_per_core,
            "peak_system_cpu_per_core_percent": peak_per_core,
            "logical_read_mib": peak(self.samples["logical_read_mib"]),
            "logical_write_mib": peak(self.samples["logical_write_mib"]),
            "physical_read_mib": peak(self.samples["physical_read_mib"]),
            "physical_write_mib": peak(self.samples["physical_write_mib"])
        }

    # Static method that return an empty/default resource statistics dictionary
    @staticmethod
    def as_dict() -> dict[str, float | list[float]]:
        return {
            "avg_rss_mib": 0.0,
            "peak_rss_mib": 0.0,
            "memory_percent_of_total": 0.0,
            "avg_process_cpu_percent": 0.0,
            "peak_process_cpu_percent": 0.0,
            "avg_system_cpu_per_core_percent": [],
            "peak_system_cpu_per_core_percent": [],
            "logical_read_mib": 0.0,
            "logical_write_mib": 0.0, 
            "physical_read_mib": 0.0,
            "physical_write_mib": 0.0 
        }
# Timer class for benchmarking source code sections
class Timer:
    # Initialise timer class
    def __init__(self):
        self.times = {}

    # Start a named timer
    def start(self, name: str) -> None:
        self.times[f"{name}_start"] = time.perf_counter()

    # Stop a named timer
    def stop(self, name: str) -> None:
        self.times[f"{name}_end"] = time.perf_counter()

    # Return execution time for a named timer
    def duration(self, name: str) -> float:
        return self.times[f"{name}_end"] - self.times[f"{name}_start"]

    # Return current timestamp
    @staticmethod
    def get_timestamp() -> datetime:
        return datetime.now().astimezone()

# Compute the dataset_size
def compute_dataset_size_mib(
    path: str | Path
) -> float:
    path = Path(path)
    if not path.exists():
        return 0.0
    total_bytes = sum(f.stat().st_size for f in path.rglob("*") if f.is_file())
    return total_bytes / (1024 * 1024)

# Save results to json and csv
def save_result_to_csv_and_json(
        result: dict, 
        csv_path: str,
        json_path: str
) -> None:
    
    Path(csv_path).parent.mkdir(parents=True, exist_ok=True)
    Path(json_path).parent.mkdir(parents=True, exist_ok=True)

    row = result.copy()

    # Convert list fields to strings so CSV can store them
    row["avg_system_cpu_per_core_percent"] = str(row["avg_system_cpu_per_core_percent"])
    row["peak_system_cpu_per_core_percent"] = str(row["peak_system_cpu_per_core_percent"])

    df = pd.DataFrame([row])
    csv_file = Path(csv_path)

    if csv_file.exists():
        df.to_csv(csv_file, mode="a", header=False, index=False)
    else:
        df.to_csv(csv_file, mode="w", header=True, index=False)

    with open(json_path, "w", encoding = "utf-8") as f:
        json.dump(result, f, indent = 4)


# Check if a pandas Dataframe is empty
def is_empty(df: pd.DataFrame) -> bool:
    return df.empty

# Count parquet files in a given directory recursively
# Parameters:
# parquet_path: directory path containing parquet files (default behaviour) OR glob pattern if use_pattern = True
# use_pattern: if parquet_path is a glob pattern
# Returns:
# number of parquet files found
def count_parquet_files(
    parquet_path: str | Path,
    use_pattern: bool = False
) -> int:
    
    if use_pattern:
        return len(list(Path().glob(str(parquet_path))))             
    else: 
        path = Path(parquet_path)
        if not path.exists():
            return 0
            
        return sum(1 for _ in path.rglob("*.parquet"))
        
        
# Save results to json and csv for cleaning, transformation and profiling stage
def save_result_to_csv_and_json_cleaning(
        result: dict, 
        csv_path: str,
        json_path: str
) -> None:

    csv_path_obj = Path(csv_path)
    json_path_obj = Path(json_path)
    
    csv_path_obj.parent.mkdir(parents=True, exist_ok=True)
    json_path_obj.parent.mkdir(parents=True, exist_ok=True)

    row = result.copy()

    df_summary = row.pop("summary")
    df_missing_values = row.pop("missing_values")

    # Convert list fields to strings so CSV can store them
    row["avg_system_cpu_per_core_percent"] = str(row["avg_system_cpu_per_core_percent"])
    row["peak_system_cpu_per_core_percent"] = str(row["peak_system_cpu_per_core_percent"])

    df = pd.DataFrame([row])

    if csv_path_obj.exists():
        df.to_csv(csv_path_obj, mode="a", header=False, index=False)
    else:
        df.to_csv(csv_path_obj, mode="w", header=True, index=False)

    timestamp = str(row["benchmark_start_timestamp"]).replace(":", "-")
    test_number = row["test_number"]
    # Get filename
    stem = json_path_obj.stem 
    out_dir = csv_path_obj.parent

    if isinstance(df_summary, pd.DataFrame) and not df_summary.empty:
        summary_path = out_dir / f"{stem}_{test_number}_{timestamp}_summary.csv"
        df_summary.to_csv(summary_path, index=True)
        row["summary_csv_path"] = str(summary_path)

    if isinstance(df_missing_values, pd.DataFrame) and not df_missing_values.empty:
        missing_path = out_dir / f"{stem}_{test_number}_{timestamp}_missing_values.csv"
        df_missing_values.to_csv(missing_path, index=True)
        row["missing_values_csv_path"] = str(missing_path)


    with open(json_path_obj, "w", encoding = "utf-8") as f:
        json.dump(row, f, indent = 4)
