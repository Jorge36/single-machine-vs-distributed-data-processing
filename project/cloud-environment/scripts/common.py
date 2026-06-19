import logging
from pathlib import Path
import psutil
import time
from datetime import datetime
from dataclasses import dataclass, asdict
import threading
from pyspark.sql import SparkSession, DataFrame
from typing import Any
import csv
import json
import requests
import os

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

PARQUET_FOLDERS = {
    "1st-slice": "hdfs:///data/parquet/1st-slice_compacted",
    "2nd-slice": "hdfs:///data/parquet/2nd-slice_compacted",
    "3rd-slice": "hdfs:///data/parquet/3rd-slice_compacted",
    "4th-slice": "hdfs:///data/parquet/4th-slice_compacted"
}

# Initialises and returns a configured logger
def setup_logger(
    log_path: str | Path = "logs/pipeline.log"
) -> logging.Logger:
    # Ensure the log directory exists
    Path(log_path).parent.mkdir(parents = True, exist_ok = True)

    logger = logging.getLogger("schema_logger")
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
        self._failed_process_metric_count = 0

        self.samples = {
            "rss_mib": [],
            "process_cpu_percent": [],
            "system_cpu_per_core_percent": [],
            "logical_read_mib": [],
            "logical_write_mib": [],
            "physical_read_mib": [],
            "physical_write_mib": [],
            "net_sent_mib": [],
            "net_received_mib": []
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
        function: callable
    ) -> float:
        total = 0
        procs = [self.process]
        if self.include_children:
            procs.extend(self._get_children())

        for p in procs:
            try:
                total += function(p)
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess) as e:
                self._failed_process_metric_count += 1
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

    # Return the current network I/O in MiB (bytes sent and bytes received)
    def _get_net_mib(self) -> tuple[float, float]:
        counters = psutil.net_io_counters()
        return counters.bytes_sent / self._scale, counters.bytes_recv / self._scale
        
    # Return 

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

        sent_initial, received_initial = self._get_net_mib()
        self._net_initial = (sent_initial, received_initial)

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

            
            sent_mib, received_mib = self._get_net_mib()
            self.samples["net_sent_mib"].append(sent_mib - self._net_initial[0])
            self.samples["net_received_mib"].append(received_mib - self._net_initial[1])

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
            "physical_write_mib": peak(self.samples["physical_write_mib"]),
            "net_sent_mib": peak(self.samples["net_sent_mib"]),
            "net_received_mib": peak(self.samples["net_received_mib"]),
            "failed_process_metric_count": self._failed_process_metric_count
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
            "physical_write_mib": 0.0,
            "net_sent_mib": 0.0,
            "net_received_mib": 0.0,
            "failed_process_metric_count": 0
        }
# Timer class for benchmarking source code sections
class Timer:
    # Initialise timer class
    def __init__(self):
        self.times = {}

    # Start a named timer
    def start(self, name) -> None:
        self.times[f"{name}_start"] = time.perf_counter()

    # Stop a named timer
    def stop(self, name) -> None:
        self.times[f"{name}_end"] = time.perf_counter()

    # Return execution time for a named timer
    def duration(self, name) -> float:
        return self.times[f"{name}_end"] - self.times[f"{name}_start"]

    # Return current timestamp
    @staticmethod
    def get_timestamp() -> datetime:
        return datetime.now().astimezone()

# Stores per-stage execution metrics collected from Spark's StageInfo class
@dataclass
class SparkStageMetrics:
    stage_id: int
    attempt_id: int | None
    name: str
    num_tasks: int
    num_active_tasks: int
    num_completed_tasks: int
    num_failed_tasks: int

# Stores aggregated job-level metrics, including jod IDs, stage IDs
# executor count, paralellism and all per-stage metrics
@dataclass
class SparkJobMetrics:
    job_ids: list[int]
    stage_ids: list[int]
    num_jobs: int
    num_stages: int
    num_executors: int
    parallelism: int
    stage_metrics: list[dict[str, Any]]

# Collect Spark job, stage, and executor metrics between two snapshots
class SparkMetricsCollector:

    # Initialise the collector
    def __init__(
        self, 
        spark_session: SparkSession,
    ):
        self.spark_session = spark_session
        self.sc = spark_session.sparkContext
        self.tracker = self.sc.statusTracker()

    # Returns the number of active executors in the cluster
    def _get_executor_count(self) -> int:
        mem_status = self.sc._jsc.sc().getExecutorMemoryStatus()
        return mem_status.size()

    # Returns all job IDs known to spark, otherwise an empty set 
    def _get_all_job_ids(self) -> set[int]:
        try:
            return set(self.tracker.getJobIdsForGroup(None))
        except Exception as e:
            print(f"No job ids were found: {type(e).__name__}: {e}" )
            return set()

    # Capture a snapshot of active jobs, active stages and executor count using the class SparkStatusTracker
    def snapshot(self) -> dict[str, Any]:
        active_jobs = list(self.tracker.getActiveJobsIds())
        active_stages = list(self.tracker.getActiveStageIds())
        return {
            "active_jobs": set(active_jobs),
            "active_stages": set(active_stages),
            "executor_count": self._get_executor_count()
        }

    # Computes job and stage metrics that ocurred between two snapshots
    def collect_between(
        self, 
        before: dict[str, Any],
        after: dict[str, Any] 
    ) -> dict[str, Any]:

        job_ids = set()
        try:
            group_job_ids = set(self.tracker.getJobIdsForGroup(self.sc.getLocalProperty("spark.jobGroup.id")))
            job_ids = job_ids.union(group_job_ids)
        except Exception as e:
            print(f"Failed to get job ids: {type(e).__name__}: {e}")

        job_ids = job_ids.union(set(after.get("active_jobs", [])))
        job_ids = job_ids.union(set(before.get("active_jobs", [])))

        stage_ids = set()
        stage_metrics: list[dict[str, Any]] = []

        for job_id in sorted(job_ids):
            info = self.tracker.getJobInfo(job_id)
            if info is None:
                continue
            sids = list(info.stageIds)
            stage_ids.update(sids)

        for stage_id in sorted(stage_ids):
            sinfo = self.tracker.getStageInfo(stage_id)
            if sinfo is None:
                continue

            metric = SparkStageMetrics(
                stage_id = stage_id,
                attempt_id = getattr(sinfo, "currentAttemptId", None),
                name = str(getattr(sinfo, "name", "")),
                num_tasks = int(getattr(sinfo, "numTasks", 0)),
                num_active_tasks = int(getattr(sinfo, "numActiveTasks", 0)),
                num_completed_tasks = int(getattr(sinfo, "numCompletedTasks", 0)),
                num_failed_tasks = int(getattr(sinfo, "numFailedTasks", 0))                
            )

            stage_metrics.append(asdict(metric))

        return asdict(
            SparkJobMetrics(
                job_ids = sorted(job_ids),
                stage_ids = sorted(stage_ids),
                num_jobs = len(job_ids),
                num_stages = len(stage_ids),
                num_executors = after.get("executor_count",0),
                parallelism = self.sc.defaultParallelism,
                stage_metrics = stage_metrics
            )
        )

    # function that return an empty/default SparkJobMetrics
    @staticmethod
    def as_dict(spark_session: SparkSession) -> dict[str, Any]:
        return asdict(
            SparkJobMetrics(
                job_ids = [],
                stage_ids = [],
                num_jobs = 0,
                num_stages = 0,
                num_executors = 0,
                parallelism = spark_session.sparkContext.defaultParallelism,
                stage_metrics = []
            )
        )

# Collects Spark REST API metrics for the current Spark application
class SparkRestMetricsCollector:
    # Initialise SparkRestMetricsCollector class
    def __init__(
        self, 
        spark_session: SparkSession, 
        base_url: str = "http://node1:4040",
    ):    
        self.spark_session = spark_session
        self.sc = spark_session.sparkContext
        self.base_url = base_url.rstrip("/")
        self.app_id = self.sc.applicationId
        self.mib = 1024 * 1024 # bytes -> MiB
        self.sec = 1000 # ms -> seconds

    # Get a JSON endpoint from the REST Spark API
    def _get_json(
        self,
        path: str    
    ) -> Any:
        url = f"{self.base_url}{path}"
        r = requests.get(url, timeout = 10)
        r.raise_for_status()
        return r.json()

    # Collect aggregated Spark metrics from the REST API.
    def collect(self) -> dict[str, Any]:
        executors = self._get_json(f"/api/v1/applications/{self.app_id}/executors")
        stages = self._get_json(f"/api/v1/applications/{self.app_id}/stages")
        jobs = self._get_json(f"/api/v1/applications/{self.app_id}/jobs")

        total_input_mib = 0
        total_output_mib = 0
        total_shuffle_read_mib = 0
        total_shuffle_write_mib = 0
        total_executor_runtime_sec = 0
        total_tasks = 0

        stage_summaries = []

        for st in stages:
            total_input_mib += st.get("inputBytes", 0) / self.mib
            total_output_mib += st.get("outputBytes", 0) / self.mib
            total_shuffle_read_mib += st.get("shuffleReadBytes", 0) / self.mib
            total_shuffle_write_mib += st.get("shuffleWriteBytes", 0) / self.mib
            total_executor_runtime_sec += st.get("executorRunTime", 0) / self.sec # seconds
            total_tasks += st.get("numTasks", 0) 

            stage_summaries.append({
                "stage_id": st.get("stageId"),
                "attempt_id": st.get("attemptId"),
                "name": st.get("name"),
                "status": st.get("status"),
                "num_tasks": st.get("numTasks"),
                "input_mib": st.get("inputBytes", 0) / self.mib,
                "output_mib": st.get("outputBytes", 0) / self.mib,
                "shuffle_read_mib": st.get("shuffleReadBytes", 0) / self.mib,
                "shuffle_write_mib": st.get("shuffleWriteBytes", 0) / self.mib,
                "executor_runtime_sec": st.get("executorRunTime", 0) / self.sec, # seconds
            })

        return {
            "spark_app_id": self.app_id,
            "num_jobs": len(jobs),
            "num_stages": len(stages),
            "num_executors": len(executors),
            "total_tasks": total_tasks,
            "total_input_mib": total_input_mib,
            "total_output_mib": total_output_mib,
            "total_shuffle_read_mib": total_shuffle_read_mib,
            "total_shuffle_write_mib": total_shuffle_write_mib,
            "total_executor_runtime_sec": total_executor_runtime_sec,
            "stages": stage_summaries,
            "executors": executors,
            "jobs": jobs
        }

    # static function/method that return an empty/default Spark Rest Metrics
    @staticmethod
    def as_dict() -> dict[str, Any]:
        return {
            "spark_app_id": "",
            "num_jobs": 0,
            "num_stages": 0,
            "num_executors": 0,
            "total_tasks": 0,
            "total_input_mib": 0.0,
            "total_output_mib": 0.0,
            "total_shuffle_read_mib": 0.0,
            "total_shuffle_write_mib": 0.0,
            "total_executor_runtime_sec": 0.0,
            "stages": [],
            "executors": [],
            "jobs": []
        }
        
# Create a Spark Session with a Spark Context
def create_spark_session(
    logger: logging.Logger | None = None
) -> SparkSession:

    if logger is None:
        logger = setup_logger()

    try: 
        spark_session = SparkSession.getActiveSession()
        if spark_session is not None:
            spark_session.stop()
    except Exception as e:
        log_error_msg(logger, f"Session could not be stopped due to the following error: {type(e).__name__}: {e}")

    spark_session = (
        SparkSession.builder
        .appName("thesis-cloud-env") 
        .getOrCreate()
    )

    spark_session.sparkContext.setLogLevel("WARN")
    return spark_session

# Test Spark Session
def test_session(
        spark_session: SparkSession
) -> None:
    print(spark_session.version)
    print(spark_session.sparkContext.getConf().get("spark.jars"))
    print(spark_session._jvm.org.apache.hadoop.util.VersionInfo.getVersion())
    spark_session.range(5).show()

# Count files in a FileSystem
def count_files_fs(
    spark_session: SparkSession,
    path: str,
    suffix: str | None = ".csv",
    recursive: bool = True
) -> int:

    sc = spark_session.sparkContext
    hadoop_conf = sc._jsc.hadoopConfiguration()
    jvm = sc._jvm

    fs_path = jvm.org.apache.hadoop.fs.Path(path)
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(fs_path.toUri(), hadoop_conf)
    
    if not fs.exists(fs_path):
        return 0
    
    count = 0
    it = fs.listFiles(fs_path, recursive) # recursive

    while it.hasNext():
        file = it.next()
        name = file.getPath().getName()

        if suffix is None or name.endswith(suffix):
            count += 1

    return count  

# Compute size of files in a FileSystem
def compute_dataset_size_mib(
    spark_session: SparkSession,
    path: str,
    suffix: str | None = ".csv",
    recursive: bool = True
) -> float:

    MIB = 1024 * 1024
    
    sc = spark_session.sparkContext
    hadoop_conf = sc._jsc.hadoopConfiguration()
    jvm = sc._jvm

    fs_path = jvm.org.apache.hadoop.fs.Path(path)
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(fs_path.toUri(), hadoop_conf)

    if not fs.exists(fs_path):
        return 0.0

    total_bytes = 0
    files_iter = fs.listFiles(fs_path, recursive) # recursive

    while files_iter.hasNext():
        file_status = files_iter.next()
        name = file_status.getPath().getName()

        if suffix is None or name.endswith(suffix):
            total_bytes += file_status.getLen()

    return total_bytes / MIB

# Build the CSV row for Spark ingestion benchmarks
# Keep only the field for the CSV file
def build_csv_row_ingestion_spark(result: dict[str, Any]) -> dict[str, Any]:

    return {
        "stage": result["stage"],
        "operation": result["operation"],
        "test_number": result["test_number"],
        "slice_path": result["slice_path"],
        "processed_files": result["processed_files"],
        "processed_rows": result["processed_rows"],
        "execution_time_sec": result["execution_time_sec"],
        "throughput_rows_sec": result["throughput_rows_sec"],
        "dataset_size_mib": result["dataset_size_mib"],
        "benchmark_start_timestamp": result["benchmark_start_timestamp"],
        "benchmark_end_timestamp": result["benchmark_end_timestamp"],
        "avg_rss_mib": result["avg_rss_mib"],
        "peak_rss_mib": result["peak_rss_mib"],
        "memory_percent_of_total": result["memory_percent_of_total"],
        "avg_process_cpu_percent": result["avg_process_cpu_percent"],
        "peak_process_cpu_percent": result["peak_process_cpu_percent"],
        "avg_system_cpu_per_core_percent": str(result["avg_system_cpu_per_core_percent"]),
        "peak_system_cpu_per_core_percent": str(result["peak_system_cpu_per_core_percent"]),
        "logical_read_mib": result["logical_read_mib"],
        "logical_write_mib": result["logical_write_mib"],  
        "physical_read_mib": result["physical_read_mib"],
        "physical_write_mib": result["physical_write_mib"],  
        "net_sent_mib": result["net_sent_mib"],
        "net_received_mib": result["net_received_mib"],
        "failed_process_metric_count": result["failed_process_metric_count"],
        "msg": result["msg"],
        "dataframe_partitions": result["dataframe_partitions"],
    }

# Build the CSV row for Spark cleaning, transformation and profiling benchmarks
# Keep only the field for the CSV file
def build_csv_row_cleaning_transformation_spark(result: dict[str, Any]) -> dict[str, Any]:

    return {
        "stage": result["stage"],
        "operation": result["operation"],
        "test_number": result["test_number"],
        "slice_path": result["slice_path"],
        "processed_files": result["processed_files"],
        "processed_rows": result["processed_rows"],
        "rows_after_cleaning": result["rows_after_cleaning"],
        "execution_time_sec": result["execution_time_sec"],
        "throughput_rows_sec": result["throughput_rows_sec"],
        "dataset_size_mib": result["dataset_size_mib"],
        "benchmark_start_timestamp": result["benchmark_start_timestamp"],
        "benchmark_end_timestamp": result["benchmark_end_timestamp"],
        "avg_rss_mib": result["avg_rss_mib"],
        "peak_rss_mib": result["peak_rss_mib"],
        "memory_percent_of_total": result["memory_percent_of_total"],
        "avg_process_cpu_percent": result["avg_process_cpu_percent"],
        "peak_process_cpu_percent": result["peak_process_cpu_percent"],
        "avg_system_cpu_per_core_percent": str(result["avg_system_cpu_per_core_percent"]),
        "peak_system_cpu_per_core_percent": str(result["peak_system_cpu_per_core_percent"]),
        "logical_read_mib": result["logical_read_mib"],
        "logical_write_mib": result["logical_write_mib"],  
        "physical_read_mib": result["physical_read_mib"],
        "physical_write_mib": result["physical_write_mib"],
        "net_sent_mib": result["net_sent_mib"],
        "net_received_mib": result["net_received_mib"],
        "failed_process_metric_count": result["failed_process_metric_count"],
        "msg": result["msg"],
        "dataframe_partitions": result["dataframe_partitions"],
    }

# Build the CSV row for Spark persistence to parquet files benchmarks
# Keep only the field for the CSV file
def build_csv_row_parquet_persistence_spark(result: dict[str, Any]) -> dict[str, Any]:

    return {
        "stage": result["stage"],
        "operation": result["operation"],
        "test_number": result["test_number"],
        "slice_path": result["slice_path"],
        "parquet_path": result["parquet_path"],
        "processed_files": result["processed_files"],
        "processed_rows": result["processed_rows"],
        "rows_after_cleaning": result["rows_after_cleaning"],
        "execution_time_sec": result["execution_time_sec"],
        "throughput_rows_sec": result["throughput_rows_sec"],
        "dataset_size_mib": result["dataset_size_mib"],
        "parquet_files_size_mib": result["parquet_files_size_mib"],
        "created_parquet_files": result["created_parquet_files"],
        "benchmark_start_timestamp": result["benchmark_start_timestamp"],
        "benchmark_end_timestamp": result["benchmark_end_timestamp"],
        "avg_rss_mib": result["avg_rss_mib"],
        "peak_rss_mib": result["peak_rss_mib"],
        "memory_percent_of_total": result["memory_percent_of_total"],
        "avg_process_cpu_percent": result["avg_process_cpu_percent"],
        "peak_process_cpu_percent": result["peak_process_cpu_percent"],
        "avg_system_cpu_per_core_percent": str(result["avg_system_cpu_per_core_percent"]),
        "peak_system_cpu_per_core_percent": str(result["peak_system_cpu_per_core_percent"]),
        "logical_read_mib": result["logical_read_mib"],
        "logical_write_mib": result["logical_write_mib"],
        "physical_read_mib": result["physical_read_mib"],
        "physical_write_mib": result["physical_write_mib"],
        "net_sent_mib": result["net_sent_mib"],
        "net_received_mib": result["net_received_mib"],
        "failed_process_metric_count": result["failed_process_metric_count"],
        "msg": result["msg"],
        "dataframe_partitions": result["dataframe_partitions"],
        "dataframe_partitions_parquet": result["dataframe_partitions_parquet"],
    }


# Save results to json and csv
def save_result_to_csv_and_json(
        result: dict, 
        csv_path: str,
        json_path: str,
        csv_row_builder
) -> None:
    
    csv_path_obj = Path(csv_path)
    json_path_obj = Path(json_path)

    csv_path_obj.parent.mkdir(parents=True, exist_ok=True)
    json_path_obj.parent.mkdir(parents=True, exist_ok=True)

    row = csv_row_builder(result)

    file_exists = csv_path_obj.exists()
    with open(csv_path_obj, "a" if file_exists else "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames = list(row.keys()))
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)

    with open(json_path_obj, "w", encoding="utf-8") as f:
        json.dump(result, f, indent = 4)

# Save results to json and csv for cleaning and transformation stage
def save_result_to_csv_and_json_cleaning(
        result: dict, 
        csv_path: str,
        json_path: str,
        csv_row_builder
) -> None:
    
    csv_path_obj = Path(csv_path)
    json_path_obj = Path(json_path)

    csv_path_obj.parent.mkdir(parents = True, exist_ok = True)
    json_path_obj.parent.mkdir(parents = True, exist_ok = True)

    row = csv_row_builder(result)

    file_exists = csv_path_obj.exists()
    with open(csv_path_obj, "a" if file_exists else "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames = list(row.keys()))
        if not file_exists:
            writer.writeheader()
        writer.writerow(row)

    timestamp = str(row["benchmark_start_timestamp"]).replace(":", "-")
    test_number = row["test_number"]
    # Get filename
    stem = json_path_obj.stem 
    out_dir = csv_path_obj.parent

    summary = result.get("summary")
    if summary is not None:
        
        summary_rows = [r.asDict(recursive = True) for r in summary]

        if summary_rows:
            summary_path = out_dir / f"{stem}_{test_number}_{timestamp}_summary.csv"
            with open(summary_path, "w", newline = "", encoding = "utf-8") as f:
                writer = csv.DictWriter(f, fieldnames = list(summary_rows[0].keys()))
                writer.writeheader()
                writer.writerows(summary_rows)
        
            result["summary_csv_path"] = str(summary_path)

    missing_values = result.get("missing_values")
    if missing_values is not None:

        missing_rows = [r.asDict(recursive = True) for r in missing_values]
        if missing_rows:
            missing_path = out_dir / f"{stem}_{test_number}_{timestamp}_missing_values.csv"
            with open(missing_path, "w", newline = "", encoding = "utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=list(missing_rows[0].keys()))
                writer.writeheader()
                writer.writerows(missing_rows)
        
            result["missing_values_csv_path"] = str(missing_path)

    result.pop("summary", None)
    result.pop("missing_values", None)

    with open(json_path_obj, "w", encoding="utf-8") as f:
        json.dump(result, f, indent = 4)

# Return files in a FileSystem
def list_files_fs(
    spark_session: SparkSession,
    path: str,
    suffix: str = ".parquet",
    recursive: bool = True
) -> list[str]:

    sc = spark_session.sparkContext
    hadoop_conf = sc._jsc.hadoopConfiguration()
    jvm = sc._jvm

    fs_path = jvm.org.apache.hadoop.fs.Path(path)
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(fs_path.toUri(), hadoop_conf)

    if not fs.exists(fs_path):
        return []
    
    files = []
    it = fs.listFiles(fs_path, recursive) # recursive

    while it.hasNext():
        file = it.next()
        file_path = file.getPath()
        name = file_path.getName()
        
        if suffix is None or name.endswith(suffix):
            files.append(file_path.toString())

    return files   

# Compute size for files in a FileSystem
def compute_files_size_mib(
    spark_session: SparkSession,
    files: list[str],
) -> float:

    MIB = 1024 * 1024
    
    sc = spark_session.sparkContext
    hadoop_conf = sc._jsc.hadoopConfiguration()
    jvm = sc._jvm

    total_bytes = 0

    # Initialise FileSystem
    first_path = jvm.org.apache.hadoop.fs.Path(files[0])
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(first_path.toUri(), hadoop_conf)
    
    for f in files:
        fs_path = jvm.org.apache.hadoop.fs.Path(f)
        total_bytes += fs.getFileStatus(fs_path).getLen()
    
    return total_bytes / MIB

# Check if the path exist in the FileSystem
def path_exists_fs(
    spark_session: SparkSession,
    path: str
) -> bool:

    sc = spark_session.sparkContext
    hadoop_conf = sc._jsc.hadoopConfiguration()
    jvm = sc._jvm

    fs_path = jvm.org.apache.hadoop.fs.Path(path)
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(fs_path.toUri(), hadoop_conf)

    return fs.exists(fs_path)

# Non-recursive listing of files/directories in a path
def list_folder_fs(
    spark_session: SparkSession,
    path: str
) -> list[dict[str, Any]]:

    sc = spark_session.sparkContext
    jvm = sc._jvm
    hadoop_conf = sc._jsc.hadoopConfiguration()

    fs_path = jvm.org.apache.hadoop.fs.Path(path)
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(fs_path.toUri(), hadoop_conf)

    if not fs.exists(fs_path):
        return []

    folder_contains = []

    for status in fs.listStatus(fs_path):
        folder_contains.append({
            "path": status.getPath().toString(),
            "name": status.getPath().getName(),
            "is_dir": status.isDirectory(),
        })

    return folder_contains

# List month directories and its relative path 
def list_month_partition_dir_fs(
    spark_session: SparkSession,
    parquet_path: str
) -> list[tuple[str, str]]:

    month_dirs = []

    for year_item in list_folder_fs(spark_session, parquet_path):
        if not year_item["is_dir"]:
            continue
        if not year_item["name"].startswith("year="):
            continue

        year_path = year_item["path"]

        for month_item in list_folder_fs(spark_session, year_path):
            if not month_item["is_dir"]:
                continue
            if not month_item["name"].startswith("month="):
                continue

            month_path = month_item["path"]
            rel_path = f"{year_item['name']}/{month_item['name']}"
            month_dirs.append((month_path, rel_path))

    return month_dirs

# Check if a spark Dataframe is empty
def is_empty(df: DataFrame) -> bool:
    return len(df.take(1)) == 0

# Count rows in a DataFrame, action operation
def count_df(df: DataFrame) -> int:
    return int(df.count())

# Build the CSV row for Spark compaction to one parquet file benchmarks
# Keep only the field for the CSV file
def build_csv_row_compaction_parquet_lake_spark(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "stage": result["stage"],
        "operation": result["operation"],
        "test_number": result["test_number"],
        "parquet_path": result["parquet_path"],
        "compacted_parquet_path": result["compacted_parquet_path"],
        "processed_files": result["processed_files"],
        "processed_rows": result["processed_rows"],
        "execution_time_sec": result["execution_time_sec"],
        "throughput_rows_sec": result["throughput_rows_sec"],
        "parquet_size_mib": result["parquet_size_mib"],
        "compacted_parquet_size_mib": result["compacted_parquet_size_mib"],
        "compacted_parquet_files_created": result["compacted_parquet_files_created"],
        "total_partitions_created": result["total_partitions_created"],
        "benchmark_start_timestamp": result["benchmark_start_timestamp"],
        "benchmark_end_timestamp": result["benchmark_end_timestamp"],
        "avg_rss_mib": result["avg_rss_mib"],
        "peak_rss_mib": result["peak_rss_mib"],
        "memory_percent_of_total": result["memory_percent_of_total"],
        "avg_process_cpu_percent": result["avg_process_cpu_percent"],
        "peak_process_cpu_percent": result["peak_process_cpu_percent"],
        "avg_system_cpu_per_core_percent": str(result["avg_system_cpu_per_core_percent"]),
        "peak_system_cpu_per_core_percent": str(result["peak_system_cpu_per_core_percent"]),
        "logical_read_mib": result["logical_read_mib"],
        "logical_write_mib": result["logical_write_mib"],
        "physical_read_mib": result["physical_read_mib"],
        "physical_write_mib": result["physical_write_mib"],
        "net_sent_mib": result["net_sent_mib"],
        "net_received_mib": result["net_received_mib"],
        "failed_process_metric_count": result["failed_process_metric_count"],
        "msg": result["msg"],
        "dataframe_partitions_parquet": result["dataframe_partitions_parquet"],
        "dataframe_partitions_compacted_parquet": result["dataframe_partitions_compacted_parquet"],
    }

# Build the CSV row for Spark queries benchmarks
# Keep only the field for the CSV file
def build_csv_row_queries_spark(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "stage": result["stage"],
        "operation": result["operation"],
        "test_number": result["test_number"],
        "slice_name": result["slice_name"],
        "parquet_path": result["parquet_path"],
        "query_name": result["query_name"],
        "total_files": result["total_files"],
        "total_rows_in_dataset": result["total_rows_in_dataset"],
        "rows_returned": result["rows_returned"],
        "execution_time_sec": result["execution_time_sec"],
        "throughput_rows_sec": result["throughput_rows_sec"],
        "parquet_files_size_mib": result["parquet_files_size_mib"],
        "benchmark_start_timestamp": result["benchmark_start_timestamp"],
        "benchmark_end_timestamp": result["benchmark_end_timestamp"],
        "avg_rss_mib": result["avg_rss_mib"],
        "peak_rss_mib": result["peak_rss_mib"],
        "memory_percent_of_total": result["memory_percent_of_total"],
        "avg_process_cpu_percent": result["avg_process_cpu_percent"],
        "peak_process_cpu_percent": result["peak_process_cpu_percent"],
        "avg_system_cpu_per_core_percent": str(result["avg_system_cpu_per_core_percent"]),
        "peak_system_cpu_per_core_percent": str(result["peak_system_cpu_per_core_percent"]),
        "logical_read_mib": result["logical_read_mib"],
        "logical_write_mib": result["logical_write_mib"],
        "physical_read_mib": result["physical_read_mib"],
        "physical_write_mib": result["physical_write_mib"],
        "net_sent_mib": result["net_sent_mib"],
        "net_received_mib": result["net_received_mib"],
        "failed_process_metric_count": result["failed_process_metric_count"],
        "msg": result["msg"],
        "dataframe_partitions_parquet": result["dataframe_partitions_parquet"],
    }


# Build the CSV row for Spark machine learning benchmarks
# Keep only the field for the CSV file
def build_csv_row_ml_spark(result: dict[str, Any]) -> dict[str, Any]:
    return {
        "stage": result["stage"],
        "operation": result["operation"],
        "test_number": result["test_number"],
        "slice_name": result["slice_name"],
        "parquet_path": result["parquet_path"],
        "total_files": result["total_files"],
        "total_rows_in_dataset": result["total_rows_in_dataset"],
        "total_rows_used_ml": result["total_rows_used_ml"],
        "execution_time_sec": result["execution_time_sec"],
        "execution_time_sec_train": result["execution_time_sec_train"],
        "execution_time_sec_pred": result["execution_time_sec_pred"],
        "throughput_total_rows_sec": result["throughput_total_rows_sec"],
        "throughput_rows_used_ml_sec": result["throughput_rows_used_ml_sec"],
        "parquet_files_size_mib": result["parquet_files_size_mib"],
        "benchmark_start_timestamp": result["benchmark_start_timestamp"],
        "benchmark_end_timestamp": result["benchmark_end_timestamp"],
        "avg_rss_mib": result["avg_rss_mib"],
        "peak_rss_mib": result["peak_rss_mib"],
        "memory_percent_of_total": result["memory_percent_of_total"],
        "avg_process_cpu_percent": result["avg_process_cpu_percent"],
        "peak_process_cpu_percent": result["peak_process_cpu_percent"],
        "avg_system_cpu_per_core_percent": str(result["avg_system_cpu_per_core_percent"]),
        "peak_system_cpu_per_core_percent": str(result["peak_system_cpu_per_core_percent"]),
        "logical_read_mib": result["logical_read_mib"],
        "logical_write_mib": result["logical_write_mib"],
        "physical_read_mib": result["physical_read_mib"],
        "physical_write_mib": result["physical_write_mib"],
        # Quality metrics
        "mae": result["mae"],
        "rmse": result["rmse"],
        "r2": result["r2"],
        "msg": result["msg"],
        "dataframe_partitions_parquet": result["dataframe_partitions_parquet"],
    }
