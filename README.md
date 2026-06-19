# Evaluating and Comparing Traditional Single-Machine Analytics Tools and the Distributed Big-Data Processing Ecosystem
### Master's Dissertation Project (Grade: 70)

# Project Summary

This dissertation evaluates the performance, scalability, resource utilisation, and cost-efficiency of traditional single-machine analytics tools (Pandas, DuckDB and Scikit-learn) and a distributed Hadoop/Spark ecosystem across multiple workloads, dataset sizes, and deployment environments.

The project combines experimental benchmarking of quantitative metrics with qualitative evaluation criteria to derive evidence-based guidelines for selecting the most appropriate analytics architecture.

## Overview

This repository contains documentation and experimental materials for my Master’s thesis in Data Analytics. 
The research evaluates and compares traditional single-machine analytics tools with distributed big-data frameworks, providing guidelines for selecting the appropriate analytics stack based on data characteristics, deployment environments, implementation complexity, infrastructure cost, and setup effort.

The study focuses on performance, scalability, resource utilisation, cost-efficiency, implementation complexity, infrastructure cost, and setup effort across file formats, dataset sizes, system and hardware configurations, and deployment environments in both local and cloud-based settings.

---

## Research Objectives

The primary objectives of this research are:

- To evaluate the performance, scalability, resource utilisation, and cost-efficiency of traditional single-machine analytics tools (Pandas, DuckDB, and Scikit-learn) and a distributed big-data-processing ecosystem (Hadoop HDFS, Spark, Spark SQL, and Spark MLlib) across dataset sizes, file formats, hardware configurations, and deployment environments (local and cloud)

- To analyse and compare single-machine and distributed approaches, considering their performance, scalability, resource utilisation, cost-efficiency, and additional quantitative and qualitative evaluation criteria across both local and cloud environments
   
- To derive evidence-based guidelines for determining when the traditional single-machine analytics tools remain suitable and when the distributed ecosystem becomes more appropriate

---

## Research Design and Methodology

This study adopts a quantitative experimental research design in which multiple metrics are collected from controlled computational experiments. The primary objective is to evaluate the performance, scalability, resource utilisation, and cost-efficiency of traditional single-machine analytics tools (Pandas, DuckDB, and Scikit-learn) and a distributed big-data processing environment composed of Hadoop HDFS for distributed storage and Apache Spark, including its components Spark SQL and Spark MLlib, for distributed data processing and machine learning.

Experiments are conducted across varying:
- Data sizes
- File formats (CSV and Parquet)
- System and hardware configurations
- Deployment environments (local and cloud)
- Multiple stages of a data-processing pipeline, including ingestion, preprocessing, compaction, analytical querying, and machine learning workloads. 

In addition to performance and resource utilisation metrics, the analysis incorporates a broader set of quantitative metrics and qualitative criteria to provide a comprehensive assessment of each approach. Qualitative factors, including setup effort and time, learning curve in the context of this study, and implementation complexity, are also considered. These qualitative metrics provide insight into operational complexity and development effort beyond pure performance measurement. Algorithmic complexity is considered only at a high level to contextualise the computational behaviour of Pandas and Spark. This experimental design enables a systematic comparison between single-machine analytics tools and distributed big-data-processing technologies directly supporting the research objectives of evaluating performance, scalability, resource utilisation, cost-efficiency, as well as identifying the conditions under which approach become more appropriate.

Local Environment

Traditional analytics tools (Pandas, DuckDB, and Scikit-learn) are executed on a single machine using a Linux virtual machine (VM) running on a Windows host system. The VM is configured with 4vCPUs and ~8 GB of RAM. The following figure presents the hardware and software configuration used for the single-node experimental setup.


<img width="940" height="779" alt="image" src="https://github.com/user-attachments/assets/f061552e-6b4b-414b-8369-9c771da4a721" />


Cloud Environment
 
The distributed analytical stack composed of Hadoop HDFS, Spark, Spark SQL, and Spark MLlib is deployed on Amazon Web Services. This environment enables the evaluation of distributed execution and horizontal scalability by adding nodes to the configuration. The following figure illustrates the distributed two-node experimental environment deployed on AWS


<img width="921" height="1131" alt="image" src="https://github.com/user-attachments/assets/f46a7e10-581b-40c7-850a-2a8fee0c9c69" />


---

## Datasets

The primary dataset used in this research is the **NYC TLC Yellow Trip-Record Dataset**, which is publicly available through the City of New York’s Open Data Portal.

- The dataset contains neither direct personal identifiers nor sensitive data
- It is permitted for academic use
- No attempts are made to re-identify individuals or combine data with external sources
- The data is naturally partitioned by month and year, which facilitates the controlled generation of progressively larger datasets

The datasets consist of chronological accumulated monthly data files, with each slice covering a specific time period as shown in the following table:


<img width="940" height="273" alt="image" src="https://github.com/user-attachments/assets/e7f75f29-e8d1-4fa4-9ff6-55ad844eabbd" />


---

## Pipeline architecture

The system is designed as a data-processing pipeline built on a data lake architecture where raw and processed data are stored using open formats (CSV and Parquet).  It is implemented using a combination of Pandas, DuckDB, and Scikit‑learn in the single‑machine environment, and Hadoop HDFS, Apache Spark, Spark SQL, and Spark MLlib in the distributed environment. Both implementations operate over the same data lake architecture and processing workflow shown in figure below.

Operationally, it is structured in two stages: a schema-on-read phase for loading CSV files and a schema-on-write phase for producing the final Parquet outputs. The CSV datasets used in the pipeline were generated from Parquet files obtained from the NYC Open Data repository to enable testing of both open data formats within the pipeline.

The workflow begins with raw CSV data ingestion, followed by cleaning, transformation, profiling, and the creation of intermediate Parquet files partitioned by year and month. These Parquet files are then compacted into a single Parquet file per partition. Finally, analytical queries and machine learning tasks are executed on the compacted Parquet files. The following figure shows the pipeline architecture.


<img width="940" height="608" alt="image" src="https://github.com/user-attachments/assets/c454c57e-9e7a-4369-968d-88799300cdd7" />


---

## Execution Strategies

In the single-machine pipeline, data processing is performed using two execution strategies: in-memory processing and chunk-based processing. The in-memory approach loads the full dataset into memory, while chunk-based processing handles large datasets incrementally by reading data from disk in fixed-size chunks.  The latter strategy is necessary due to memory constraints when working with large CSV files. In this implementation, chunk-based processing results in the generation of multiple Parquet intermediate files in the local file system, which motivates the inclusion of a compaction stage in the pipeline.

---

## Repository Contents

- **research/** – Research Documentation: Early research materials, including the initial proposal and refined research plan
- **doc/** – Final dissertation document
- **project/** – Source code (Python scripts), benchmarking results in CSV and JSON formats, and the figures generated for the final dissertation document using the Streamlit framework 
  
---

## Technologies Used

- Python
- Pandas
- DuckDB
- Scikit-learn
- Apache Spark, Spark SQL, Spark MLlib
- Hadoop (HDFS)
- Jupyter Notebook
- AWS (for cloud-based experiments)
- Parquet files
- Streamlit framework
- Tmux

---

## Status

This repository contains the final implementation, documentation, and experimental work developed for the Master's dissertation project.

The research has been completed and evaluated, resulting in a final dissertation grade of 70.

The repository also includes the preliminary research work carried out during the previous semester for the Research and Professional Ethics module, 
including the refined research plan that formed the foundation of the dissertation project (research directory).

---

## Results and Evaluation

### Ingestion Stage

Single-machine in-memory processing (Pandas) performs efficiently for the smallest dataset slice (see Figure below). This is expected, as Pandas does not incur the overhead associated with the distributed environment. Spark introduces additional overhead due to its execution model, which involves organising work into tasks, coordinating execution and communication between components, even when running on a single machine.

Network activity is observed through the net_sent_mib and net_received_mib metrics, which reflect data transfer associated with interactions with Hadoop HDFS. In single-node Spark deployment using HDFS, these values include internal TCP communication between Spark and the local HDFS.

When comparing chunk-based processing with the distributed configuration, the single-node Spark setup exhibits higher execution time across all dataset slices. This suggests the presence of overhead associated with the distributed execution model operating on a single node, where additional layers of execution such as task scheduling and stage and task generation introduce computational cost without fully benefiting from distributed parallelism. In contrast, chunk-based processing follows a simpler execution model, processing data sequentially in manageable portions without requiring task orchestration or inter-component communication. This can lead to lower overhead and more efficient execution for the single-node environment. This represents a key advantage when handling datasets that exceed available memory, where incremental or partition-based (Spark) processing enables continued execution without failure. 


<img width="940" height="543" alt="image" src="https://github.com/user-attachments/assets/b67d9c0e-0a8e-4b4b-b363-47ebc79bf7e0" />


However, when scaling out to a two-node distributed configuration, the performance behaviour changes significantly (see Figure below). From the second dataset slice onwards, two-node Spark outperforms chunk-based processing. This highlights a key advantage of distributed systems: the ability to scale out by adding more nodes. While Pandas can scale up by increasing hardware capacity, it remains constrained by its single-machine execution model and limited support for high-parallel task execution (see Table below). The last column in the table shows that Pandas provides only modest parallelism for this Pandas version and stack architecture, as reflected in the peak_process_cpu_percent_average values (average of the highest CPU utilisation reached during each run). Although Pandas can utilise more than one core, its parallelism remains limited. Average values are reported because each single-machine experiment was executed three times per slice.


<img width="940" height="516" alt="image" src="https://github.com/user-attachments/assets/a441bb08-f681-4b66-ab8b-ef8c281ad701" />


<img width="940" height="493" alt="image" src="https://github.com/user-attachments/assets/d34ab666-13dd-4a1a-8e30-1676da83c4bf" />


### Cleaning, Transformation and Profiling Stage

The performance impact of this stage is clearly reflected in the execution time results in the figure below, where execution times increase substantially across all configurations compared to the ingestion stage. In single-node Spark, this behaviour is highly likely associated with the cost of multi-stage processing and repeated actions. In particular, the number of Spark Jobs, tasks, and stages grew substantially. For example, while the third dataset slice in the ingestion stage required only 4 jobs, 5 stages, and 272 tasks, the same slice in this stage expands to 71 jobs, 160 stages, and 11802 tasks. This shows that the workload is decomposed into much smaller units of work. Each of these units introduces some degree of scheduling, coordination, and execution overhead, which contribute to the observed increase in execution time. 


<img width="940" height="564" alt="image" src="https://github.com/user-attachments/assets/3975bd72-9144-4b18-b955-00f2e2a7f437" />


Furthermore, the dataset size for the third slice is 10899MiB (~11.43GB), but single-node Spark reports 277509MiB total input (~291.33GB), it is around 25 times the dataset size. This suggests that the same data is read many times during execution, due to the repeated triggering of actions such as count, write, and collect. 

Taken together, these observations indicate that single-node Spark repeatedly scans the underlying CSV data, because multiple actions are executed without persisting intermediate results in memory. This behaviour can increase I/O overhead, network activity, and contribute to the overall execution time, and is also consistent with Spark’s lazy evaluation model, where each action can trigger a recomputation of the transformations when intermediate results are not cached.

### Analytical queries Stage

The analytical queries results demonstrate clear differences between the execution characteristics of DuckDB and SparkSQL across all the Parquet dataset sizes. For both analytical workloads, DuckDB consistently achieved lower execution times than Spark SQL in both single-node and two-node distributed configurations (see Figures below).


<img width="940" height="935" alt="image" src="https://github.com/user-attachments/assets/378f9c77-5e92-4d20-b009-fb174fa22395" />


<img width="940" height="1133" alt="image" src="https://github.com/user-attachments/assets/f90837a0-37de-4253-87a1-a51673f62e1f" />


### Evidence-Based Guidelines
Key findings: 

- Prefer in-memory Pandas for datasets that fit comfortably in memory
- Use single-machine chunk-based processing when datasets exceed memory capacity and distributed infrastructure is not justified
- Use Spark/Hadoop ecosystem for large datasets workloads when sufficient computational resources are available to benefit from distributed parallelism
- Distributed Spark/Hadoop workloads require careful pipeline design to minimise recomputation overhead
- Single-machine tools remained highly competitive for several workloads
- Chunk-based processing improved scalability compared to in-memory execution
- Distributed Spark/Hadoop configurations improved scalability and performance for some larger workloads through additional parallel resources
- DuckDB consistently outperformed Spark SQL for the evaluated analytical SQL workloads
- The choice between single-machine and distributed approaches depends on dataset size, scalability requirements, infrastructure cost, available resources and technical expertise

---

## Conclusion

The experimental results demonstrated that single-machine analytics tools remain highly competitive for many workloads, while distributed Spark/Hadoop configurations can provide scalability and performance benefits when sufficient parallel resources are available.

The findings indicate that the choice between single-machine and distributed architectures depends on workload characteristics, scalability requirements, available resources, infrastructure cost, and technical expertise.

---

## Author

**Jorge Alberto Robla López**  
Master of Science in Data Analytics  
CCT College, Dublin

GitHub: https://github.com/Jorge36
