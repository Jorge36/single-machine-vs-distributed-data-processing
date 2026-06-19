from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("fix-parquet").getOrCreate()

input_path = "file:///home/ubuntu/data/4th-slice_spark_compatible"
output_path = "file:///home/ubuntu/data/4th-slice_spark_compatible_final_version/"

df = (
    spark.read
    .option("recursiveFileLookup", "true")
    .parquet(input_path)
    .withColumn("_file", F.input_file_name())
)

df = df.drop("year", "month")

df = (
    df
    .withColumn("pickup_datetime", F.col("pickup_datetime").cast("timestamp"))
    .withColumn("dropoff_datetime", F.col("dropoff_datetime").cast("timestamp"))
)

df = (
    df
    .withColumn("year", F.regexp_extract("_file", r"year=(\d+)", 1))
    .withColumn("month", F.regexp_extract("_file", r"month=(\d+)", 1))
    .drop("_file")
)

df = (
    df
    .withColumn("year", F.col("year").cast("int"))
    .withColumn("month", F.col("month").cast("int"))
)

df = df.repartition(1, "year", "month")

# Write with SAME partitioning
(
    df.write
    .mode("overwrite")
    .partitionBy("year", "month")
    .parquet(output_path)
)
