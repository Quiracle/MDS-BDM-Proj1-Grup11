from pyspark.sql import SparkSession
spark = (
    SparkSession.builder
    .appName("DeltaInDocker")
    .master("spark://spark-master:7077")
    .config("spark.executor.extraJavaOptions", "-Dlog4j.configuration=log4j.properties")
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
print("Writing delta table...")
# Create and write a Delta table
df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
df.write.mode("overwrite").parquet("file:///data/test-parquet")

df.write.format("delta").mode("overwrite").save("file:///data/delta/people")

spark.catalog.clearCache()
# Read it back

df2 = spark.read.format("delta").load("file:///data/delta/people")
rows = df2.collect()
for row in rows:
    print(row)
