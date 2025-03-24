from pyspark.sql import SparkSession

builder = (
    SparkSession.builder.appName("DeltaLakeLocal")
    .master("spark://localhost:7077")
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = builder.getOrCreate()

# Example: Write a Delta table
df = spark.createDataFrame([(1, "Alice"), (2, "Bob")], ["id", "name"])
df.write.format("delta").mode("overwrite").save("/data/delta/people")

# Read it back
df2 = spark.read.format("delta").load("/data/delta/people")
df2.show()
