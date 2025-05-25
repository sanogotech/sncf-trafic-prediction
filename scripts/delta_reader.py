from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime

spark = SparkSession.builder.appName("ParquetReader").getOrCreate()

df = spark.read.parquet("/home/jovyan/data/delta_sncf")

# Convertir les timestamps en format lisible
df = df.withColumn("arrival_time", from_unixtime("arrival", "yyyy-MM-dd HH:mm:ss")) \
       .withColumn("departure_time", from_unixtime("departure", "yyyy-MM-dd HH:mm:ss"))

df.select("trip_id", "stop_id", "arrival_time", "departure_time").show(truncate=False)

spark.stop()
