from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import StructType, StringType, ArrayType, StructField, LongType

#  Créer la session Spark avec Kafka
spark = SparkSession.builder \
    .appName("SNCFTripUpdatesConsumer") \
    .getOrCreate()

#  Définir le schéma du message JSON
update_schema = StructType([
    StructField("stop_id", StringType()),
    StructField("arrival", LongType()),
    StructField("departure", LongType())
])

schema = StructType([
    StructField("trip_id", StringType()),
    StructField("start_date", StringType()),
    StructField("updates", ArrayType(update_schema))
])

#  Lire les messages du topic Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "sncf-trip-updates") \
    .option("startingOffsets", "latest") \
    .load()

# Convertir les messages en chaînes JSON
json_df = df.selectExpr("CAST(value AS STRING) as json_str")

#  Parser les chaînes JSON en colonnes structurées
parsed_df = json_df.select(from_json(col("json_str"), schema).alias("data")).select("data.*")

#  Décomposer chaque arrêt (ligne par arrêt)
exploded_df = parsed_df.select(
    col("trip_id"),
    col("start_date"),
    explode(col("updates")).alias("stop")
).select(
    col("trip_id"),
    col("start_date"),
    col("stop.stop_id").alias("stop_id"),
    col("stop.arrival").alias("arrival"),
    col("stop.departure").alias("departure")
)

#  Afficher en continu les résultats dans la console
query = exploded_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
