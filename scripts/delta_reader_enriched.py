from pyspark.sql import SparkSession

#  Démarrer une session Spark avec Delta
spark = SparkSession.builder \
    .appName("DeltaEnrichedReader") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

#  Charger les données enrichies sauvegardées
df = spark.read.format("delta").load("/home/jovyan/data/delta_enriched_sncf")

#  Afficher un aperçu
df.show(truncate=False)
df.printSchema()

spark.stop()
