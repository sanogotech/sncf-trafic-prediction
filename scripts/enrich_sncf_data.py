from pyspark.sql import SparkSession

# Initialisation Spark avec Delta
spark = SparkSession.builder \
    .appName("EnrichSNCFData") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Lecture des données Delta existantes
df_trips = spark.read.format("delta").load("/home/jovyan/data/delta_sncf")

# Lecture du fichier CSV des arrêts
df_stops = spark.read.option("header", True).csv("/home/jovyan/data/Exemple_stops_csv.csv")

# Jointure des deux datasets
df_enriched = df_trips.join(df_stops, on="stop_id", how="left")

# Écriture du résultat dans une nouvelle table Delta
df_enriched.write.format("delta").mode("overwrite").save("/home/jovyan/data/delta_sncf_enriched")

# Affichage de validation
df_enriched.show(truncate=False)
#  Enregistrer le DataFrame enrichi au format Delta
df_enriched.write.format("delta") \
    .mode("overwrite") \
    .save("/home/jovyan/data/delta_enriched_sncf")

print(" Données enrichies sauvegardées avec succès dans /home/jovyan/data/delta_enriched_sncf")

