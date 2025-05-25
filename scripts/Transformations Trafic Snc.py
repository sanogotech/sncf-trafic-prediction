from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, max, min, count, avg, expr

# Création de la session Spark avec Delta Lake activé
delta_path = "/home/jovyan/data/delta_sncf"

spark = SparkSession.builder \
    .appName("TransformationsTraficSNCF") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Lecture des données enrichies
trajets = spark.read.format("delta").load(delta_path)

# Calcul du temps d’arrêt en gare
trajets = trajets.withColumn("temps_arret", col("departure") - col("arrival"))

# Suppression des lignes où les temps d’arrêt ne sont pas calculables
trajets = trajets.filter(col("temps_arret").isNotNull())

# Calcul du temps total de parcours par trip_id
temps_parcours = trajets.groupBy("trip_id").agg(
    (max("departure") - min("arrival")).alias("temps_total_parcours"),
    count("stop_id").alias("nb_arrets"),
    avg("temps_arret").alias("temps_moyen_arret")
)

# Jointure avec les trajets enrichis pour ajouter les statistiques
donnees_finales = trajets.join(temps_parcours, on="trip_id", how="left")

# Affichage d’un exemple
donnees_finales.select(
    "trip_id", "stop_id", "arrival", "departure", "temps_arret",
    "temps_total_parcours", "nb_arrets", "temps_moyen_arret"
).show(truncate=False)

# Enregistrement dans un nouveau dossier Delta enrichi
donnees_finales.write.format("delta").mode("overwrite").save("/home/jovyan/data/trafic_sncf_enrichi")

spark.stop()
