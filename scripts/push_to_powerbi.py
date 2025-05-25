import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, col

# Créer une session Spark avec support Delta
spark = SparkSession.builder \
    .appName("PushToPowerBI") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Lire les données enrichies depuis Delta Lake
chemin_delta = "/home/jovyan/work/data/delta_sncf_enriched"
df = spark.read.format("delta").load(chemin_delta)

# Appliquer la conversion des timestamps
df = df.withColumn("heure_arrivee", from_unixtime(col("arrival"))) \
       .withColumn("heure_depart", from_unixtime(col("departure")))

# Sélectionner les colonnes nécessaires
df_selection = df.select("stop_id", "stop_name", "trip_id", "start_date", "arrival", "departure", "heure_arrivee", "heure_depart")

# Convertir en JSON compatible Power BI (via Pandas)
data = df_selection.limit(10).toPandas().to_dict(orient="records")

# URL d’API de Power BI (remplace par ton URL exacte si nécessaire)
url = "https://api.powerbi.com/beta/78613bf0-3424-4615-9325-8f6a7d76a04a/datasets/9e42dacb-5d36-4f40-a2b8-c01f63adc78c/rows?experience=fabric-developer&key=lwBq0dtI5wk9DxsFW9fl%2BSv9LVSit9oTMnNFpeqAZ4LhF1CH00iEUBgtaWSXEIiUxQe3CKfZxb3k3LgZWIuVdA%3D%3D"

# Envoyer les données
headers = {"Content-Type": "application/json"}
response = requests.post(url, headers=headers, data=json.dumps(data))

# Afficher le statut
if response.status_code == 200:
    print("Envoi réussi vers Power BI.")
else:
    print("Erreur lors de l'envoi :", response.status_code)
    print(response.text)
