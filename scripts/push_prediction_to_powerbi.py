
import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_unixtime, col

# Spark session
spark = SparkSession.builder     .appName("PushToPowerBI")     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")     .getOrCreate()

# Charger données
df = spark.read.format("delta").load("/home/jovyan/data/delta_sncf_enriched")
df = df.withColumn("heure_arrivee", from_unixtime(col("arrival")))        .withColumn("heure_depart", from_unixtime(col("departure")))
df_selection = df.select("stop_id", "stop_name", "trip_id", "start_date", "arrival", "departure", "heure_arrivee", "heure_depart")
data = df_selection.limit(10).toPandas().to_dict(orient="records")

# API Power BI
url = "https://api.powerbi.com/beta/78613bf0-3424-4615-9325-8f6a7d76a04a/datasets/9e42dacb-5d36-4f40-a2b8-c01f63adc78c/rows?experience=power-bi&key=lwBq0dtI5wk9DxsFW9fl%2BSv9LVSit9oTMnNFpeqAZ4LhF1CH00iEUBgtaWSXEIiUxQe3CKfZxb3k3LgZWIuVdA%3D%3D"
headers = {"Content-Type": "application/json"}
response = requests.post(url, headers=headers, data=json.dumps(data))

# Résultat
if response.status_code == 200:
    print(" Envoi réussi vers Power BI.")
else:
    print(" Erreur lors de l'envoi :", response.status_code)
    print(response.text)
