
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder

# Spark session
spark = SparkSession.builder     .appName("SNCF_ML_Training")     .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")     .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")     .getOrCreate()

# Charger les données enrichies
df = spark.read.format("delta").load("/home/jovyan/data/delta_sncf_enriched")
df = df.toPandas()
df = df.dropna(subset=["arrival", "departure", "stop_id", "trip_id", "stop_name"])

# Feature engineering
df["temps_arret"] = df["departure"] - df["arrival"]
df["stop_name_enc"] = LabelEncoder().fit_transform(df["stop_name"])

stats = df.groupby("trip_id").agg({
    "departure": "max",
    "arrival": "min",
    "temps_arret": "mean",
    "stop_id": "count"
}).rename(columns={
    "departure": "max_departure",
    "arrival": "min_arrival",
    "temps_arret": "temps_moyen_arret",
    "stop_id": "nb_arrets"
})
stats["temps_total_parcours"] = stats["max_departure"] - stats["min_arrival"]
df = df.merge(stats[["temps_moyen_arret", "nb_arrets", "temps_total_parcours"]], on="trip_id")

# Simuler une variable cible : retard_arrivee
mean_arrival = df.groupby("stop_id")[["arrival"]].mean().reset_index().rename(columns={"arrival": "mean_arrival"})
df = df.merge(mean_arrival, on="stop_id", how="left")
df["retard_arrivee"] = df["arrival"] - df["mean_arrival"]
df = df.dropna(subset=["retard_arrivee"])

# Nettoyage
df = df[(df["retard_arrivee"] > -3600) & (df["retard_arrivee"] < 7200)]

# ML
features = ["stop_name_enc", "nb_arrets"]
X = df[features]
y = df["retard_arrivee"]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
model = RandomForestRegressor()
model.fit(X_train, y_train)
preds = model.predict(X_test)
mae = mean_absolute_error(y_test, preds)

print(f"✅ MAE retard_arrivee (secondes) : {mae:.2f}")
