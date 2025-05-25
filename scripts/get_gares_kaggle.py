import pandas as pd
import kagglehub

# Télécharger le dataset
chemin = kagglehub.dataset_download("nathanlauga/french-train-station")

# Chemin vers le fichier CSV correct
csv_file = f"{chemin}/liste-des-gares.csv"

# Lecture et affichage des données
df = pd.read_csv(csv_file)
print(df.head())

# Sauvegarde au format CSV si besoin
df.to_csv("/home/jovyan/work/data/gares_kaggle.csv", index=False)
