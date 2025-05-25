# 🚆 sncf-trafic-prediction

Prédiction des retards des trains SNCF à partir de données de passages en gare, en temps réel ou batch, via une stack Big Data moderne (Spark, Kafka, Delta, Airflow, Power BI).

## 🎯 Objectif

Ce projet vise à **prévoir les retards des trains** à partir de données de passages en gare, en s'appuyant sur une architecture de **traitement temps réel** et un pipeline complet de **Machine Learning**, depuis l'ingestion des données jusqu'à la visualisation.

## 🧱 Stack technique

| Composant                 | Rôle                                                                 |
|---------------------------|----------------------------------------------------------------------|
| **Docker + Compose**      | Conteneurisation et orchestration des services                       |
| **Apache Kafka**          | Ingestion en streaming des données simulées                          |
| **Apache Spark**          | Traitement des données + entraînement ML avec Spark MLlib            |
| **Delta Lake**            | Format de stockage transactionnel sur Spark                          |
| **Airflow**               | Orchestration des tâches ETL + ML                                    |
| **PostgreSQL**            | Backend pour Airflow                                                 |
| **Power BI (DirectQuery)**| Visualisation temps réel (optionnelle)                               |
| **Python + Pandas + scikit-learn** | Préparation des données et export vers Power BI             |

## ⚙️ Architecture du projet

```
.
├── docker/
│   └── jupyter/              # Dockerfile custom Spark + Delta + Jupyter
├── scripts/
│   ├── train_model.py        # Entraînement ML
│   └── push_prediction_to_powerbi.py  # Export vers Power BI (API REST)
├── data/                     # Données brutes (exclues du dépôt GitHub)
├── notebooks/                # Analyses exploratoires
├── dags/                     # DAGs Airflow (exclus de ce dépôt)
├── docker-compose.yml        # Stack complète
└── README.md
```

## 🔁 Pipeline complet

1. **Kafka simule** des messages de trafic ferroviaire.
2. **Airflow** déclenche `train_model.py` (batch ML en Spark).
3. Les prédictions sont stockées et/ou envoyées à Power BI.
4. `push_prediction_to_powerbi.py` publie les données prêtes à visualiser.

## ✅ Avancement

| Étape                                 | Statut  |
|---------------------------------------|---------|
| Environnement Docker complet          | ✅ Fait |
| Kafka + Zookeeper                     | ✅ Fait |
| PostgreSQL (Airflow)                  | ✅ Fait |
| Spark avec Delta Lake                 | ✅ Fait |
| Script d'entraînement ML (Spark)      | ✅ Fait |
| Orchestration avec Airflow            | ✅ Fait |
| Export des prédictions (Power BI)     | ✅ Fait |
| Dashboard Power BI                    | ❌ Optionnel (non finalisé) |

## 🧠 Points forts techniques

- Utilisation de **Delta Lake** pour les performances et l’atomicité.
- Traitement **distribué et scalable** avec Spark.
- Architecture **modulaire**, conteneurisée, et automatisée.
- Support de **traitement batch + streaming**.
- Possibilité d’extension facile vers du temps réel complet avec Kafka + Spark Structured Streaming.

## 📦 Génération locale des données

Créez le producteur Kafka et lancez l'enrichissement en local :

```bash
bash bootstrap/produce_data.sh
```

Ce script :
- Lance le simulateur Kafka
- Déclenche le traitement Spark
- Écrit un fichier enrichi exportable vers Power BI

## 🚀 Lancer le projet

```bash
# Démarrer tous les services
docker-compose up -d --build

# Accéder à Jupyter : http://localhost:8888
# Accéder à Airflow : http://localhost:8080
```

## 📊 (Facultatif) Mesures Power BI

- `Nombre Trajets` : `COUNT(RealTimeData[trip_id])`
- `Retard Moyen (s)` : `AVERAGE(RealTimeData[departure] - RealTimeData[arrival])`
- `Durée Moyenne Parcours (s)` : `AVERAGE(RealTimeData[arrival] - RealTimeData[departure])`

## 🙋 À propos

**Auteur : Aldiouma Mbaye**  
Projet personnel réalisé pour approfondir l’architecture Big Data & MLOps avec Kafka, Spark et Airflow.  

