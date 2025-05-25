# sncf-trafic-prediction

# 🚆 Projet SNCF - Prédiction des Retards en Temps Réel

## 🎯 Objectif

Ce projet vise à **prévoir les retards des trains** à partir de données de passages en gare, en s'appuyant sur une architecture de **traitement temps réel** et un pipeline complet de **Machine Learning**, depuis l'ingestion des données jusqu'à la visualisation.

---

## 🧱 Stack technique

| Composant       | Rôle                                                                 |
|-----------------|----------------------------------------------------------------------|
| **Docker + Compose** | Conteneurisation et orchestration des services                     |
| **Apache Kafka**     | Ingestion en streaming des données simulées                        |
| **Apache Spark**     | Traitement des données + entraînement ML avec Spark MLlib         |
| **Delta Lake**       | Format de stockage transactionnel sur Spark                        |
| **Airflow**          | Orchestration des tâches ETL + ML                                  |
| **PostgreSQL**       | Backend pour Airflow                                               |
| **Power BI (DirectQuery)** | Visualisation temps réel (optionnelle)                        |
| **Python + Pandas + scikit-learn** | Préparation des données et export vers Power BI (si besoin) |

---

## ⚙️ Architecture du projet

```bash
.
├── docker/
│   └── jupyter/              # Dockerfile custom Spark + Delta + Jupyter
├── scripts/
│   ├── train_model.py        # Entraînement ML
│   └── push_prediction_to_powerbi.py  # Export vers Power BI (API REST)
├── dags/
│   └── train_predict_dag.py  # DAG Airflow pour orchestration
├── data/                     # Données brutes
├── notebooks/                # Analyses exploratoires
├── docker-compose.yml        # Stack complète
└── README.md
```

---

## 🔁 Pipeline complet

1. **Kafka simule** des messages de trafic ferroviaire.
2. **Airflow** déclenche `train_model.py` (batch ML en Spark).
3. Les prédictions sont stockées et/ou envoyées à Power BI.
4. Un deuxième script (`push_prediction_to_powerbi.py`) peut pousser les données vers un tableau de bord.

---

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

---

## 📊 Mesures DAX créées dans Power BI

Des mesures clés pour l’analyse des performances ferroviaires :
- `Nombre Trajets` : `COUNT(RealTimeData[trip_id])`
- `Retard Moyen (s)` : `AVERAGE(RealTimeData[departure] - RealTimeData[arrival])`
- `Durée Moyenne Parcours (s)` : `AVERAGE(RealTimeData[arrival] - RealTimeData[departure])`

---

## 🧠 Points forts techniques

- Utilisation de **Delta Lake** pour les performances et l’atomicité.
- Traitement **distribué et scalable** avec Spark.
- Architecture **modulaire**, conteneurisée, et automatisée.
- Support de **traitement batch + streaming**.
- Possibilité d’extension facile vers du temps réel complet avec Kafka + Spark Structured Streaming.

---

## ⚠️ Points d'amélioration / TODO

- Ajouter des contrôles qualité dans `train_model.py` (ex : éliminer valeurs aberrantes).
- Compléter le dashboard Power BI avec des visuels comme :
  - Évolution des retards par jour/heure
  - Retard moyen par arrêt (`stop_name`)
  - Carte des retards si coordonnées disponibles
- Option : publier un dataset "mock" sur Kaggle ou DataHub pour rendre le projet reproductible.

---

## 🚀 Lancer le projet

```bash
# Démarrer tous les services
docker-compose up -d --build

# Accéder à Jupyter : http://localhost:8888
# Accéder à Airflow : http://localhost:8080
```

---

## ✨ Résultat attendu

À chaque exécution automatique ou manuelle du pipeline :
- Les données sont prétraitées et enrichies
- Un modèle prédit les retards de fin de parcours
- Les résultats sont prêts à être visualisés ou exploités

---

## 🙋 Auteur

**Aldio [GitHub Username]**  
Projet réalisé dans le cadre d’un apprentissage Data Engineering / ML Ops.

> N'hésitez pas à me contacter pour une démo technique ou une revue du code !
