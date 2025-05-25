#!/bin/bash

echo "⚙️ Lancement du simulateur de flux SNCF avec Spark + Kafka"

# Active l'environnement si besoin ou démarre Docker Compose
# docker-compose up -d kafka zookeeper jupyter

echo "Envoi des événements..."
python scripts/produce_sncf_trip_updates.py

echo " Transformation et enrichissement des données..."
python scripts/enrich_sncf_data.py

echo " Données prêtes dans scripts/export_powerbi/trafic_enrichi/"
