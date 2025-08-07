#!/bin/bash

# Paramètres
JAR_PATH="s3a://nba-data-pipeline/jars/nba_2.12-1.0.jar"
CLASS_NAME="Transform"
LOCAL_OUTPUT_DIR="/home/ec2-user/output_nba"
S3_OUTPUT_DIR="s3://nba-data-pipeline/output-nba/"
FINAL_NAME="nba_output.csv"

# Nettoyage local
echo "🧹 Suppression de l'ancien dossier local..."
rm -rf "$LOCAL_OUTPUT_DIR"
mkdir -p "$LOCAL_OUTPUT_DIR"

# Exécution du job Spark
echo "🚀 Exécution du job Spark..."
spark-submit \
  --class $CLASS_NAME \
  --master yarn \
  --deploy-mode client \
  "$JAR_PATH"

# Petite pause pour éviter un race condition
sleep 5

# Recherche du fichier CSV généré
CSV_FILE=$(find "$LOCAL_OUTPUT_DIR" -name "part-*.csv" | head -n 1)

# Vérification + envoi S3
if [ -f "$CSV_FILE" ]; then
  echo "📁 Fichier trouvé : $CSV_FILE"
  echo "☁️ Envoi vers S3 : ${S3_OUTPUT_DIR}${FINAL_NAME}"
  aws s3 cp "$CSV_FILE" "${S3_OUTPUT_DIR}${FINAL_NAME}"
  echo "✅ Terminé !"
else
  echo "❌ Erreur : aucun fichier CSV trouvé dans $LOCAL_OUTPUT_DIR"
fi

