FROM jupyter/pyspark-notebook:spark-3.3.2

USER root

# 📦 Installer curl et les bibliothèques Python
RUN apt-get update && apt-get install -y curl && \
    pip install --upgrade pip && \
    pip install pandas numpy matplotlib seaborn scikit-learn pyarrow

# 📦 Télécharger les bons JARs Delta Lake pour Spark 3.3
RUN mkdir -p /usr/local/spark/jars && \
    curl -L -o /usr/local/spark/jars/delta-core_2.12-2.3.0.jar https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.3.0/delta-core_2.12-2.3.0.jar && \
    curl -L -o /usr/local/spark/jars/delta-storage-2.3.0.jar https://repo1.maven.org/maven2/io/delta/delta-storage/2.3.0/delta-storage-2.3.0.jar

# ⚙️ Définir les variables pour Spark + Delta
ENV SPARK_JARS_DIR="/usr/local/spark/jars"
ENV SPARK_CLASSPATH="$SPARK_JARS_DIR/*"
ENV PYSPARK_SUBMIT_ARGS="--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
 --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog pyspark-shell"

USER jovyan

CMD ["start-notebook.sh", "--NotebookApp.token=''", "--NotebookApp.password=''"]
