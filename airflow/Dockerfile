# ==============================================================================
# airflow/Dockerfile — Imagen de Apache Airflow + OpenJDK 17 + PySpark + S3A
# Versiones fijadas y verificadas:
# - Apache Airflow: 2.10.4 (Python 3.11)
# - PySpark: 3.5.3 (compilado contra Hadoop 3.3.4)
# - Conector S3A: hadoop-aws:3.3.4 + aws-java-sdk-bundle:1.12.262
# Ejecuta como usuario no-root (airflow) por seguridad.
# ==============================================================================
FROM apache/airflow:2.10.4-python3.11

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl \
        procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

USER airflow

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Descargar JARs oficiales de S3A compatibles con PySpark 3.5.3 (Hadoop 3.3.4)
# directamente al directorio de jars de PySpark para ejecución determinista sin red
RUN PYSPARK_JARS_DIR=$(python3 -c "import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), 'jars'))") && \
    curl -fSL https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
        -o "${PYSPARK_JARS_DIR}/hadoop-aws-3.3.4.jar" && \
    curl -fSL https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
        -o "${PYSPARK_JARS_DIR}/aws-java-sdk-bundle-1.12.262.jar" && \
    curl -fSL https://jdbc.postgresql.org/download/postgresql-42.7.4.jar \
        -o "${PYSPARK_JARS_DIR}/postgresql-42.7.4.jar"

