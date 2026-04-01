FROM apache/airflow:2.8.1

USER root

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /opt/airflow/jars

RUN curl -sS https://repo1.maven.org/maven2/io/openlineage/openlineage-spark_2.12/1.8.0/openlineage-spark_2.12-1.8.0.jar \
    -o /opt/airflow/jars/openlineage-spark-1.8.0.jar

USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir pyspark==3.5.2