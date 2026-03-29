FROM apache/airflow:2.8.1

USER root

RUN apt-get update && \
    apt-get install -y openjdk-17-jdk procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir pyspark==3.5.2