FROM apache/airflow:2.8.4

USER root
RUN apt-get update && apt-get install -y default-jre && apt-get clean
USER airflow

# Install the Apache Spark provider package using pip
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark


