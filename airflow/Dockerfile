FROM apache/airflow:2.10.4-python3.10

USER root
RUN apt-get update && \
    apt-get install -y gcc python3-dev openjdk-17-jdk && \
    apt-get clean

# Set JAVA_HOME environment variable
USER airflow
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64

RUN pip install --no-cache-dir apache-airflow==2.10.4 apache-airflow-providers-apache-spark==4.1.0 pyspark==4.1.0