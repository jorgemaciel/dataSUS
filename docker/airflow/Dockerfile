FROM apache/airflow:2.7.1-python3.11

USER root
RUN apt-get update && \
    apt-get install -y git gcc python3-dev openjdk-17-jdk make sudo && \
    apt-get clean

# Set JAVA_HOME environment variable
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-arm64

ENV AIRFLOW__CORE__LOAD_EXAMPLES            False
ENV AIRFLOW__CORE__EXECUTOR                 LocalExecutor
ENV AIRFLOW__DATABASE__SQL_ALCHEMY_CONN     postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
ENV AIRFLOW__WEBSERVER_BASE_URL             http://localhost:9090
ENV AIRFLOW__WEBSERVER__SECRET_KEY          46BKJoQYlPPOexq0OhDZnIlNepKFf87WFwLbfzqDDho=

ADD ./pysus /home/airflow/pysus
ADD ./datasus/ingest.py /home/airflow/pysus 

WORKDIR /home/airflow/pysus

RUN make install

USER airflow
RUN pip install apache-airflow apache-airflow-providers-apache-spark pyspark==3.5.1 minio setuptools pandas tqdm

