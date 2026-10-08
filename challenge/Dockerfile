FROM apache/airflow:2.10.5

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

RUN mkdir -p /opt/airflow/jars \
    && curl -L \
       https://jdbc.postgresql.org/download/postgresql-42.7.4.jar \
       -o /opt/airflow/jars/postgresql.jar \
    && chown -R airflow:root /opt/airflow/jars

USER airflow

RUN pip install --no-cache-dir pyspark==3.5.1