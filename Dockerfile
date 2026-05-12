FROM apache/airflow:2.9.1-python3.10

USER root

# Install OpenJDK 17 for Spark
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
           openjdk-17-jre-headless \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s /usr/lib/jvm/java-17-openjdk-$(dpkg --print-architecture) /usr/lib/jvm/java-17-openjdk

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk

USER airflow

# Install PySpark, Iceberg AWS bundle, and testing tools
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
