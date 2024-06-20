#Image for Airflow workers
FROM apache/airflow
COPY requirements.txt .
RUN pip install --upgrade pip

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         gcc \
         heimdal-dev \
#        openjdk-17-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
USER airflow
#ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

RUN pip install -r requirements.txt 
RUN pip uninstall -y argparse
