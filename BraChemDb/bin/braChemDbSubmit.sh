#!/bin/bash
# This script submits the API Main class as a Spark Job
# Created to test the API Spark functions or use them from Airflow

MSDQHOME=/usr/local/msd-database-management


JAVA_HOME_PATH="/usr/local/openjdk/jdk-12.0.2+10/"
JARS_PATH="/usr/share/java/sansa-stack-spark_2.12-0.8.4_ExDistAD.jar"
CONF_FILE_PATH="$MSDQHOME/msdQuery.conf"
APP_JAR_PATH="$MSDQHOME/apps/lib/BraChemDb-assembly-0.1.0-SNAPSHOT.jar"
MAIN_CLASS="fr.inrae.brachemdb.Main"

spark-submit \
  --deploy-mode cluster \
  --driver-memory 4G \
  --executor-memory 12G \
  --executor-cores 5 \
  --num-executors 48 \
  --conf "spark.yarn.submit.waitAppCompletion=true" \
  --conf "spark.yarn.appMasterEnv.JAVA_HOME=$JAVA_HOME_PATH" \
  --conf "spark.executorEnv.JAVA_HOME=$JAVA_HOME_PATH" \
  --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=log4j.properties -Dlog4j.debug=true" \
  --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=log4j.properties -Dlog4j.debug=true" \
  --conf spark.shuffle.service.enabled=true \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.speculation=true \
  --jars $JARS_PATH \
  --files $CONF_FILE_PATH \
  --class $MAIN_CLASS \
  $APP_JAR_PATH "$@"
