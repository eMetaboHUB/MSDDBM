#!/bin/bash
# This development script is used to compile the API and run its main class as a Spark Job

USAGE='Expecting at least one argument: "scanGraphs" or "sparqlRequest" or "void" or "qa"'
if [ "$#" -lt 1 ]
then
  echo $USAGE >&2
  exit 1
fi

case $1 in
    scanGraphs|sparqlRequest|void|qa)
        ;;
    *)
        # The wrong first argument.
        echo $USAGE >&2
        exit 1
esac



export SBT_OPTS="-Xmx2048M -XX:+CMSClassUnloadingEnabled -Xss2M  -Duser.timezone=GMT"

sbt clean assembly && spark-submit  \
              --deploy-mode cluster \
              --driver-memory 2G \
              --executor-memory 1G \
              --num-executors 5 \
              --conf spark.yarn.appMasterEnv.JAVA_HOME="/usr/local/openjdk/jdk-12.0.2+10/" \
              --conf spark.executorEnv.JAVA_HOME="/usr/local/openjdk/jdk-12.0.2+10/"  \
              --conf spark.yarn.submit.waitAppCompletion="true" \
              --jars /usr/share/java/sansa-stack-spark_2.12-0.8.4_ExDistAD.jar \
              --class "fr.inrae.msd.Main" \
              assembly/msd-query.jar "$@"

