#!/bin/bash

if [ "$1" == "--help" ]
then
  echo "$0 --reset to erase postgres DB content + docker images)"
  echo "$0 --local to run local hadoop and Spark connections setup (instead of cluster mode)"
  exit 0
fi

echo "#######################"
echo "lancement de l'installation et la configuration d'Airflow pour MetaboHUB"
echo "#######################"

if [ "$1" == "--reset" ]
then
  docker compose down --volumes --rmi all
fi

mkdir -p ./dags ./logs ./plugins ./config ./ingestion

mkdir -p /data/ingestion
ln -s /data/ingestion/ "$(pwd)/ingestion"
ln -s /data/ingestion/  /opt/airflow/ingestion


echo -e "AIRFLOW_UID=$(id -u)" > .env

docker compose up airflow-init

docker compose up -d
if [ "$1" == "--local" ]
then
  ./containerizedAirflowTasks/createHadoopConnection-localdocker.sh
  ./containerizedAirflowTasks/createSSHConnection-localdocker.sh
  #./createSparkConnection-localdocker.sh
else
  ./containerizedAirflowTasks/createHadoopConnection-cluster.sh
  ./containerizedAirflowTasks/createSSHConnection-cluster.sh
  #./containerizedAirflowTasks/createSparkConnection-cluster.sh
fi


./containerizedAirflowTasks/buildDockerOperatorImages.sh

#cp ./sansa-stack-spark_2.12-0.8.4_ExDistAD.jar ./msddbm/
cp API/libToInstall/sansa-stack-spark_2.12-0.8.0-RC3-SNAPSHOT-jar-with-dependencies.jar /usr/share/java/
