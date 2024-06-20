#!/bin/bash
export SBT_OPTS="-Xms512M -Xmx2G -Xss2M -XX:MaxMetaspaceSize=1G"
# This script installs the app on the cluster
MSDQUERYHOME=/usr/local/msd-database-management

#sbt publishLocal || { echo 'local sbt publication failed, exiting' ; exit 1; }
sbt assembly || { echo 'assembly failed, exiting' ; exit 1; }


echo "####################################################"
echo deploying app to $MSDQUERYHOME/apps/
sudo -u root mkdir -p $MSDQUERYHOME/apps/bin/
sudo -u root mkdir -p $MSDQUERYHOME/apps/lib/

ASSYFILE=target/scala-2.12/BraChemDb-assembly-0.1.0-SNAPSHOT.jar
if [ ! -f "$ASSYFILE" ]; then
    echo "$ASSYFILE does not exist. Maybe something went wrong during compilation or assembly. Stopping"
    exit 1
fi
sudo -u root cp $ASSYFILE $MSDQUERYHOME/apps/lib/
#cp $ASSYFILE ../msddbm/ # for Airflow operators
sudo -u root cp bin/* $MSDQUERYHOME/apps/bin/
sudo chmod a+x $MSDQUERYHOME/apps/bin/braChemDbSubmit.sh
#sudo -u root cp src/main/resources/* $MSDQUERYHOME/
#cp src/main/resources/* ../msddbm/
#echo $VERSION > version
#sudo -u root cp version  $MSDQUERYHOME/


echo "Done."
echo "####################################################"
