#!/bin/bash
export SBT_OPTS="-Xms512M -Xmx2G -Xss2M -XX:MaxMetaspaceSize=1G"
# This script installs locally the API
# TODO add deployment to sbt/maven repo
MSDQUERYHOME=/usr/local/msd-database-management
mkdir -p lib/
#cp libToInstall/*.jar lib/
sbt publishLocal || { echo 'local sbt publication failed, exiting' ; exit 1; }
sbt assembly || { echo 'assembly failed, exiting' ; exit 1; }

VERSIONFILE=target/scala-2.12/classes/msdDeployement/version
if [ ! -f "$VERSIONFILE" ]; then
    echo "$VERSIONFILE does not exist. Check this script and build.sbt content. Stopping"
    exit 1
fi
source target/scala-2.12/classes/msdDeployement/version

echo "####################################################"
echo deploying API version $VERSION to $MSDQUERYHOME
sudo -u root mkdir -p $MSDQUERYHOME/bin/
sudo -u root mkdir -p $MSDQUERYHOME/lib/

ASSYFILE=target/scala-2.12/msddbm-assembly-$VERSION.jar
if [ ! -f "$ASSYFILE" ]; then
    echo "$ASSYFILE does not exist. Maybe something went wrong during compilation or assembly. Stopping"
    exit 1
fi
sudo -u root cp $ASSYFILE $MSDQUERYHOME/lib/ #for command-line queries
#cp $ASSYFILE ../msddbm/ # for Airflow operators
sudo -u root cp bin/* $MSDQUERYHOME/bin/
sudo -u root cp src/main/resources/* $MSDQUERYHOME/
#cp src/main/resources/* ../msddbm/
echo $VERSION > version
sudo -u root cp version  $MSDQUERYHOME/


echo "Done."
echo "####################################################"
