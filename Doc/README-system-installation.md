# Airflow for the Metabolomic Semantic Data Lake
This document covers the installation of Airflow. To add new databases, refer to Doc/README-new-Airflow-DAGs.md.

## Prerequisites

### Hadoop
The system requires sbt and a Hadoop server. To install the latter on Debian 11, see:
[How to Install Hadoop on Debian 11](https://www.rosehosting.com/blog/how-to-install-hadoop-on-debian-11/)

The Hadoop configuration must enable webHDFS exchanges. The parameter is located in hdfs-site.xml.
```xml
<property>
	<name>dfs.webhdfs.enabled</name>
	<value>true</value>
</property>  
```

To use Airflow locally, hdfs-site-localdocker is provided as an example.
You also need to update the /etc/hosts file on the machine by adding the entry:
```
127.0.0.1	host.docker.internal
```

### Java 12

Spark 3.1.2 makes use of now illegal instructions in JVMs newer than 12.  
To install Java 12 on a debian 12 machine :
```shell
wget https://download.java.net/java/GA/jdk12.0.2/e482c34c86bd4bf8b56c0b35558996b9/10/GPL/openjdk-12.0.2_linux-x64_bin.tar.gz
tar zxvf openjdk-12.0.2_linux-x64_bin.tar.gz
sudo mv jdk-12.0.2/ /usr/lib/jvm/
sudo update-alternatives --install /usr/bin/java java /usr/lib/jvm/jdk-12.0.2/bin/java 1
sudo update-alternatives --install /usr/bin/javac java /usr/lib/jvm/jdk-12.0.2/bin/javac 1
sudo update-alternatives --config java #Select java 12 as default
sudo update-alternatives --config javac  #Select javac 12 as default
```


## Installation

The provided docker-compose.yaml file allows launching Airflow on the cluster.
The created account has the default login "airflow" and password "airflow".

To initialize the system:

1) Configure the script createHadoopConnection-cluster.sh with the Hadoop server information (or createHadoopConnection-localdocker.sh for local testing) and
In docker-compose.yaml, replace 123 with the docker group ID (cat /etc/group | grep docker)
```yaml
group_add:
    - "123" # for dockeroperator - access to /var/run/docker.sock
```

2) Configure the Hadoop connection in the following files:
```
createHadoopConnection-cluster.sh
createSSHConnection-cluster.sh

# or, for a local test configuration :
createHadoopConnection-localdocker.sh
createSSHConnection--localdocker.sh
```


3) Run the Airflow initialization script
```
sudo ./msd-database-management_install.sh

# or for local testing :
sudo ./msd-database-management_install.sh --local
```

This script creates the directories ./dags ./logs ./plugins ./config ./msddbm
It sets the AIRFLOW_UID environment variable in ./.env
It launches docker compose up airflow-init for system initialization (especially postgres).
Finally, the script starts Airflow (docker compose up -d).

Once Airflow is running, the script calls another script that establishes the connection between Airflow and Hadoop:
```
./createHadoopConnection-cluster.sh
```
For local tests on a Dockerized Hadoop, you can take inspiration from the parameters of:
```
./createHadoopConnection-localdocker.sh
```

Then the same kind of script is called to create the SSH connection between airflow and the Spark cluster.
```shell
  ./createSSHConnection-cluster.sh
```

Finally, the script builds the Docker images of the relevant operators:
```
./buildDockerOperatorImages.sh 
```

In order to run Spark jobs, the MSDDBM API has to be deployed. See Doc/README-API.md

## Launch
To launch Airflow:

```
docker compose up -d
```

## Usage
The DAGs managed by Airflow are located in the dags/ directory.

The web interface is open locally on port 8079.

The airflow.sh script allows running Airflow commands in the CLI. Run:
```
./airflow.sh cheat-sheet
```
to get the list of available commands.

## Reset
If necessary, to start over from scratch:
```
./msd-database-management_install.sh --reset
```