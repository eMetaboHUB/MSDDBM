# Local Apache Spark setup

In order to test the system on a single machine with tiny graphs, it is possible to configure Spark locally.

1) Download Spark sources or binaries at ```https://spark.apache.org/downloads.html```

2) Decompress and, if needed, compile with ```./build/mvn -DskipTests clean package```
3) add these lines to your .bashrc and source it
```bash
export SPARK_HOME=your-path-to/spark-3.3.4
export PATH=$PATH:$SPARK_HOME/bin
export HADOOP_CONF_DIR=/your-path-to-hdfs-site.xml
```  

4) Launch ```sbin/start-master.sh```
5) Configure your local MSD in ```/usr/local/msd-database-management/msdQuery2.conf``` 
6) You should be able to run the ```localMsdSparkJob``` script.