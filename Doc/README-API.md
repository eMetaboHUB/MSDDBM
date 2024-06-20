# API for the Metabolomic Semantic Data Lake

#  Cluster deployment

Run the API compilation and deployment script:
```bash
cd API # necessary for sbt
./deploy.sh
```

This script installs the following files:

```
/usr/local/msd-database-management/
├── bin
│ ├── msddbm
│ ├── msdSparkJob
│ └── version
├── lib
│ └── msddbm-assembly-<version>.jar
├── msdQuery.conf
└── version

```

## Environment variables

Important : the airflow MSD setup uses ```msdSparkJob``` to trigger spark jobs via SSH. 
the following variables have to be set in the ```.profile``` file of the ssh user (but not their ```.bashrc```) :

```shell
export PATH=$PATH:/usr/local/msd-database-management/bin
export SPARK_HOME=...
export PATH=$PATH:$SPARK_HOME/bin
export HADOOP_CONF_DIR=...
```

## Bash helpers
The `msddbm` script utilizes the Main class of the JAR file to explore the data lake. See README-API-CLI.md for details.

The other script, `msdSparkJob`, allows launching Spark jobs (VoID calculation only for now).

For example, to generate the void of the graph `dublin_core_dcmi_terms` in the directory `/data/dublinCoreVoid/`:

```bash
msdSparkJob void -n dublin_core_dcmi_terms -od /data/dublinCoreVoid/
```

The `/usr/local/msd-database-management/version` file contains the version number of the JAR used by 
`msddbm` and `msdSparkJob`. It is created by `deploy.sh` based on the version number located in `API/build.sbt`.



## Scala API documentation

The scala API documentation can be generated with ```sbt doc``` and then found at 
```API/target/scala-2.12/api/fr/inrae/msd/index.html```.

## Wrapper usage example

A wrapper is provided in order to use the API easily. It gives access to graph triples and to a Datalake object, 
allowing to explore the data lake programmatically.
### Example
First, open a Spark shell providing the API jar (/usr/local/msd-database-management/lib/msddbm-assembly-0.1.0.jar), for example : 
```shell
export JAVA_HOME=/usr/local/openjdk/jdk-12.0.2+10/
spark-shell --name MSD_API_Test \
--conf "spark.serializer=org.apache.spark.serializer.KryoSerializer"  \
--conf "spark.sql.crossJoin.enabled=true"   \
--conf "spark.kryo.registrator=net.sansa_stack.rdf.spark.io.JenaKryoRegistrator,\
net.sansa_stack.query.spark.ontop.OntopKryoRegistrator,\
net.sansa_stack.query.spark.sparqlify.KryoRegistratorSparqlify"  \
--conf "spark.kryoserializer.buffer.max=1024"   \
--executor-memory 12g \
--driver-memory 4g \
--num-executors 38 \
--conf spark.yarn.appMasterEnv.JAVA_HOME="/usr/local/openjdk/jdk-12.0.2+10/" \
--conf spark.executorEnv.JAVA_HOME="/usr/local/openjdk/jdk-12.0.2+10/" \
--jars /usr/share/java/spray-json_2.12.jar,/usr/share/java/sansa-stack-spark_2.12-0.8.0-RC3-SNAPSHOT-jar-with-dependencies.jar,/usr/local/msd-database-management/lib/msddbm-assembly-[version].jar
```


Import the API, then initialize a wrapper object :
  ```scala
  import fr.inrae.msd._
  val wrapper = MsdWrapper(spark)
  ```

It is now possible to access the data lake. 
Let's get the latest graphs list and print their names :
```scala
val graphs = wrapper.dataLake.getLatestGraphs
graphs.map(_.dirname).foreach(println)
```
Here is the result : 
```shell
chebi
chemOnt
chembl
dublin_core_dcmi_terms
hmdb
knapsack_scrap
metanetx
ncbitaxon
nlm_mesh
nlm_mesh_ontology
pubchem_compound_general
pubchem_descriptor_compound_general
pubchem_inchikey
pubchem_reference
pubchem_substance
pubchem_synonym
pubchem_void
sem_chem
skos
spar_cito
spar_fabio
test
unit_test_dag
```

Now we can choose a graph from the list, for example to get its files :
```scala
val synonym= wrapper.dataLake.getGraph("pubchem_synonym")
val uris = synonym.getFilesUris
uris.foreach(println)
```
This will print the following : 
```scala
hdfs://147.100.175.223:9000/data/pubchem_synonym_v2024-02-03/pc_synonym2compound_000001.ttl
hdfs://147.100.175.223:9000/data/pubchem_synonym_v2024-02-03/pc_synonym2compound_000002.ttl
hdfs://147.100.175.223:9000/data/pubchem_synonym_v2024-02-03/pc_synonym2compound_000003.ttl
hdfs://147.100.175.223:9000/data/pubchem_synonym_v2024-02-03/pc_synonym2compound_000004.ttl
[...]
```

For Sansa usage, the getTriples function directly returns thes files contents as an RDD :
```scala
val triples = wrapper.getTriples("pubchem_synonym")
```

We can now use the triples. For example :
```scala
triples.take(10).map(_.getPredicate)
```
```scala
res0: Array[org.apache.jena.graph.Node] = Array(http://semanticscience.org/resource/SIO_000011, http://semanticscience.org/resource/SIO_000011, http://semanticscience.org/resource/SIO_000011, http://semanticscience.org/resource/SIO_000011, http://semanticscience.org/resource/SIO_000011, http://semanticscience.org/resource/SIO_000011, http://semanticscience.org/resource/SIO_000011, http://semanticscience.org/resource/SIO_000011, http://semanticscience.org/resource/SIO_000011, http://semanticscience.org/resource/SIO_000011)
```

To load only a limited part of the graph, it is possible to use the overloaded getTriples function :
```scala
val files = synonym.getFiles // 46 items
val filteredFiles = files.filter(_.contains("type")) // 17 files holding substring "type"
val triples = wrapper.getSubGraphTriples("pubchem_synonym","",filteredFiles)

```

