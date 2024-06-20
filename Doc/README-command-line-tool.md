# Command-line interface tool for the Metabolomic Semantic Data Lake

The Main class of the API provides utilities to explore the MSD.
It can be called directly from a fat jar or from indirectly via a script.
Here is the command for the jar :
```shell
java -jar msddbm-assembly-<version>.jar <arguments> -c configFile
```
The msddbm script should be deployed alongside with the jar in /usr/local/msd-database-management. It takes care of the "java -jar" command, gets the last version and config file details :
```shell
msddbm <arguments>
```
Without any argument, the program will print its usage.
Let's see its arguments' details.

## List MSD graphs
There are two commands to list available graphs on the datalake. 
They both scan the metadata available on HDFS for names, versions and serialization format of the graphs.
the listGraphs command outputs the latest versions only, while listAllGraphs retrieves the full content of the MSD.
```shell
msddbm listGraphs
#or
msddbm listAllGraphs
```

Here is an output of listGraphs :
```shell
$ msddbm listGraphs
chebi 2024-02-01 (RDFXML) 
chemOnt 2_1 (OBO) 
chembl 33.0 (TURTLE) 
dublin_core_dcmi_terms 2020-01-20 (NTRIPLES) 
hmdb 2021-11-17 (XML) 
knapsack_scrap N63723 (TURTLE) 
metanetx 4.4 (TURTLE) 
ncbitaxon 2023-12-12 (RDFXML) 
nlm_mesh SHA_4bc58657 (NTRIPLES) 
nlm_mesh_ontology 0.9.3 (TURTLE) 
pubchem_compound_general 2024-02-03 (TURTLE) 
pubchem_descriptor_compound_general 2023-12-09 (TURTLE) 
pubchem_inchikey 2024-02-03 (TURTLE) 
pubchem_reference 2023-12-09 (TURTLE) 
pubchem_substance 2023-12-09 (TURTLE) 
pubchem_synonym 2024-02-03 (TURTLE) 
pubchem_void 2024-02-03 (TURTLE) 
sem_chem 2.0 (RDFXML) 
skos 2011-08-06 (RDFXML) 
spar_cito 2.8.1 (TURTLE) 
spar_fabio 2.2 (TURTLE) 
test A (TURTLE) 
unit_test_dag A (TURTLE) 

Number of graphs listed : 23

```

## Display MSD graphs sizes
Graph sizes, expressed in number of triples, can be listed with the following command :
```shell
msddbm gs
chebi 2024-03-01, RDFXML, 6860047 triples, ontology
chembl 33.0, TURTLE, 671767450 triples, instance graph
[...]

Number of graphs listed : 23

Latest ontologies total: 2.43e+07 triples
Latest instance graphs total: 7.64e+09 triples
Global : 7.67e+09 triples
```
The size is retrieved from the VoID companion graph generated with Spark on HDFS during the ingestion, 
so this command may be slower than the simpler listGraphs. 



## List instance or ontological graphs only
Graphs having no field ```onlology_namespace``` in their JSON metadata file are considered to be instance graphs, 
and will be listed with the command: 
```msddbm lsi```
On the contrary, ontologies can be listed with :
```msddbm lso```

## Display graph metadata
During the ingestion, some information is written to manage the graph. 
This information can be retrieved in JSON syntax for a given graph name with the following command :
```shell
msddbm metadata -n <graphName> [-v <graphVersion]
```
Please note that ```-n <graphNName> [-v <graphVersion]``` options work with every command targeting a specific graph.

Example output :
```shell
$ msddbm metadata -gn spar_fabio
{"download_url":"https://sparontologies.github.io/fabio/current/","version":"2.2","file_list":["fabio.ttl"],"dir_name":"spar_fabio","compression":"","format":"TURTLE","date_importation_msd":"2024-02-05T22:34:28.884117","actual_file_list":["fabio.ttl"]}
```

## Display graph VoID statistics

The VoID turtle file of a graph can be displayed with the stat command.
Please note that some graphs may not have been analysed (because of their format for example). 
In this case the command will end with an error.
Here is an example (shortened for clarity) :
```shell
$ msddbm stats -n sem_chem
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
                    @prefix void: <http://rdfs.org/ns/void#> .
                   [...]
<http://stats.lod2.eu/rdf/void/?source=/data/sem_chem_v2.0>

void:classes 10;
void:classPartition [ 
void:class <http://www.w3.org/2002/07/owl#Class>; 
void:triples 351;
],[ 
void:class <http://www.w3.org/2002/07/owl#Restriction>; 
void:triples 203;
],[ 
void:class <http://www.w3.org/2002/07/owl#ObjectProperty>; 
void:triples 46;
],[...]
void:property <http://purl.org/dc/elements/1.1/language>; 
void:triples 1;
];
void:vocabulary  <http://purl.org/ontology/bibo/>, <http://www.semanticweb.org/ontologies/>, [...];
a void:Dataset .


```

## Find graphs with given classes or properties
findProp and findClass commands take respectively a list of RDF property or classes, 
and print a list of graph using each of them simultaneously.
The list should be given between double quotes.
Please note that the filter looks for strings _ending_ with the given class or property names.
```shell
$ msddbm findClass -l "#Datatype"

4 graphs containing following class: #Datatype 

dublin_core_dcmi_terms 2020-01-20 (located in /data/dublin_core_dcmi_terms_v2020-01-20)
Class http://www.w3.org/2000/01/rdf-schema#Datatype found 12 time(s)

sem_chem 2.0 (located in /data/sem_chem_v2.0)
Class http://www.w3.org/2000/01/rdf-schema#Datatype found 2 time(s)
Class http://www.w3.org/2002/07/owl#DatatypeProperty found 6 time(s)

spar_cito 2.8.1 (located in /data/spar_cito_v2.8.1)
Class http://www.w3.org/2000/01/rdf-schema#Datatype found 1 time(s)
Class http://www.w3.org/2002/07/owl#DatatypeProperty found 3 time(s)

spar_fabio 2.2 (located in /data/spar_fabio_v2.2)
Class http://www.w3.org/2000/01/rdf-schema#Datatype found 5 time(s)
Class http://www.w3.org/2002/07/owl#DatatypeProperty found 67 time(s)
```

Here is another example with a list of two classes :
```shell
$msddbm findClass -l "#Datatype,#Property"

1 graph containing following classes: 
#Datatype #Property 

dublin_core_dcmi_terms 2020-01-20 (located in /data/dublin_core_dcmi_terms_v2020-01-20)
Class http://www.w3.org/2000/01/rdf-schema#Datatype found 12 time(s)
Class http://www.w3.org/1999/02/22-rdf-syntax-ns#Property found 55 time(s)
```

## List RDF files of a graph

The listFilesUris command prints a list of HDFS URIs of the graph's files

```shell
$ msddbm listFilesUris -n pubchem_inchikey
hdfs://147.100.175.223:9000/data/pubchem_inchikey_v2024-02-03/pc_inchikey2compound_000001.ttl
hdfs://147.100.175.223:9000/data/pubchem_inchikey_v2024-02-03/pc_inchikey2compound_000002.ttl
hdfs://147.100.175.223:9000/data/pubchem_inchikey_v2024-02-03/pc_inchikey2compound_000003.ttl
hdfs://147.100.175.223:9000/data/pubchem_inchikey_v2024-02-03/pc_inchikey2compound_000004.ttl
hdfs://147.100.175.223:9000/data/pubchem_inchikey_v2024-02-03/pc_inchikey2compound_000005.ttl
[...]
```

listFilesPaths prints the HDFS absolute paths of the files
```shell
$ msddbm listFilesPaths -n pubchem_inchikey
/data/pubchem_inchikey_v2024-02-03/pc_inchikey2compound_000001.ttl
/data/pubchem_inchikey_v2024-02-03/pc_inchikey2compound_000002.ttl
/data/pubchem_inchikey_v2024-02-03/pc_inchikey2compound_000003.ttl
/data/pubchem_inchikey_v2024-02-03/pc_inchikey2compound_000004.ttl
/data/pubchem_inchikey_v2024-02-03/pc_inchikey2compound_000005.ttl
[...]
```

listFilesNames gets only base names :
```shell
$ msddbm listFilesNames -n pubchem_inchikey
pc_inchikey2compound_000001.ttl
pc_inchikey2compound_000002.ttl
pc_inchikey2compound_000003.ttl
pc_inchikey2compound_000004.ttl
pc_inchikey2compound_000005.ttl
[...]
```

## Spark job for VoID statistics
Although it appears in the help, the void command in not meant to be run from the shell, but as a spark-submit command.
It is wrapped in the msdSparkJob script, in /usr/local/msd-database-management/bin directory.
Here is how the script calls the tool. 

```shell
spark-submit  \
              --deploy-mode cluster \
              --driver-memory 4G \
              --executor-memory 12G \
              --num-executors 48 \
              --conf spark.yarn.submit.waitAppCompletion="true" \
              --jars /usr/share/java/sansa-stack-spark_2.12-0.8.4_ExDistAD.jar \
              --conf spark.yarn.appMasterEnv.JAVA_HOME="/usr/local/openjdk/jdk-12.0.2+10/" \
              --conf spark.executorEnv.JAVA_HOME="/usr/local/openjdk/jdk-12.0.2+10/"  \
              --files $MSDQHOME/msdQuery.conf \
              --class "fr.inrae.msd.Main" \
              $MSDQHOME/lib/msddbm-assembly-$VERSION.jar  "$@" -c msdQuery.conf
```

The number of executors and their amount of attributed memory can be approximated with the help of the spreadsheet  in
"Doc/SparkResourcesCalc.ods" (it takes the cluster's total memory, nodes and cores as input). 
