# Metabolomic Semantic Datalake Database Management 

## General information
The code currently developed in this repository allows setting up a data lake on a Spark/Hadoop cluster containing reference graphs in metabolomics, as well as a system for analyzing and exploiting these graphs.

The ingestion and update part is implemented using a containerized version of Airflow, preconfigured to automatically ingest around twenty reference metabolomics graphs onto HDFS. During ingestion and update, metadata facilitating the exploitation of the graphs is generated. To install this system, refer to Doc/README-ingestion.md.

A Scala library and a command line tool are provided to facilitate access to the graphs present in the data lake, see Doc/README-API.md.

The addition of new knowledge graphs to the lake is documented in Doc/README-new-Airflow-Dag.md.

## Current datalake configuration

Graphs are available from Elrond (147.99.177.166) in /data :
```hdfs dfs -ls /data``` to list directories and metadata files.

Please do not add graphs manually, use Airflow and the API and its scripts to handle the datalake.
