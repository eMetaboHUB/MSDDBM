#!/bin/bash
./airflow.sh connections delete hdfs_connection

./airflow.sh connections add 'hdfs_connection' \
    --conn-json '{
        "conn_type": "hdfs",
        "login": "hadoop",
        "password": "p2m2",
        "host": "host.docker.internal" ,
        "port": 9870
    }'

