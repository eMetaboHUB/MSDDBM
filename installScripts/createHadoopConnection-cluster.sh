#!/bin/bash
./airflow.sh connections delete hdfs_connection

./airflow.sh connections add 'hdfs_connection' \
    --conn-json '{
        "conn_type": "hdfs",
        "login": "gulaisney",
        "password": "*****",
        "host": "147.100.175.223" ,
        "port": 9870
    }'

