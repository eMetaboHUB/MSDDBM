#!/bin/bash
./airflow.sh connections delete msd_ssh_connection
./airflow.sh connections add 'msd_ssh_connection' --conn-json '{"conn_type": "ssh","login": "guillaume","password": "****","host": "host.docker.internal"}'
