#!/bin/bash
cd pupDocker || exit 1
docker build -f Dockerfile -t docker_pup_task .

cd ../knapsackDocker || exit 1
sbt docker:publishLocal

cd ../sbml2rdfDocker || exit 1
docker build -f Dockerfile -t docker_sbml2rdf_task .


