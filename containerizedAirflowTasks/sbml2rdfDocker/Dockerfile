FROM inraep2m2/sbml2rdf
USER root

RUN apk update && apk add --no-cache python3
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Update pip and install necessary packages as root
RUN pip install --upgrade pip setuptools
RUN pip install hdfs docopt urllib3 six idna charset-normalizer certifi requests

# Change ownership of the virtual environment to the airflow user
RUN adduser -D airflow
RUN chown -R airflow:airflow /opt/venv

USER airflow

COPY hdfsSBML2RDF.sh /
COPY sbml2rdf.py /
ENTRYPOINT ["/hdfsSBML2RDF.sh"]

