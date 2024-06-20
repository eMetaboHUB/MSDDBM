FROM roychri/pup
USER root

RUN apk update \
ENV PYTHONUNBUFFERED=1
RUN apk add --update --no-cache python3 && ln -sf python3 /usr/bin/python
RUN python3 -m ensurepip
RUN pip3 install --no-cache --upgrade pip setuptools


RUN adduser -D airflow

USER airflow
RUN pip install hdfs
COPY hdfspup.sh /
COPY pup_it.py /
ENTRYPOINT ["/hdfspup.sh"]
