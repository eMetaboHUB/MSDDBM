from airflow.models.dag import dag
from ingestion_tasks import *


# ATTENTION, EN TETE DE FICHIER, IMPORTS SPECIFIQUES AIRFLOW UNIQUEMENT


@dag(default_args=default_args,
     dag_id="spar_cito",
     tags=["ingestion", "sparontologies", "cito"],
     catchup=False,
     params={"local_temp_repository": "/opt/airflow/ingestion",
             "hdfs_data_dir": "/data",
             "hdfs_tmp_dir": "/tmp",
             "hdfs_connection_id": "hdfs_connection",
             "n_kept_versions": 1},
     max_active_runs=1,
     schedule="@weekly",
     )
def spar_cito_ontology_taskflow():
    """
    get https://sparontologies.github.io/cito/current/cito.ttl
    """

    @task(task_id="get_spar_cito_info")
    def get_spar_cito_info(**kwargs):
        from fonctions import http_download
        import re
        page = http_download("https://sparontologies.github.io/cito/current/cito.ttl").decode('UTF-8')
        version = re.findall("owl:versionInfo \"(.*)\"\^\^xsd:string", page)[0]
        base_url = "https://sparontologies.github.io/cito/current/"
        liste = ["cito.ttl", ]

        from fonctions import write_info_file, check_rdf_format_string, get_dag_id
        info = {"download_url": base_url,
                "version": version,
                "file_list": liste,
                "dir_name": get_dag_id(),
                "compression": "",
                "format": check_rdf_format_string("TURTLE"),
                "ontology_namespace": "http://purl.org/spar/cito/"
                }
        write_info_file(info)

    get_spar_cito_info() >> check_and_ingest()


spar_cito_ontology_taskflow()
