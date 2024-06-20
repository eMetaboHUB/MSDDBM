from airflow.models.dag import dag
from ingestion_tasks import *


# ATTENTION, EN TETE DE FICHIER, IMPORTS SPECIFIQUES AIRFLOW UNIQUEMENT


@dag(default_args=default_args,
     dag_id="skos",
     tags=["ingestion", "skos"],
     catchup=False,
     params={"local_temp_repository": "/opt/airflow/ingestion",
             "hdfs_data_dir": "/data",
             "hdfs_tmp_dir": "/tmp",
             "hdfs_connection_id": "hdfs_connection",
             "n_kept_versions": 1},
     max_active_runs=1,
     schedule="@weekly",
     )
def skos_taskflow():
    """
    get https://www.w3.org/2009/08/skos-reference/skos.rdf
    """

    @task(task_id="get_skos_info")
    def get_skos_info(**kwargs):
        from fonctions import http_download
        import re
        page = http_download("https://www.w3.org/2009/08/skos-reference/").decode('UTF-8')
        version = re.findall("</td><td><a href=\"skos\.rdf\">skos\.rdf</a></td><td align=\"right\">(.*)  </td>", page)[
                      0][0:10]
        base_url = "https://www.w3.org/2009/08/skos-reference/"
        liste = ["skos.rdf", ]

        from fonctions import write_info_file, check_rdf_format_string, get_dag_id
        info = {"download_url": base_url,
                "version": version,
                "file_list": liste,
                "dir_name": get_dag_id(),
                "compression": "",
                "format": check_rdf_format_string("RDFXML"),
                "ontology_namespace": "http://www.w3.org/2004/02/skos/core#"
                }
        write_info_file(info)

    get_skos_info() >> check_and_ingest()


skos_taskflow()
