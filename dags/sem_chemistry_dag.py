from airflow.models.dag import dag
from ingestion_tasks import *


# ATTENTION, EN TETE DE FICHIER, IMPORTS SPECIFIQUES AIRFLOW UNIQUEMENT


@dag(default_args=default_args,
     dag_id="sem_chem",
     tags=["ingestion", "semantic_chemistry", "ontology"],
     catchup=False,
     params={"local_temp_repository": "/opt/airflow/ingestion",
             "hdfs_data_dir": "/data",
             "hdfs_tmp_dir": "/tmp",
             "hdfs_connection_id": "hdfs_connection",
             "n_kept_versions": 1},
     max_active_runs=1,
     schedule="@weekly",
     )
def sem_chem_ontology_taskflow():
    """
    get semantic chemistry
    """

    @task(task_id="get_sem_chem_info")
    def get_sem_chem_info(**kwargs):
        from fonctions import http_download
        import re
        page = http_download(
            "https://raw.githubusercontent.com/egonw/semanticchemistry/master/ontology/cheminf.owl").decode('UTF-8')
        version = re.findall("<owl:versionInfo rdf:datatype=\"&xsd;string\">(.*)<\/owl:versionInfo>", page)[0]
        base_url = "https://raw.githubusercontent.com/egonw/semanticchemistry/master/ontology/"
        liste = ["cheminf.owl", ]

        from fonctions import write_info_file, check_rdf_format_string, get_dag_id
        info = {"download_url": base_url,
                "version": version,
                "file_list": liste,
                "dir_name": get_dag_id(),
                "compression": "",
                "format": check_rdf_format_string("RDFXML"),
                "ontology_namespace": "http://semanticscience.org/resource/"
                }
        write_info_file(info)

    get_sem_chem_info() >> check_and_ingest()


sem_chem_ontology_taskflow()
