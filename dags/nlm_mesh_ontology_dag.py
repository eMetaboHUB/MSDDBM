from airflow.models.dag import dag
from ingestion_tasks import *


# ATTENTION, EN TETE DE FICHIER, IMPORTS SPECIFIQUES AIRFLOW UNIQUEMENT


@dag(default_args=default_args,
     dag_id="nlm_mesh_ontology",
     tags=["ingestion", "nlm_mesh_ontology"],
     catchup=False,
     params={"local_temp_repository": "/opt/airflow/ingestion",
             "hdfs_data_dir": "/data",
             "hdfs_tmp_dir": "/tmp",
             "hdfs_connection_id": "hdfs_connection",
             "n_kept_versions": 1},
     max_active_runs=1,
     schedule="@weekly",
     )
def nlm_mesh_ontology_taskflow():
    """
    get nlm-mesh_ontology from
    """

    @task(task_id="get_nlm_mesh_ontology_info")
    def get_nlm_mesh_ontology_info(**kwargs):
        from fonctions import http_download, check_rdf_format_string
        import re
        page = http_download("https://nlmpubs.nlm.nih.gov/projects/mesh/").decode('UTF-8')
        version = re.findall("<li><a href=\"vocabulary_(.*.)\\.ttl\"> vocabulary_\\1\\.ttl</a></li>", page)[0]
        base_url = "https://nlmpubs.nlm.nih.gov/projects/mesh/"
        liste = ["vocabulary_" + version + ".ttl", ]

        from fonctions import write_info_file, get_dag_id
        info = {"download_url": base_url,
                "version": version,
                "file_list": liste,
                "dir_name": get_dag_id(),
                "compression": "",
                "format": check_rdf_format_string("TURTLE"),
                "ontology_namespace": "http://id.nlm.nih.gov/mesh/vocab#"
                }
        write_info_file(info)

    get_nlm_mesh_ontology_info() >> check_and_ingest()


nlm_mesh_ontology_taskflow()
