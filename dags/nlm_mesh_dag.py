from airflow.models.dag import dag
from ingestion_tasks import *


# ATTENTION, EN TETE DE FICHIER, IMPORTS SPECIFIQUES AIRFLOW UNIQUEMENT


@dag(default_args=default_args,
     dag_id="nlm_mesh",
     tags=["ingestion", "nlm_mesh"],
     catchup=False,
     params={"local_temp_repository": "/opt/airflow/ingestion",
             "hdfs_data_dir": "/data",
             "hdfs_tmp_dir": "/tmp",
             "hdfs_connection_id": "hdfs_connection",
             "n_kept_versions": 1},
     max_active_runs=1,
     schedule="@weekly",
     )
def nlm_mesh_taskflow():
    """
    get nlm-mesh from https://nlmpubs.nlm.nih.gov/projects/mesh/rdf/mesh.nt.gz
    """

    @task(task_id="get_nlm_mesh_info")
    def get_nlm_mesh_info(**kwargs):
        from fonctions import http_download
        base_sha = http_download("https://nlmpubs.nlm.nih.gov/projects/mesh/rdf/mesh.nt.sha1").decode('UTF-8')
        version = "SHA_" + base_sha[0:8]
        if version == "SHA_":
            raise Exception(
                "Impossible de récupérer la version sur https://nlmpubs.nlm.nih.gov/projects/mesh/rdf/mesh.nt.sha1")
        base_url = "https://nlmpubs.nlm.nih.gov/projects/mesh/rdf/"
        liste = ["mesh.nt.gz"]

        from fonctions import write_info_file, check_rdf_format_string, get_dag_id
        info = {"download_url": base_url,
                "version": version,
                "file_list": liste,
                "dir_name": get_dag_id(),
                "compression": "gz",
                "format": check_rdf_format_string("NTRIPLES")
                }
        write_info_file(info)

    get_nlm_mesh_info() >> check_and_ingest()


nlm_mesh_taskflow()
