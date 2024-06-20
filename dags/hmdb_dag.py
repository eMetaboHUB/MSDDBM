from airflow.models.dag import dag
from ingestion_tasks import *


# ATTENTION, EN TETE DE FICHIER, IMPORTS SPECIFIQUES AIRFLOW UNIQUEMENT


@dag(default_args=default_args,
     dag_id="hmdb",
     tags=["ingestion", "hmdb"],
     catchup=False,
     schedule="@weekly",
     params={"local_temp_repository": "/opt/airflow/ingestion",
             "hdfs_data_dir": "/data",
             "hdfs_tmp_dir": "/tmp",
             "hdfs_connection_id": "hdfs_connection",
             "n_kept_versions": 1},
     max_active_runs=1
     )
def hmdb_taskflow():
    """
    Download hmdb from https://hmdb.ca/system/downloads/current/hmdb_metabolites.zip
    """

    @task(task_id="get_hmdb_info")
    def get_hmdb_info(**kwargs):
        ti = kwargs['task_instance']
        import re
        from fonctions import http_download, get_dag_id
        version_source = http_download("https://hmdb.ca/downloads").decode('UTF-8')
        version = re.findall("All Metabolites</td><td>(\d{4}-\d{2}-\d{2})", version_source)[2]

        base_url = "https://hmdb.ca/system/downloads/current/"
        liste = ["hmdb_metabolites.zip", ]

        from fonctions import write_info_file
        info = {"download_url": base_url,
                "version": version,
                "file_list": liste,
                "dir_name": get_dag_id(),
                "compression": "zip",
                "format": "XML"
                }
        write_info_file(info)

    get_hmdb_info() >> check_and_ingest()


hmdb_taskflow()
