from airflow.models.dag import dag
from ingestion_tasks import *


# ATTENTION, EN TETE DE FICHIER, IMPORTS SPECIFIQUES AIRFLOW UNIQUEMENT

@dag(default_args=default_args,
     dag_id="chemOnt",
     tags=["ingestion", "chemont"],
     params={"local_temp_repository": "/opt/airflow/ingestion",
             "hdfs_data_dir": "/data",
             "hdfs_tmp_dir": "/tmp",
             "hdfs_connection_id": "hdfs_connection",
             "n_kept_versions": 1},
     catchup=False,
     schedule="@weekly",
     max_active_runs=1
     )
def chemont_taskflow():
    @task(task_id="get_chemont_db_info")
    def get_chemont_db_info():
        from fonctions import write_info_file, get_dag_id
        liste = ["ChemOnt_2_1.obo.zip"]
        info = {"download_url": "http://classyfire.wishartlab.com/system/downloads/1_0/chemont/",
                "version": "2_1",
                "file_list": liste,
                "dir_name": get_dag_id(),
                "compression": "zip",
                "format": "OBO"}
        write_info_file(info)

    get_chemont_db_info() >> check_and_ingest()


chemont_taskflow()
