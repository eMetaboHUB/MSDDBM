from airflow.models.dag import dag
from ingestion_tasks import *

# ATTENTION, EN TETE DE FICHIER, IMPORTS SPECIFIQUES AIRFLOW UNIQUEMENT

@dag(default_args=default_args,
     dag_id="metanetx",
     tags=["ingestion", "metanetx"],
     params={"local_temp_repository": "/opt/airflow/ingestion",  # utilisé pour extraction des zips
             "hdfs_data_dir": "/data",  # Stockage des bases
             "hdfs_tmp_dir": "/tmp",  # Pour le fichier provisoire d'infos de téléchargement
             "hdfs_connection_id": "hdfs_connection", # connexion webhdfs préalablement configurée
             "n_kept_versions": 1},  # Nombre de versions à conserver sur le cluster
     catchup=False,
     schedule="@weekly",
     max_active_runs=1
     )
def metanetx_taskflow():
    @task(task_id="get_metanetx_db_info")
    def get_metanetx_db_info():
        from fonctions import write_info_file, http_download, check_rdf_format_string, get_dag_id
        # Extraction de la version en cours via le manuel
        manual = http_download("https://ftp.vital-it.ch/databases/metanetx/MNXref/latest/USERMANUAL.md")
        first_line_bytes = manual.splitlines()[0]
        first_line = first_line_bytes.decode('UTF-8')
        import re
        version = re.sub(".+([0-9]+.[0-9]+)", "\\1", first_line)
        liste = ["metanetx.ttl.gz", ]

        # Initialisation des infos de téléchargement
        info = {"download_url": "https://ftp.vital-it.ch/databases/metanetx/MNXref/latest/",
                "version": version,
                "file_list": liste,
                "dir_name": get_dag_id(),  # le nom du dossier dans le datalake
                "compression": "gz",  # le format de compression, pour l'étape de décompression
                "format": check_rdf_format_string("TURTLE")
                }

        write_info_file(info)

    get_metanetx_db_info() >> check_and_ingest()


metanetx_taskflow()
