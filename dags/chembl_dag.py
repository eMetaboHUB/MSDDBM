from airflow.models.dag import dag
from ingestion_tasks import *


# ATTENTION, EN TETE DE FICHIER, IMPORTS SPECIFIQUES AIRFLOW UNIQUEMENT


@dag(default_args=default_args,
     dag_id="chembl",
     tags=["ingestion", "chembl"],
     catchup=False,
     schedule="@weekly",
     params={"local_temp_repository": "/opt/airflow/ingestion",
             "hdfs_data_dir": "/data",
             "hdfs_tmp_dir": "/tmp",
             "hdfs_connection_id": "hdfs_connection",
             "n_kept_versions": 1},
     max_active_runs=1
     )
def chembl_taskflow():
    """
    Download chembl from https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBL-RDF/latest/
    """

    @task(task_id="get_chembl_info")
    def get_chembl_info(**kwargs):
        ti = kwargs['task_instance']
        version_source = ti.xcom_pull(key='return_value', task_ids='get_chembl_version')
        import re
        version = re.findall("[0-9]+.[0-9\\-]+", version_source)[0]
        base_url = "https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBL-RDF/latest/"
        from fonctions import http_download
        listebrute = http_download(base_url).decode('UTF-8')
        liste_liens = re.findall(r'a href=\"(\S+)\"', listebrute)
        fichiers = list(filter(lambda s: "ttl.gz" in s, liste_liens))

        from fonctions import write_info_file, check_rdf_format_string, get_dag_id
        info = {"download_url": base_url,
                "version": version,
                "file_list": fichiers,
                "dir_name": get_dag_id(),
                "compression": "gz",
                "format": check_rdf_format_string("TURTLE"),
                }
        write_info_file(info)

    from airflow.operators.bash import BashOperator
    get_chembl_version = BashOperator(
        task_id="get_chembl_version",
        bash_command="curl -s https://ftp.ebi.ac.uk/pub/databases/chembl/ChEMBL-RDF/latest/void.ttl.gz | gunzip | grep hasCurrentVersion",
        do_xcom_push=True,
    )

    get_chembl_version >> get_chembl_info() >> check_and_ingest()


chembl_taskflow()
