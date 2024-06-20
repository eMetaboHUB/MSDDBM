from airflow.models.dag import dag
from ingestion_tasks import *


# ATTENTION, EN TETE DE FICHIER, IMPORTS SPECIFIQUES AIRFLOW UNIQUEMENT


@dag(default_args=default_args,
     dag_id="chebi",
     tags=["ingestion", "chebi"],
     catchup=False,
     schedule="@weekly",
     params={"local_temp_repository": "/opt/airflow/ingestion",
             "hdfs_data_dir": "/data",
             "hdfs_tmp_dir": "/tmp",
             "hdfs_connection_id": "hdfs_connection",
             "n_kept_versions": 1},
     max_active_runs=1
     )
def chebi_taskflow():
    """
    Download chebi from https://ftp.ebi.ac.uk/pub/databases/chebi/ontology/chebi.owl.gz
    """

    @task(task_id="get_chebi_info")
    def get_chebi_info(**kwargs):
        ti = kwargs['task_instance']
        version_source = ti.xcom_pull(key='return_value', task_ids='get_chebi_version')
        import re
        version = re.findall("[0-9][0-9\\-]+", version_source)[0]
        base_url = "https://ftp.ebi.ac.uk/pub/databases/chebi/ontology/"
        liste = ["chebi.owl.gz"]
        from fonctions import write_info_file, check_rdf_format_string, get_dag_id
        info = {"download_url": base_url,
                "version": version,
                "file_list": liste,
                "dir_name": get_dag_id(),
                "compression": "gz",
                "format": check_rdf_format_string("RDFXML"),
                "ontology_namespace": "http://purl.obolibrary.org/obo/chebi/"
                }
        write_info_file(info)

    from airflow.operators.bash import BashOperator
    get_chebi_version = BashOperator(
        task_id="get_chebi_version",
        bash_command="curl -s https://ftp.ebi.ac.uk/pub/databases/chebi/ontology/ | grep chebi.owl.gz",
        do_xcom_push=True,
    )

    get_chebi_version >> get_chebi_info() >> check_and_ingest()


chebi_taskflow()
