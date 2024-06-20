from airflow.models.dag import dag
from ingestion_tasks import *

# ATTENTION, EN TETE DE FICHIER, IMPORTS SPECIFIQUES AIRFLOW UNIQUEMENT

@dag(default_args=default_args,
     dag_id="pubchem_compound_general",
     tags=["ingestion", "ncbi_pubchem_compound_general"],
     params={"local_temp_repository": "/opt/airflow/ingestion",
             "hdfs_data_dir": "/data",
             "hdfs_tmp_dir": "/tmp",
             "hdfs_connection_id": "hdfs_connection",
             "n_kept_versions": 1},
     catchup=False,
     schedule="@weekly",
     max_active_runs=1
     )
def compound_taskflow():
    """
    Download turtle file PubChem Compound General https://ftp.ncbi.nlm.nih.gov/pubchem/RDF/compound/general/
    """

    @task(task_id="get_compound_info")
    def get_compound_info(**kwargs):
        from fonctions import http_download
        import re
        ti = kwargs['task_instance']
        version = ti.xcom_pull(key='return_value', task_ids='get_pubchem_version')

        listebrute = http_download("https://ftp.ncbi.nlm.nih.gov/pubchem/RDF/compound/general/").decode('UTF-8')
        # "(a href=\"\S+\")"gm
        liste_liens = re.findall(r'a href=\"(\S+)\"', listebrute)
        fichiers = list(filter(lambda s: "ttl.gz" in s, liste_liens))

        from fonctions import write_info_file, check_rdf_format_string, get_dag_id
        info = {"download_url": "https://ftp.ncbi.nlm.nih.gov/pubchem/RDF/compound/general/",
                "version": version,
                "file_list": fichiers,
                "dir_name": get_dag_id(),
                "compression": "gz",
                "format": check_rdf_format_string("TURTLE")
                }
        write_info_file(info)

    from airflow.operators.bash import BashOperator

    get_pubchem_version = BashOperator(
        task_id="get_pubchem_version",
        bash_command="curl -s https://ftp.ncbi.nlm.nih.gov/pubchem/RDF/ | grep void.ttl | awk '{ print $3 }'",
        do_xcom_push=True,
    )

    get_pubchem_version >> get_compound_info() >> check_and_ingest()


compound_taskflow()

