from airflow.models.dag import dag
from ingestion_tasks import *


# ATTENTION, EN TETE DE FICHIER, IMPORTS SPECIFIQUES AIRFLOW UNIQUEMENT


@dag(default_args=default_args,
     dag_id="dublin_core_dcmi_terms",
     tags=["ingestion", "dublin_core_dcmi_terms"],
     catchup=False,
     schedule="@weekly",
     params={"local_temp_repository": "/opt/airflow/ingestion",
             "hdfs_data_dir": "/data",
             "hdfs_tmp_dir": "/tmp",
             "hdfs_connection_id": "hdfs_connection",
             "n_kept_versions": 2},
     max_active_runs=1
     )
def dublin_core_dcmi_terms_taskflow():
    """
    Download dublin_core_dcmi_terms
    """

    @task(task_id="get_dublin_core_dcmi_terms_info")
    def get_dublin_core_dcmi_terms_info(**kwargs):
        ti = kwargs['task_instance']
        version = ti.xcom_pull(key='return_value', task_ids='get_dc_version')
        base_url = "https://www.dublincore.org/specifications/dublin-core/dcmi-terms/"
        liste = ["dublin_core_terms.nt", ]
        from fonctions import write_info_file, check_rdf_format_string, get_dag_id
        info = {"download_url": base_url,
                "version": version,
                "file_list": liste,
                "dir_name": get_dag_id(),
                "compression": "",
                "format": check_rdf_format_string("NTRIPLES"),
                "ontology_namespace": "http://purl.org/dc/terms/"
                }
        write_info_file(info)

    from airflow.operators.bash import BashOperator
    get_dublin_core_dcmi_terms_version = BashOperator(
        task_id="get_dc_version",
        bash_command="curl https://www.dublincore.org/specifications/dublin-core/dcmi-terms/ | grep \"http://dublincore.org/specifications/dublin-core/dcmi-term\" | awk 'match($0,\"[0-9]+-[0-9]+-[0-9]+\") { print substr($0,RSTART,RLENGTH) }'",
        do_xcom_push=True,
    )

    get_dublin_core_dcmi_terms_version >> get_dublin_core_dcmi_terms_info() >> check_and_ingest()


dublin_core_dcmi_terms_taskflow()
