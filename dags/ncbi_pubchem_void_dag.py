from airflow.models.dag import dag
from ingestion_tasks import *
from airflow.decorators import task, task_group
from airflow.operators.python import PythonOperator
from airflow.providers.apache.hdfs.sensors.web_hdfs import WebHdfsSensor




# ATTENTION, EN TETE DE FICHIER, IMPORTS SPECIFIQUES AIRFLOW UNIQUEMENT


@dag(default_args=default_args,
     dag_id="pubchem_void",
     tags=["ingestion", "ncbi_pubchem_void"],
     catchup=False,
     schedule="@weekly",
     params={"local_temp_repository": "/opt/airflow/ingestion",
             "hdfs_data_dir": "/data",
             "hdfs_tmp_dir": "/tmp",
             "hdfs_connection_id": "hdfs_connection",
             "n_kept_versions": 2},
     max_active_runs=1
     )
def pubchem_void_taskflow():
    """
    Download pubchem void file from https://ftp.ncbi.nlm.nih.gov/pubchem/RDF/void.ttl
    """

    @task(task_id="get_pubchem_void_info")
    def get_pubchem_void_info(**kwargs):
        ti = kwargs['task_instance']
        version = ti.xcom_pull(key='return_value', task_ids='get_pubchem_version')
        base_url = "https://ftp.ncbi.nlm.nih.gov/pubchem/RDF/"
        liste = ["void.ttl", ]

        from fonctions import write_info_file, check_rdf_format_string, get_dag_id
        info = {"download_url": base_url,
                "version": version,
                "file_list": liste,
                "dir_name": get_dag_id(),
                "compression": "",
                "format": check_rdf_format_string("TURTLE")
                }
        write_info_file(info)

    from airflow.operators.bash import BashOperator
    get_pubchem_version = BashOperator(
        task_id="get_pubchem_version",
        bash_command="curl -s https://ftp.ncbi.nlm.nih.gov/pubchem/RDF/ | grep void.ttl | awk '{ print $3 }'",
        do_xcom_push=True,
    )



    from airflow.operators.trigger_dagrun import TriggerDagRunOperator
    trigger_brachemdb = TriggerDagRunOperator(
        task_id='trigger_brachemdb',
        trigger_dag_id='brachemdb',
        wait_for_completion=False,  # Optional: Wait for the triggered DAG to complete
    )

    @task.branch(task_id="check_for_new_version")
    def specific_check_version():
        """
        Comparaison de la version locale avec celle qui est disponible
        Si nouvelle version, lance l'ingestion, sinon lance le log de la verification
        :returns tâche suivante
        """
        from fonctions import get_local_current_version, load_info_file, get_dag_param
        import os.path
        info = load_info_file()
        if get_local_current_version() != info["version"] or os.path.isfile(
                get_dag_param("local_temp_repository") + "/FORCE_UPDATES"):
            ret = "ingest.get_file_list"
        else:
            ret = "log_check"

        return ret

    def generate_file_path(**kwargs):
        from fonctions import load_info_file, get_dag_param
        info = load_info_file()
        version = info.get("version")
        if version is None:
            raise Exception("No version found from current dag communication JSON file")

        substance_path = get_dag_param("hdfs_data_dir").rstrip('/') + '/pubchem_substance_metadata_v' + version
        reference_path = get_dag_param("hdfs_data_dir").rstrip('/') + '/pubchem_reference_metadata_v' + version
        kwargs['ti'].xcom_push(key='substance_path', value=substance_path)
        kwargs['ti'].xcom_push(key='reference_path', value=reference_path)

    generate_file_path_task = PythonOperator(
        task_id='generate_file_path',
        python_callable=generate_file_path,
        provide_context=True
    )

    substance_sensor_task = WebHdfsSensor(
        task_id='wait_for_pc_substance',
        filepath="{{ ti.xcom_pull(task_ids='generate_file_path', key='substance_path') }}",
        webhdfs_conn_id='hdfs_connection',
        timeout=60000,
        poke_interval=30,
        mode='poke',
    )

    reference_sensor_task = WebHdfsSensor(
        task_id='wait_for_pc_reference',
        filepath="{{ ti.xcom_pull(task_ids='generate_file_path', key='reference_path') }}",
        webhdfs_conn_id='hdfs_connection',
        timeout=60000,
        poke_interval=30,
        mode='poke',
    )

    branch = get_pubchem_version >> get_pubchem_void_info() >> specific_check_version()
    branch >> ingest() >> generate_file_path_task >> reference_sensor_task >> substance_sensor_task >> trigger_brachemdb
    branch >> log_check()


pubchem_void_taskflow()
