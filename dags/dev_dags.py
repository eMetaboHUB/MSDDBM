import pendulum
from airflow.models.dag import dag
from ingestion_tasks import *

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2023, 10, 1, tz="UTC"),
    'email': ['guillaume.laisney@inrae.fr'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}


@dag(default_args=default_args,
     dag_id="z_devel_dag",
     start_date=pendulum.datetime(2023, 10, 1, tz="UTC"),
     catchup=False,
     schedule="@daily",
     tags=["developpement"],
     params={"local_temp_repository": "/opt/airflow/ingestion",
             "hdfs_data_dir": "/data",
             "hdfs_tmp_dir": "/tmp",
             "hdfs_connection_id": "hdfs_connection",
             "n_kept_versions": 2},
     max_active_runs=1
     )
def test_taskflow():
    @task(task_id="get_test_db_info2")
    def get_test_db_info2():
        from fonctions import write_info_file, get_dag_id
        liste = ["pc_inchikey_type_000009.ttl.gz",
                 # "pc_inchikey_value_000012.ttl.gz",
                 ]
        info = {"download_url": "https://ftp.ncbi.nlm.nih.gov/pubchem/RDF/inchikey/",
                "version": "12",
                "file_list": liste,
                "dir_name": get_dag_id(),
                "compression": "gz",
                "format": "TURTLE"
                }
        write_info_file(info)

    @task(task_id="failing_task")
    def failure():
        raise Exception("Test exception")

    # Important : In order to run localMsdSparkJob,
    # those variables have to be set in .profile (not .bashrc) :
    # export PATH=$PATH:/usr/local/msd-database-management/bin
    # export SPARK_HOME=...
    # export PATH=$PATH:$SPARK_HOME/bin
    # export HADOOP_CONF_DIR=...

    get_test_db_info2() >> check_and_ingest()

    # get_test_db_info2() >> ssh_task

    # get_test_db_info2() >> failure() >> check_and_ingest()

    # get_test_db_info2() >> single_download_and_decompress.expand(file=get_file_list()) >> gen_metadata() >> db_clean()


test_taskflow()
