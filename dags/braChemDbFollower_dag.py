from airflow.models.dag import dag
from ingestion_tasks import *
from airflow.contrib.operators.ssh_operator import SSHOperator



@dag(default_args=default_args,
     dag_id="brachemdb",
     tags=["monitoring", "literature", "Brassicaceae"],
     params={"local_temp_repository": "/opt/airflow/ingestion",
             "hdfs_data_dir": "/data/",
             "HDFS_TMP_DIR": "/data/tmp/",
             "hdfs_connection_id": "hdfs_connection",
             "dest_email": "p2m2-it@inrae.fr"
             },
     catchup=False,
     schedule=None,
     max_active_runs=1
     )
def brachemdb_taskflow():
    @task(task_id="pre_clean")
    def pre_clean():
        from fonctions import get_client, get_dag_param
        client = get_client()
        diff_file_name = get_dag_param("HDFS_TMP_DIR") + "/brachemdb/exploration.diff"
        status = client.status(diff_file_name, strict=False)
        if status is not None:
            client.delete(diff_file_name)
            print("Deleted")

    run_graph_exploration = SSHOperator(
        ssh_conn_id='msd_ssh_connection',
        task_id='run_graph_exploration',
        command="bash -l /usr/local/msd-database-management/apps/bin/braChemDbSubmit.sh run",
        cmd_timeout=None,
    )

    compare_results = SSHOperator(
        ssh_conn_id='msd_ssh_connection',
        task_id='compare_results',
        command="bash -l /usr/local/msd-database-management/apps/bin/braChemDbSubmit.sh makeDiff",
        cmd_timeout=None,
    )

    @task.branch(task_id="branch_on_new_results")
    def branch_on_new_results():
        from fonctions import get_client, get_dag_param
        client = get_client()
        diff_file_name = get_dag_param("hdfs_data_dir") + "/braChemDb/brachemdbResults"

        status = client.status(diff_file_name, strict=False)
        if status is None:
            ret = "do_nothing"
        else:
            # read diff
            with get_client().read(diff_file_name, encoding='utf-8') as file:
                file_content = file.read()
                if file_content.startswith("# NO newer results"):
                    ret = "do_nothing"
                else:
                    ret = "send_email_task"
        return ret

    @task(task_id="do_nothing")
    def do_nothing():
        print("nothing")

    @task(task_id="send_email_task")
    def send_email_task():
        import datetime
        from fonctions import get_client, get_dag_param

        def read_and_write_file(client, hdfs_file_name, local_file_name):
            with client.read(hdfs_file_name, encoding='utf-8') as hdfs_file:
                file_content = hdfs_file.read()
            with open(local_file_name, 'w', encoding='utf-8') as local_file:
                local_file.write(file_content)

        # Parameters
        worker_diff_file_name = "/tmp/brachemdbResults"
        worker_complete_file_name = worker_diff_file_name + "-complete"

        hdfs_data_dir = get_dag_param("hdfs_data_dir")
        diff_file_name = f"{hdfs_data_dir}/braChemDb/brachemdbResults"
        complete_results_filename = f"{diff_file_name}-complete"

        # HDFS client
        hdfs_client = get_client()

        # Read and write the files
        read_and_write_file(hdfs_client, diff_file_name, worker_diff_file_name)
        read_and_write_file(hdfs_client, complete_results_filename, worker_complete_file_name)

        html_content = f"""
        <h3>BraChemDb Alert</h3>
        <p>This is an email sent from Airflow on {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}.</p>
        <p>Please find attached files :</p>
        <p>brachemdbResults holding differences with precedent results,</p>
        <p>brachemdbResults-complete containing the full search results.</p>
        """

        from airflow.utils.email import send_email
        from fonctions import get_dag_param
        email_param = get_dag_param("dest_email")
        if email_param != "":
            dest = email_param
        else:
            dest = "p2m2-it@inrae.fr"

        send_email(to=dest,
                   subject='New BraChemDb results found',
                   html_content=html_content,
                   files=[worker_diff_file_name, worker_complete_file_name])

    run_graph_exploration >> compare_results >> branch_on_new_results() >> [send_email_task(), do_nothing()]


brachemdb_taskflow()
