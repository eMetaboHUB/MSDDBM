from airflow.models.dag import dag
from ingestion_tasks import *
from airflow.providers.docker.operators.docker import DockerOperator


# ATTENTION, EN TETE DE FICHIER, IMPORTS SPECIFIQUES AIRFLOW UNIQUEMENT

@dag(default_args=default_args,
     dag_id="knapsack_scrap",
     tags=["ingestion", "knapsack scrap"],
     params={"local_temp_repository": "/opt/airflow/ingestion",
             "hdfs_data_dir": "/data/",
             "n_kept_versions": 1,
             "HDFS_TMP_DIR": "/data/tmp/",
             "hdfs_connection_id": "hdfs_connection",
             },
     catchup=False,
     schedule="@weekly",
     max_active_runs=1
     )
def knapsack_scrap_taskflow():
    @task(task_id="get_knapsack_scrap_db_info")
    def get_knapsack_scrap_db_info():
        from fonctions import write_info_file, get_dag_param, get_client, check_rdf_format_string, get_dag_id

        version_file_name = get_dag_param("HDFS_TMP_DIR") + "knapsack.version"
        with get_client().read(version_file_name) as reader:
            version = reader.read().decode("utf-8")
        get_client().delete(version_file_name)

        liste = ["knapsack.ttl"]
        info = {"download_url": "http://www.knapsackfamily.com/knapsack_core/",  # pour les metadonnées seulement
                "version": version,
                "file_list": liste,
                "dir_name": get_dag_id(),
                "compression": "",
                "hdfs_preloaded": "True",
                "hdfs_preloaded_location": get_dag_param("HDFS_TMP_DIR"),
                "hdfs_preloaded_remove_after_decomp": "True",
                "format": check_rdf_format_string("TURTLE")
                }
        write_info_file(info)

    @task(task_id="get_knapsack_html_file")
    def get_knapsack_html_file():
        import subprocess
        import os
        from fonctions import hdfs_upload, get_dag_param
        curl_command = ("#!/bin/bash\n" +
                        "curl $'http://www.knapsackfamily.com/knapsack_core/result.php?sname=organism&word=\\u0021-BS" \
                        "-\\u0021*'   -H 'Connection: keep-alive'   -H 'Upgrade-Insecure-Requests: 1'   -H 'User-Agent: " \
                        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 " \
                        "Safari/537.36'   -H 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif," \
                        "image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'   -H 'Referer: " \
                        "http://www.knapsackfamily.com/knapsack_core/info.php'   -H 'Accept-Language: fr-FR,fr;q=0.9," \
                        "en-US;q=0.8,en;q=0.7' --compressed --insecure > /tmp/knapsack.html")
        # l'execution directe de la commande par subprocess ne fonctionne pas
        with open("/tmp/my_curly_command.sh", "w") as file1:
            file1.write(curl_command)
        os.chmod("/tmp/my_curly_command.sh", 0o700)
        code = subprocess.run("/tmp/my_curly_command.sh").returncode
        if code != 0:
            raise Exception("Erreur à l'execution de curl pour le téléchargement. Code: " + str(code))

        hdfs_upload("/tmp/knapsack.html", get_dag_param("HDFS_TMP_DIR") + "knapsack.html")

    html2jon_task = DockerOperator(
        task_id='docker_pup',
        image='docker_pup_task',
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
        xcom_all=False,
        environment={
            "HADOOP_USERNAME": "{{ conn.hdfs_connection.login }}",
            "HDFS_INPUT_PATH": "{{ params.HDFS_TMP_DIR }}" + "knapsack.html",
            "HDFS_OUTPUT_PATH": "{{ params.HDFS_TMP_DIR }}" + "knapsack.json",
            "HADOOP_HOST": "http://"+"{{ conn.hdfs_connection.host }}" + ":""{{ conn.hdfs_connection.port }}"
        },
        extra_hosts={"host.docker.internal": "host-gateway"}
    )

    json2ttl = DockerOperator(
        task_id='docker_turtle_gen',
        image='metabohub/knapsack:1.0',
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
        xcom_all=False,

        environment={
            "HADOOP_USER_NAME": "{{ conn.hdfs_connection.login }}",
            "JSON_INPUT_PATH": "{{ params.HDFS_TMP_DIR }}" + "knapsack.json",
            "HDFS_TMP_DIR": "{{ params.HDFS_TMP_DIR }}",
            "TTL_OUTPUT_FILE_NAME": "knapsack.ttl",
            "VERSION_OUTPUT_FILE_NAME": "knapsack.version",
            "HADOOP_URL": "http://"+"{{ conn.hdfs_connection.host }}" + ":""{{ conn.hdfs_connection.port }}",
            "HADOOP_PWD": "{{ conn.hdfs_connection.password }}"
        },
        extra_hosts={"host.docker.internal": "host-gateway"}

    )


    get_knapsack_html_file() >> html2jon_task >> json2ttl >> get_knapsack_scrap_db_info() >> check_and_ingest()


knapsack_scrap_taskflow()
