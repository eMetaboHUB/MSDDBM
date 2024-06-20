from airflow.models.dag import dag
from ingestion_tasks import *
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator


# ATTENTION, EN TETE DE FICHIER, IMPORTS SPECIFIQUES AIRFLOW UNIQUEMENT

@dag(default_args=default_args,
     dag_id="human_gem",
     tags=["ingestion", "human_gem"],
     params={"local_temp_repository": "/opt/airflow/ingestion",
             "hdfs_data_dir": "/data/",
             "n_kept_versions": 2,
             "HDFS_TMP_DIR": "/data/tmp/",
             "hdfs_connection_id": "hdfs_connection",
             },
     catchup=False,
     schedule="@weekly",
     max_active_runs=1
     )
def human_gem_taskflow():
    def get_human_gem_db_info():
        from fonctions import write_info_file, get_dag_param, get_client, check_rdf_format_string, get_dag_id

        import requests

        def get_latest_release(repo):
            url = f"https://api.github.com/repos/{repo}/releases/latest"
            response = requests.get(url)
            data = response.json()
            return data.get('tag_name', 'No release found')

        repo = "sysbiochalmers/Human-GEM"
        version = get_latest_release(repo)

        liste = ["human_gem.ttl"]
        info = {"download_url": "https://github.com/SysBioChalmers/Human-GEM/blob/main/model/Human-GEM.xml",
                # pour les metadonnées seulement
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

    get_human_gem_db_info = PythonOperator(
        task_id="get_human_gem_db_info",
        python_callable=get_human_gem_db_info,
    )

    def get_human_gem_xml_file():
        import requests

        from fonctions import hdfs_upload, get_dag_param, http_download

        # Seems that github won't send raw content to our usual download function, so we add this one...
        def download_github_file(gh_url, output_gh_filename):
            response = requests.get(gh_url)
            if response.status_code == 200:
                with open(output_gh_filename, 'wb') as file:
                    file.write(response.content)
                print(f"File downloaded successfully: {output_gh_filename}")
            else:
                print(f"Failed to download file: HTTP {response.status_code}")
                exit(1)

        # URL of the GitHub file
        url = "https://raw.githubusercontent.com/SysBioChalmers/Human-GEM/main/model/Human-GEM.xml"
        output_filename = "/tmp/Human-GEM.xml"

        # Download the file
        download_github_file(url, output_filename)

        # http_download("https://raw.githubusercontent.com/SysBioChalmers/Human-GEM/main/model/Human-GEM.xml",
        #              "/tmp/Human-GEM.xml", local=True)

        hdfs_upload("/tmp/Human-GEM.xml", get_dag_param("HDFS_TMP_DIR") + "Human-GEM.xml")

    get_human_gem_xml_file_task = PythonOperator(
        task_id='get_human_gem_xml_file',
        python_callable=get_human_gem_xml_file
    )

    xml2ttl_task = DockerOperator(
        task_id='xml2ttl',
        image='docker_sbml2rdf_task',
        auto_remove="success",
        docker_url="unix://var/run/docker.sock",
        mount_tmp_dir=False,
        xcom_all=False,
        environment={
            "HADOOP_USERNAME": "{{ conn.hdfs_connection.login }}",
            "HDFS_INPUT_PATH": "{{ params.HDFS_TMP_DIR }}" + "Human-GEM.xml",
            "HDFS_OUTPUT_PATH": "{{ params.HDFS_TMP_DIR }}" + "human_gem.ttl",
            "HADOOP_HOST": "http://" + "{{ conn.hdfs_connection.host }}" + ":""{{ conn.hdfs_connection.port }}"
        },
        extra_hosts={"host.docker.internal": "host-gateway"}
    )

    # get_human_gem_xml_file() >> xml2ttl_task >> get_human_gem_db_info() >> check_and_ingest()
    res1 = get_human_gem_xml_file_task >> xml2ttl_task >> get_human_gem_db_info
    res1 >> check_and_ingest()


human_gem_taskflow()
