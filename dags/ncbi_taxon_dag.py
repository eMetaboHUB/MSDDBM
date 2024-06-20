from airflow.models.dag import dag
from ingestion_tasks import *


# ATTENTION, EN TETE DE FICHIER, IMPORTS SPECIFIQUES AIRFLOW UNIQUEMENT


@dag(default_args=default_args,
     dag_id="ncbitaxon",
     tags=["ingestion", "ncbi", "taxon"],
     params={"local_temp_repository": "/opt/airflow/ingestion",
             "hdfs_data_dir": "/data",
             "hdfs_tmp_dir": "/tmp",
             "hdfs_connection_id": "hdfs_connection",
             "n_kept_versions": 1},
     catchup=False,
     max_active_runs=1,
     schedule="@weekly",
     )
def ncbi_taskflow():
    @task(task_id="get_ncbitaxon_db_info")
    def get_ncbitaxon_db_info():
        from fonctions import write_info_file, http_download, check_rdf_format_string, get_dag_id
        # Récupérartion de l'url du fichier et la version courante dans un arbre json
        ncbi_json = http_download("https://api.github.com/repos/obophenotype/ncbitaxon/releases/latest")
        import json
        parsed_json = json.loads(ncbi_json)
        assets = parsed_json['assets']
        asset = list(filter(lambda e: e["name"] == "ncbitaxon.owl.gz", assets))[0]

        version = asset["updated_at"][0:10]

        import os
        from urllib.parse import urlparse
        parsed_url = urlparse(asset["browser_download_url"])
        liste = [os.path.basename(parsed_url.path), ]
        base_url = parsed_url.scheme + "://" + parsed_url.netloc + "/" + os.path.dirname(parsed_url.path) + "/"

        info = {"download_url": base_url,
                "file_list": liste,
                "version": version,
                "dir_name": get_dag_id(),
                "compression": "gz",
                "format": check_rdf_format_string("RDFXML"),
                "ontology_namespace": "http://purl.obolibrary.org/obo/ncbitaxon#"
                }

        write_info_file(info)

    get_ncbitaxon_db_info() >> check_and_ingest()


ncbi_taskflow()
