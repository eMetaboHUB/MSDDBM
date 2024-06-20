from datetime import datetime
from airflow.decorators import task, task_group
from airflow.contrib.operators.ssh_operator import SSHOperator
from datetime import timedelta
import pendulum

# ATTENTION, EN TETE DE FICHIER, IMPORTS SPECIFIQUES AIRFLOW UNIQUEMENT

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['p2m2-it@inrae.fr'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 20,
    'retry_delay': timedelta(minutes=5),
    'start_date': pendulum.datetime(2023, 11, 1, tz="UTC"),
}


@task(task_id="downld_and_decomp")
def single_download_and_decompress(file):
    """
    Tâche de téléchargement, en http(s). Télécharge localement et décompresse si besoin
    le fichier donné en argument puis envoie vers HDFS
    """
    import os
    import shutil
    from fonctions import hdfs_upload, get_dag_param, http_download, load_info_file, \
        std_db_directory_name, get_db_update_id
    info = load_info_file()
    hdfs_dir_path = std_db_directory_name(get_dag_param("hdfs_data_dir"))

    # création dossier temp sur le worker
    temp_worker_dir = (std_db_directory_name(get_dag_param("local_temp_repository"))
                       + get_db_update_id()
                       + file
                       + ".tmp/")
    os.makedirs(temp_worker_dir, exist_ok=True)

    # création dossier destination sur le cluster
    from fonctions import get_client
    status = get_client().status(hdfs_dir_path, strict=False)
    if status is None:
        get_client().makedirs(hdfs_dir_path, permission=777)

    # choix de la fonction de décompression
    def no_comp(fichier):
        l = list()  # todo en 1 ligne ?
        l.append(fichier)
        return l

    decomp_f = no_comp

    if info["compression"] == "gz":
        from fonctions import local_gunzip
        decomp_f = local_gunzip
    else:
        if info["compression"] == "zip":
            from fonctions import local_unzip
            decomp_f = local_unzip

    url = info["download_url"] + file
    local_filename = temp_worker_dir + file

    if info.get("hdfs_preloaded", "False") == "True":
        get_client().download(info["hdfs_preloaded_location"] + file, local_filename, overwrite=True)
    else:
        http_download(url, local_filename, local=True)

    liste = decomp_f(local_filename)
    for fichier_decomp in liste:
        hdfs_upload(fichier_decomp, hdfs_dir_path)
        os.remove(fichier_decomp)

    if info.get("hdfs_preloaded_remove_after_decomp", False) == "True":
        get_client().delete(info["hdfs_preloaded_location"] + file)

    try:
        shutil.rmtree(temp_worker_dir, ignore_errors=True)
        # os.removedirs(temp_worker_dir)
    except:
        print("Error removing temporary directory" + temp_worker_dir)


@task(task_id="gen_metadata")
def gen_metadata():
    """
    Génération des métadonnées du téléchargement, stockées dans le même répertoire que les données,
    au format JSON pour l'instant.
    La tâche produit 2 fichiers identiques : un dont le nom contient la version, l'autre avec la version "latest".
    Todo: stocker une version en turtle selon ontologie de métadonnées ?
    """
    from fonctions import load_info_file, get_client, metadata_filename, get_dag_param, \
        std_db_directory_name

    info = load_info_file()

    hdfs_dir_path = std_db_directory_name(get_dag_param("hdfs_data_dir"))
    actual_file_list = get_client().list(hdfs_dir_path, status=False)

    added_data: dict = {"date_importation_msd": datetime.now().isoformat(),
                        "actual_file_list": actual_file_list}
    info.update(added_data)
    # fichier pour infos sur la dernière version:  son nom ne contient pas la version mais "_latest"
    # (écrase le précédent)
    import json
    with get_client().write(metadata_filename(get_dag_param("hdfs_data_dir")), encoding='utf-8',
                            overwrite=True) as fp:
        json.dump(info, fp)

    # idem mais avec numero de version, pour le suivi des bases presentes
    with get_client().write(metadata_filename(get_dag_param("hdfs_data_dir"), with_version=True), encoding='utf-8',
                            overwrite=True) as fpv:
        json.dump(info, fpv)


@task(task_id="db_clean")
def db_clean():
    """
    Suppression des anciennes bases. Cette tâche ne laisse que les n_kept_versions dernières versions sur le serveur.
    2 versions devraient suffire pour éviter de planter un calcul en cours ?
    """
    from fonctions import (get_client, get_dag_param, metadata_filename_base, load_json_hdfs,
                           load_info_file,
                           metadata_filename, std_db_directory_name)
    info = load_info_file()
    nkv = get_dag_param("n_kept_versions")
    if nkv != "":
        n_kept_versions = nkv
    else:
        n_kept_versions = 2

    client = get_client()
    # récupération de la liste des fichiers de metadonnées de la base présents sur HDFS dans "paths"
    present_files_names = client.list(get_dag_param("hdfs_data_dir"), status=False)
    metadata_file_basename = metadata_filename_base(info)
    metafiles = list(filter(lambda s: (metadata_file_basename in s) and ('_latest' not in s), present_files_names))
    paths = list(map(lambda s: get_dag_param("hdfs_data_dir") + "/" + s, metafiles))

    # Tri par ancienneté décroissante des versions
    versions_vs_dates = sorted(
        list(map(
            lambda p: (
                datetime.fromisoformat(load_json_hdfs(p)["date_importation_msd"]), (load_json_hdfs(p)["version"]))
            , paths)), reverse=True)

    # Elagage de la liste pour ne récupérer que les versions à effacer
    if len(versions_vs_dates) > n_kept_versions:
        del versions_vs_dates[0:n_kept_versions:1]
    else:
        del versions_vs_dates[::]

    # Création des listes des noms de fichiers à effacer

    metafiles_to_delete = list(
        map(
            lambda t: metadata_filename(
                path=get_dag_param("hdfs_data_dir"),
                with_version=True,
                force_version=True,
                forced_version_value=t[1])
            , versions_vs_dates
        )
    )

    db_dirs_to_delete = list(
        map(
            lambda t: std_db_directory_name(
                path=get_dag_param("hdfs_data_dir"),
                force_version=True,
                forced_version_value=t[1])
            , versions_vs_dates
        )
    )

    for file in metafiles_to_delete:
        get_client().delete(file, recursive=False, skip_trash=True)

    for rep in db_dirs_to_delete:
        get_client().delete(rep, recursive=True, skip_trash=True)

    # Effacage des fichiers temporaires
    worker_dir = std_db_directory_name(get_dag_param("local_temp_repository"))
    try:
        import os
        os.removedirs(worker_dir)
    except:
        print(worker_dir + "ne devait pas déjà exister")


@task(task_id="log_check")
def log_check():
    """
    Tâche à lancer quand il n'y a pas de mise à jour disponible.
    On ajoute simplement une ligne de log.
    Todo: envoyer un mail ?
    """
    from datetime import datetime
    from fonctions import log
    from airflow.operators.python import get_current_context
    context = get_current_context()
    dag = context["dag"]
    message = dag.dag_id + ": Pas de mise à jour disponible lors de la vérification du " + datetime.now().isoformat() + "\n"
    log(message)


@task.branch(task_id="check_for_new_version")
def check_for_new_version():
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
        ret = "check_and_ingest.ingest.get_file_list"
    else:
        ret = "check_and_ingest.log_check"

    return ret


@task(task_id="get_file_list")
def get_file_list():
    from fonctions import load_info_file
    return list(load_info_file().get("file_list", ""))


spark_void = SSHOperator(
    ssh_conn_id='msd_ssh_connection',
    task_id='spark_void',
    command="bash -l msdSparkJob void -n {{dag.dag_id}}",
    cmd_timeout=None,)

spark_enhance_void = SSHOperator(
    ssh_conn_id='msd_ssh_connection',
    task_id='spark_enhance_void',
    command="bash -l msdSparkJob enhanceVoid -n {{dag.dag_id}}",
    cmd_timeout=None,)

spark_parquetize = SSHOperator(
    ssh_conn_id='msd_ssh_connection',
    task_id='spark_parquetize',
    command="bash -l msdSparkJob toParquet -n {{dag.dag_id}} --maxTriplesPerFile 3000000",
    cmd_timeout=None,)


@task_group(group_id="ingest")
def ingest():
    """
    Task group to run if new version available
    """

    (single_download_and_decompress.expand(file=get_file_list())
     >> gen_metadata()
     >> db_clean()
     >> spark_parquetize
     >> spark_void
     >> spark_enhance_void
     )


@task_group(group_id="check_and_ingest")
def check_and_ingest():
    """
    Groupe de tâches à lancer pour vérifier la présence d'une nouvelle version, le cas échéant lancer l'ingestion.
    """
    check_for_new_version() >> [ingest(), log_check()]
