import time



def std_db_directory_name(path: str, force_version: bool = False, forced_version_value="0") -> str:
    """
    :param path: emplacement du repertoire de base
    :param force_version: si besoin du nom de fichier pour une autre version que celle du DAG en cours
    :param forced_version_value: valeur de la version éventuellement spécifiée
    :return:  Le nom standardisé de toutes les bases
    """
    info = load_info_file()
    std_name = (path.rstrip('/')
                + "/"
                + info.get("dir_name", "noBaseDirSpecified")
                + get_version_string(force_version, forced_version_value)
                + "/")

    return std_name


def metadata_filename(path: str, with_version: bool = False, force_version: bool = False,
                      forced_version_value: str = "noForcedVersionSpecified") -> str:
    """
    :param path: chemin à ajouter au nom du fichier
    :param with_version: inclure la version de la base ingérée, sinon ajouter  a "_latest" au nom de fichier
    :param force_version: si besoin du nom de fichier pour une autre version que celle du DAG en cours
    :param forced_version_value: valeur de la version éventuellement spécifiée
    :return: le nom standardisé du fichier de métadonnées
    """
    info = load_info_file()
    meta_name = path.rstrip('/') + "/" + metadata_filename_base(info)

    if with_version:
        meta_name += get_version_string(force_version, forced_version_value)
    else:
        meta_name += "_latest"

    return meta_name


def metadata_filename_base(info) -> str:
    """
    :param info: le dictionnaire spécifié au tout début du DAG
    :return: la partie commune à tous les fichiers de metadonnées du DAG
    """
    return info.get("dir_name", "noDirNameSpecified") + '_metadata'


def get_version_string(force_version: bool = False, forced_version_value="noVersionSpecified") -> str:
    """
    :return: end of directory and metadata file version, eg _v2.1
    :param force_version: whether to force version or not
    :param forced_version_value: the value to set instead of retrieving it from the current metadata file
    """
    if force_version:
        if forced_version_value == "":
            forced_version_value = "noVersionSpecified"

        return "_v" + forced_version_value
    else:
        return "_v" + load_info_file().get("version", "noVersionSpecified")


def hdfs_upload(local_path, hdfs_path):
    """
    :param local_path: Chemin sur le worker (attention, en architecture distribuée, on ne sait pas sur quel machine il
    est lancé, le worker)
    :param hdfs_path: Chemin sur le cluster
    :return: Envoi d'un fichier local du worker sur le cluster.
    """
    from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
    webhdfs_hook = WebHDFSHook(webhdfs_conn_id=get_dag_param('hdfs_connection_id'))
    webhdfs_hook.load_file(local_path, hdfs_path, overwrite=True, parallelism=0)


def get_local_current_version() -> str:
    """
    :return: la dernière version de la base présente sur le cluster d'aprés les fichiers de metadonnées. 0 si aucune base trouvée
    """
    import json
    fs_version = "0"
    metadata_fname = metadata_filename(get_dag_param("hdfs_data_dir"))
    client = get_client()
    status = client.status(metadata_fname, strict=False)
    if status:
        with client.read(metadata_fname, encoding='utf-8') as i:
            meta_dict = json.load(i)
            fs_version = meta_dict["version"]
    return fs_version


def get_db_update_id():
    """
    :return: un identifiant spécifique au run du dag en cours.
    """
    from airflow.operators.python import get_current_context
    context = get_current_context()
    dag = context["dag"]
    run_id = context["run_id"]
    return dag.dag_id + "_" + "".join([x if x.isalnum() else "_" for x in run_id])


def get_dag_id():
    """
    :return: Dag id, to be used as graph name
    """
    from airflow.operators.python import get_current_context
    context = get_current_context()
    dag = context["dag"]
    return dag.dag_id


def write_info_file(info):
    """
    Ecriture du fichier temporaire sur HDFS pour communication inter-tâches. Remplace les xcoms
    :param info:
    :return: side effect
    """
    import json
    with get_client().write(get_db_update_id(), encoding='utf-8', overwrite=True) as fp:
        json.dump(info, fp)


def load_info_file() -> dict:
    """
    :return: Le dictionnaire équivalent au fichier temporaire sur HDFS pour communication inter-tâches.
    Remplace les xcoms
    """
    import json
    try:
        with get_client().read(get_db_update_id(), encoding='utf-8') as i:
            info = json.load(i)
    except:
        time.sleep(2)
        with get_client().read(get_db_update_id(), encoding='utf-8') as i:
            info = json.load(i)

    return info


def load_json_hdfs(path) -> dict:
    """
    :param path: chemin HDFS
    :return: le contenu du JSON sous forme de dictionnaire
    """
    import json
    with get_client().read(path, encoding='utf-8') as i:
        info = json.load(i)
    return info


def get_dag_param(field: str) -> str:
    """
    :param field: le nom d'un paramètre du DAG
    :return: la valeur du paramètre du DAG
    """
    from airflow.operators.python import get_current_context
    context = get_current_context()

    try:
        p = context["params"]
        ret = p[field]
    except:
        ret = ""
        print("Champ non trouvé")

    return ret


def get_client():
    """
    :return: le client webHDFS pour les opérations sur les fichiers du cluster
    """
    from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
    webhdfs_hook = WebHDFSHook(webhdfs_conn_id=get_dag_param('hdfs_connection_id'))
    return webhdfs_hook.get_conn()


def log(data):
    """
    :param data: Les données à écrire dans le fichier de log
    """
    client = get_client()
    logfile_name = get_dag_param("hdfs_data_dir") + "/semantic_datalake_update.log"

    # création du fichier si inexistant
    status = client.status(logfile_name, strict=False)
    if status is None:
        with client.write(logfile_name) as f:
            f.write("")

    with client.write(logfile_name, append=True) as f:
        f.write(data)


def local_gunzip(filename) -> list:
    """
    Décompression d'un fichier gzip
    Signature bizarre mais pour compatibilité fction unzip
    :param: dummy pour compat unzip
    :param filename: le nom du fichier, genre fichier.gz
    :return: une liste qui contient le chemin complet du fichier decompressé pour compat avec unzip
    """

    def gz_decompress(file_in, file_out):
        import gzip
        import os
        import shutil
        with open(file_out, 'wb') as f_out, gzip.open(file_in, 'rb') as f_in:
            shutil.copyfileobj(f_in, f_out)
        os.remove(file_in)

    if filename[-3:] == ".gz":
        decomp_filename = filename[0:-3]
    else:
        decomp_filename = filename + "_decompressed"  # pour identifier le pbm si jamais pas ext gz...

    gz_decompress(filename, decomp_filename)

    return [decomp_filename, ]


def local_unzip(filename) -> list:
    """
    Décompression d'un fichier zip situé sur le volume docker
    rapatriement sur FS local du node qui décompresse. Pas élégant mais simple...
    Amélioration possible: le faire directement sur HDFS ?
    :param dest_dir: le répertoire sur lequel se trouve le fichier à dégzipper
    :param filename: le nom du fichier, genre fichier.gz
    :return: rien
    """
    import os
    import zipfile

    dest_dir = os.path.dirname(filename) + "/"

    with zipfile.ZipFile(filename, 'r') as zip_ref:
        zip_ref.extractall(dest_dir)
        zipliste = zip_ref.filelist

    os.remove(filename)
    liste = list(map(lambda f: dest_dir + f.filename, zipliste))
    return liste


def http_download(url: str, filepath: str = "", local: bool = False):
    """
    2 possibilités de return value : la réponse (petits volumes) ou rien mais écriture fichier (gros volumes)
    :param local: télécharger sur le volume local plutôt qu'en HDFS
    :param url: URL du fichier à télécharger
    :param filepath: si spécifié, le chemin HDFS sur lequel on enregistre
    :return: si filepath == "", le contenu de la réponse HTTP
    :decompress: télécharge en local et décompresse avant d'envoyer sur HDFS.
    """
    import requests
    print("Téléchargement de " + url)

    if filepath != "":
        if not local: # Download directly to HDFS
            http_resp = requests.get(url, stream=True)
            if http_resp.status_code != 200:
                raise Exception(f"Erreur {http_resp.status_code} au téléchargement de {url}")
            with get_client().write(filepath, overwrite=True) as f:
                for chunk in http_resp.raw.stream(1024, decode_content=False):
                    if chunk:
                        f.write(http_resp.content)
        else:
            try:
                import wget
                wget.download(url, filepath)  # Download on local filesystem
            except :
                print("Error with wget, trying with requests.get ")
                from shutil import copyfileobj
                r = requests.get(url, stream=True)
                if r.status_code == 200:
                    with open(filepath, 'wb') as f:
                        r.raw.decode_content = True
                        copyfileobj(r.raw, f)

            # from urllib.request import urlopen
            # from shutil import copyfileobj
            # from urllib.request import Request
            # r = Request(url,headers={'User-agent': 'Mozilla/5.0'})
            # with urlopen(url) as in_stream, open(filepath, 'wb') as out_file:
            #     copyfileobj(in_stream, out_file)



            #import urllib
            #urllib.request.urlretrieve(url, filepath)

        return ""  # wrote to file, nothing to return
    else:
        return requests.get(url, stream=True).content


def check_rdf_format_string(string_to_check):
    legal_format_strings = ["TURTLE",
                            "NTRIPLES",
                            "RDFXML",
                            "N3",
                            "JSONLD",
                            "RDFJSON",
                            "NQUADS",
                            "TRIG",
                            "TRIGSTAR",
                            "TRIX",
                            "TURTLESTAR",
                            "BINARY",
                            "HDT",
                            "NDJSONLD",
                            "RDFA"
                            ]
    if string_to_check in legal_format_strings:
        return string_to_check
    else:
        raise Exception("La chaîne spécifiée pour le format RDF n'est pas dans la liste.")
