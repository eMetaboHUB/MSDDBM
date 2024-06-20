import pendulum
from airflow.models.dag import dag
from ingestion_tasks import *

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2023, 10, 1, tz="UTC"),
    'email': ['guillaume.laisney@inrae.fr'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


@dag(default_args=default_args,
     dag_id="unit_test_dag",
     start_date=pendulum.datetime(2023, 10, 1, tz="UTC"),
     catchup=False,
     schedule="@daily",
     tags=["unit tests"],
     params={"local_temp_repository": "/opt/airflow/ingestion",
             "hdfs_data_dir": "/data",
             "hdfs_tmp_dir": "/tmp",
             "hdfs_connection_id": "hdfs_connection",
             "n_kept_versions": 2},  # change accordingly with test9 task
     max_active_runs=1
     )
def test_taskflow():
    @task(task_id="get_test_db_info2")
    def get_test_db_info2():
        from fonctions import write_info_file, get_dag_id
        liste = ["pc_inchikey_type_000009.ttl.gz",
                 "pc_inchikey_value_000012.ttl.gz",
                 ]
        info = {"download_url": "https://ftp.ncbi.nlm.nih.gov/pubchem/RDF/inchikey/",
                "version": "A",
                "file_list": liste,
                "dir_name": get_dag_id(),
                "compression": "gz",
                "format": "TURTLE"
                }
        write_info_file(info)

    @task(task_id="test_std_db_dir_name")
    def test1():
        from fonctions import std_db_directory_name

        assert "/test/unit_test_dag_vA/" == std_db_directory_name(path="/test",
                                                         force_version=False,
                                                         forced_version_value="new")

        assert "/test/unit_test_dag_vnew/" == std_db_directory_name(path="/test",
                                                           force_version=True,
                                                           forced_version_value="new")

        assert "/test/unit_test_dag_vnew/" == std_db_directory_name(path="/test/",
                                                           force_version=True,
                                                           forced_version_value="new")

        assert "/test/unit_test_dag_vnoVersionSpecified/" == std_db_directory_name(path="/test",
                                                                          force_version=True,
                                                                          forced_version_value="")

    @task(task_id="test_metadt_fname")
    def test2():
        from fonctions import metadata_filename
        assert metadata_filename("path_to/", ) == "path_to/unit_test_dag_metadata_latest"

        assert metadata_filename("path_to/", with_version=True) == "path_to/unit_test_dag_metadata_vA"

        assert (metadata_filename("path_to/", with_version=True, force_version=True)
                == "path_to/unit_test_dag_metadata_vnoForcedVersionSpecified")

        assert (metadata_filename("path_to/", with_version=True, force_version=True, forced_version_value="1")
                == "path_to/unit_test_dag_metadata_v1")

    @task(task_id="test_little_helpers")
    def test3():
        from fonctions import metadata_filename_base, load_info_file, get_version_string
        info = load_info_file()

        assert metadata_filename_base(info) == "unit_test_dag_metadata"
        assert get_version_string(force_version=False, forced_version_value="1") == "_vA"
        assert get_version_string(force_version=True, forced_version_value="1") == "_v1"

    @task(task_id="test_hdfs_upload_and_read")
    def test4():
        import os
        from fonctions import hdfs_upload, get_dag_param, get_client
        import json
        source_dir = get_dag_param("local_temp_repository")
        dest_dir = get_dag_param("hdfs_tmp_dir")
        filename = "testFile.txt"

        with open(source_dir + "/" + filename, 'w') as f:
            f.write('{"test_succeeded":"true"}')

        hdfs_upload(source_dir + "/" + filename, dest_dir + "/" + filename)

        os.remove(source_dir + "/" + filename)

        with get_client().read(dest_dir + "/" + filename, encoding='utf-8') as i:
            res: dict = json.load(i)

        assert res.get("test_succeeded", "false") == "true"

    @task(task_id="get_local_current_version_1")
    def test5():
        from fonctions import get_local_current_version, get_client, metadata_filename, get_dag_param
        get_client().delete(metadata_filename(get_dag_param("hdfs_data_dir")))
        get_client().delete(metadata_filename(get_dag_param("hdfs_data_dir"), with_version=True))
        assert get_local_current_version() == "0"

    @task(task_id="check_dl_and_dcomp")
    def test6():
        from fonctions import std_db_directory_name, get_dag_param, get_client
        hdfs_dir_path = std_db_directory_name(get_dag_param("hdfs_data_dir"))
        file_list: list = get_client().list(hdfs_dir_path, status=False)
        assert "pc_inchikey_type_000009.ttl" in file_list
        assert "pc_inchikey_value_000012.ttl" in file_list
        assert len(file_list) == 2

    @task(task_id="get_version_after_gen_meta")
    def test7():
        from fonctions import get_local_current_version, metadata_filename, get_dag_param, get_client
        assert get_local_current_version() == "A"

    @task(task_id="info_file_ops")
    def test8():
        import time
        from fonctions import write_info_file, load_info_file

        info_orig = load_info_file()
        write_info_file(info_orig)
        time.sleep(2)  # HDFS latency

        info_reloaded = load_info_file()
        assert info_reloaded == info_orig

        added_data: dict = {"test_validation_field": "ok", }
        info_reloaded.update(added_data)
        write_info_file(info_reloaded)
        time.sleep(2)

        info_reloaded2: dict = load_info_file()
        assert info_reloaded.get("test_validation_field") == "ok"

    @task(task_id="get_dag_param")
    def test9():
        from fonctions import get_dag_param
        assert get_dag_param("n_kept_versions") == 2
        assert get_dag_param("this_param_doesnt_exist") == ""

    @task(task_id="local_gunzip")
    def test10():
        from fonctions import local_gunzip, get_dag_param, load_json_hdfs
        import os
        import gzip
        import json
        local_tmp_dir = get_dag_param("local_temp_repository")
        filename = "testFile.json"
        path = local_tmp_dir + "/" + filename

        content = b'{"test_succeeded":"true"}'
        f = gzip.open(path + ".gz", 'wb')
        f.write(content)
        f.close()

        liste = local_gunzip(path + ".gz")
        assert path in liste
        assert len(liste) == 1

        with open(path, 'r') as f:
            j_content: dict = json.load(f)

        assert j_content.get("test_succeeded", "false") == "true"

        os.remove(path)

    @task(task_id="local_unzip")
    def test11():
        from fonctions import local_unzip, get_dag_param, load_json_hdfs
        import os
        from zipfile import ZipFile
        import json
        local_tmp_dir = get_dag_param("local_temp_repository")
        filename1 = "testFile1.json"
        filename2 = "testFile2.json"
        zip_name = "testFiles.zip"
        path = local_tmp_dir + "/" + zip_name
        path1 = local_tmp_dir + "/" + filename1
        path2 = local_tmp_dir + "/" + filename2

        content = '{"test_succeeded":"true"}'
        with open(path1, 'w') as f:
            f.write(content)
        with open(path2, 'w') as f:
            f.write(content)

        with ZipFile(path, 'w') as myzip:
            myzip.write(path1, filename1)
            myzip.write(path2, filename2)

        liste = local_unzip(path)
        print(liste)
        assert path1 in liste
        assert path2 in liste
        assert len(liste) == 2

        with open(path1, 'r') as f:
            j_content1: dict = json.load(f)
            assert j_content1.get("test_succeeded", "false") == "true"

        with open(path2, 'r') as f:
            j_content2: dict = json.load(f)
            assert j_content2.get("test_succeeded", "false") == "true"

        os.remove(path1)
        os.remove(path2)

    @task(task_id="http_dl")
    def test12():
        from fonctions import http_download, get_dag_param
        filename = "test10MB.pdf"
        local_test_path = get_dag_param("local_temp_repository") + "/" + filename
        hdfs_test_path = get_dag_param("hdfs_tmp_dir") + "/" + filename

        import os
        url = "https://link.testfile.org/PDF10MB"
        url2 = "http://speedtest.ftp.otenet.gr/files/test10Mb.db"

        buf = http_download(url)
        buf_size = round(len(buf) / (1024 * 1024))
        assert buf_size == 10

        buf2 = http_download(url2)
        buf_size2 = round(len(buf2) / (1024 * 1024))
        assert buf_size2 == 10

        http_download(url, local_test_path, local=True)
        file_stats = os.stat(local_test_path)
        file_size = round(file_stats.st_size / (1024 * 1024))
        assert file_size == 10
        os.remove(local_test_path)

        http_download(url2, local_test_path, local=True)
        file_stats = os.stat(local_test_path)
        file_size = round(file_stats.st_size / (1024 * 1024))
        assert file_size == 10
        os.remove(local_test_path)

        http_download(url, hdfs_test_path, local=False)
        from fonctions import get_client
        status = get_client().status(hdfs_test_path, strict=False)
        hdfs_file_size = round(int(status['length']) / (1024 * 1024))
        assert hdfs_file_size == 10
        get_client().delete(hdfs_test_path)

        http_download(url2, hdfs_test_path, local=False)
        from fonctions import get_client
        status = get_client().status(hdfs_test_path, strict=False)
        hdfs_file_size = round(int(status['length']) / (1024 * 1024))
        assert hdfs_file_size == 10
        get_client().delete(hdfs_test_path)

    test13 = SSHOperator(
        ssh_conn_id='msd_ssh_connection',
        task_id='run_graph_exploration',
        command="bash -l /usr/local/msd-database-management/apps/bin/braChemDbSubmit.sh test",
        cmd_timeout=None,
    )

    @task(task_id="post_db_clean")
    def post_db_clean():
        from fonctions import get_client, get_dag_param, load_info_file, metadata_filename_base
        hdfs_data_dir = get_dag_param("hdfs_data_dir")

        client = get_client()
        # récupération de la liste des fichiers de metadonnées de la base présents sur HDFS dans "paths"
        present_files_names = client.list(get_dag_param("hdfs_data_dir"), status=False)
        metadata_file_basename = metadata_filename_base(load_info_file())
        metafiles = list(filter(lambda s: (metadata_file_basename in s) and ('_latest' not in s), present_files_names))
        assert len(metafiles) == 2
    @task(task_id="pre_db_clean")
    def pre_db_clean():
        #generate mock old versions
        from fonctions import get_client, get_dag_param, metadata_filename,std_db_directory_name

        def gen_mock_db_files(year: str, version: str):
            json_info = {"version": version,
                    "date_importation_msd": year+"-01-04T12:14:14.839735",}

            hdfs_data_dir = get_dag_param("hdfs_data_dir")
            metaname = metadata_filename(hdfs_data_dir, with_version=True, force_version=True, forced_version_value=version)
            import json
            print("########## WRINTING TO " + metaname )
            with get_client().write(metaname, encoding='utf-8', overwrite=True) as fp:
                json.dump(json_info, fp)

            print("########## CREATING DIR  " + std_db_directory_name(hdfs_data_dir,force_version=True,forced_version_value=version))
            get_client().makedirs(std_db_directory_name(hdfs_data_dir,force_version=True,forced_version_value=version), permission=777)

        gen_mock_db_files("2021","B")
        gen_mock_db_files("2022", "C")
        gen_mock_db_files("2023", "D")



    @task(task_id="clean_files")
    def clean_test_files():
        from fonctions import get_dag_param, get_client, std_db_directory_name
        hdfs_dir_path = std_db_directory_name(get_dag_param("hdfs_data_dir"))
        get_client().delete(hdfs_dir_path, recursive=True, skip_trash=True)

    (get_test_db_info2()
     >> test1()
     >> test2()
     >> test3()
     >> test4()
     >> test5()
     >> single_download_and_decompress.expand(file=get_file_list())
     >> test6()
     >> gen_metadata()
     >> test7()
     >> test8()
     >> test9()
     >> test10()
     >> test11()
     >> test12()
     >> test13
     >> pre_db_clean()
     >> db_clean()
     >> post_db_clean()

     # >> clean_test_files()
     )


test_taskflow()
