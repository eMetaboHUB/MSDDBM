from hdfs import InsecureClient
import os
import subprocess

hdfs_input_path = os.environ["HDFS_INPUT_PATH"]
hdfs_output_path = os.environ["HDFS_OUTPUT_PATH"]
host = os.environ["HADOOP_HOST"]
user = os.environ["HADOOP_USERNAME"]

client = InsecureClient(url=host, user=user)
print("HDFS : downloading from " + host + " as user " + user)
client.download(hdfs_input_path, "/tmp/in.xml", overwrite=True)

subprocess.run([
    "java", "-jar", "SBML2RDF.jar",
    "-i", "/tmp/in.xml",
    "-u", "https://zenodo.org/records/10303455",
    "-o", "/tmp/out.ttl"
])

client.upload(hdfs_output_path, "/tmp/out.ttl", overwrite=True)

os.remove("/tmp/in.xml")
os.remove("/tmp/out.ttl")
client.delete(hdfs_input_path)
