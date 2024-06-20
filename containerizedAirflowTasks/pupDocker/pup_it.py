from hdfs import InsecureClient
import os
import subprocess

hdfs_input_path = os.environ["HDFS_INPUT_PATH"]
hdfs_output_path = os.environ["HDFS_OUTPUT_PATH"]
host = os.environ["HADOOP_HOST"]
user = os.environ["HADOOP_USERNAME"]

client = InsecureClient(url=host, user=user)
print("HDFS : downloading from " + host + " as user " + user)
client.download(hdfs_input_path, "/tmp/inpup.tmp", overwrite=True)

with open("/tmp/outpup.tmp", "w") as file1:
    file1.write(str(subprocess.getoutput("/pup -f /tmp/inpup.tmp 'tr json{}'")))
client.upload(hdfs_output_path, "/tmp/outpup.tmp", overwrite=True)

os.remove("/tmp/inpup.tmp")
os.remove("/tmp/outpup.tmp")
client.delete(hdfs_input_path)
