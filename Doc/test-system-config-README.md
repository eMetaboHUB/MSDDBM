# Installation systeme de test pour Airflow sur VM Debian

## Qemu + Debian
```sudo apt install qemu-utils qemu-system-x86 qemu-system-gui
qemu-img create debian.img 15G
wget  https://cdimage.debian.org/cdimage/daily-builds/daily/arch-latest/amd64/iso-cd/debian-testing-amd64-netinst.iso
qemu-system-x86_64 -hda debian.img -cdrom debian-testing-amd64-netinst.iso -boot d -m 1024
```
Lancement:
```
qemu-system-x86_64 -hda debian.img -m 4096 -smp $(nproc)  -nic user,hostfwd=tcp::5022-:22
```
Aménagements:
```
su -
usermod -aG sudo <username>
apt install sudo
```
ctrl D pour reload sudoers puis

```
sudo apt-install sudo git ssh  default-jdk default-jre -y
```
Maintenant on peut se connecter en ssh
```
ssh localhost -p 5022
```

## SBT
```
sudo apt-get update
sudo apt-get install apt-transport-https curl gnupg -yqq
echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | sudo tee /etc/apt/sources.list.d/sbt.list
echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | sudo tee /etc/apt/sources.list.d/sbt_old.list
curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | sudo -H gpg --no-default-keyring --keyring gnupg-ring:/etc/apt/trusted.gpg.d/scalasbt-release.gpg --import
sudo chmod 644 /etc/apt/trusted.gpg.d/scalasbt-release.gpg
sudo apt-get update
sudo apt-get install sbt
```

## Hadoop
```
sudo useradd -r hadoop -m -d /opt/hadoop --shell /bin/bash
sudo passwd hadoop
su - hadoop
ssh-keygen -t rsa
cat ~/.ssh/id_rsa.pub > ~/.ssh/authorized_keys
wget https://dlcdn.apache.org/hadoop/common/hadoop-3.2.4/hadoop-3.2.4.tar.gz -O hadoop-3.2.4.tar.gz
tar -xzvf hadoop-3.2.4.tar.gz -C /opt/hadoop –strip-components=1
```

```
ls /usr/lib/jvm  # pour connaître java-home
```

ajouter à /opt/hadoop/.bashrc:

```
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export HADOOP_HOME=/opt/hadoop
export PATH=$PATH:$HADOOP_HOME/bin
export PATH=$PATH:$HADOOP_HOME/sbin
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_COMMON_HOME=$HADOOP_HOME
export HADOOP_HDFS_HOME=$HADOOP_HOME
export YARN_HOME=$HADOOP_HOME
export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native
export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib"
```

puis recharger:
```
source ~/.bashrc
```

ajouter à  /opt/hadoop/etc/hadoop/hadoop-env.sh :

```
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
```

ajouter à  /opt/hadoop/etc/hadoop/core-site.xml :
```
<property>
<name>fs.default.name</name>
<value>hdfs://localhost:9000</value>
</property>
```


ajout de ce qui suit à /opt/hadoop/etc/hadoop/hdfs-site.xml (ou copie du fichier donné en exemple) :
```
<property>
<name>dfs.replication</name>
<value>1</value>
</property>
<property>
<name>dfs.namenode.name.dir</name>
<value>file:/opt/hadoop/hadoop_tmp/hdfs/namenode</value>
</property>
<property>
<name>dfs.datanode.data.dir</name>
<value>file:/opt/hadoop/hadoop_tmp/hdfs/datanode</value>
</property>
```
ajout à yarn-site.xml de :
```
<property>
<name>yarn.nodemanager.aux-services</name>
<value>mapreduce_shuffle</value>
</property>
<property>
<name>yarn.nodemanager.aux-services.mapreduce.shuffle.class</name>
<value>org.apache.hadoop.mapred.ShuffleHandler</value>
</property>
```
et enfin ajout à mapred-site.xml :
```
<property>
<name>mapreduce.framework.name</name>
<value>yarn</value>
</property>
```

Création du FS et lancement : 
```
mkdir -p /opt/hadoop/hadoop_tmp/hdfs/{namenode,datanode}
hdfs namenode -format
start-dfs.sh
```

## Docker
```
for pkg in docker.io docker-doc docker-compose podman-docker containerd runc; do sudo apt-get remove $pkg; done
sudo apt-get update
sudo apt-get install ca-certificates curl gnupg
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/debian/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/debian \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
```

Si erreur avec Trixie release (Debian 12) : remplacer trixie par bookworm dans  /etc/apt/sources.list.d/docker.list

Installation : 
```
sudo apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
```







