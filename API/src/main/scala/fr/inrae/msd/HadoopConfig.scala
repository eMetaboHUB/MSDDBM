package fr.inrae.msd

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import sbtBuildInfo.BuildInfo

import java.io.File
import java.net.URI

/**
 * Helper to access Hadoop FS during tests or local queries (do not use with Spark jobs)
 *
 * @param fileSystem file system to be accessed be client objects
 * @param dataDir    String path, indicates where the graphs are
 * @param url        the HDFS url
 */
case class HadoopConfig(fileSystem: FileSystem,
                        dataDir: String,
                        url: String,
                       )

/** Companion to class HadoopConfig. Creates an object based on a given config file.
 *
 */
object HadoopConfig {

  val apiVersion: String = BuildInfo.version

  /**
   * @param configFile a string representing the path of the local config file
   * @return an object that allows access to the datalake without using Spark
   */
  def apply(configFile: String): HadoopConfig = {
    org.apache.log4j.BasicConfigurator.configure()
    org.apache.log4j.Logger.getRootLogger.setLevel(org.apache.log4j.Level.WARN)


    import com.typesafe.config.{Config, ConfigFactory}
    val applicationConf: Config = ConfigFactory.parseFile(new File(configFile))

    val HADOOP_CONF_DIR: String = applicationConf.getString("hadoop.CONF_DIR")
    val HADOOP_USER_NAME: String = applicationConf.getString("hadoop.USER_NAME")
    val HADOOP_HOME_DIR: String = applicationConf.getString("hadoop.HOME_DIR")
    val HADOOP_URL: String = applicationConf.getString("hadoop.URL")
    val HDFS_DATA_DIR: String = applicationConf.getString("datalake.DATA_DIR")


    val conf = new Configuration()
    conf.addResource(new Path("file:///" + HADOOP_CONF_DIR + "/core-site.xml"))
    conf.addResource(new Path("file:///" + HADOOP_CONF_DIR + "/fileSystem-site.xml"))
    conf.set("fs.fileSystem.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    conf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)
    System.setProperty("HADOOP_USER_NAME", HADOOP_USER_NAME)
    System.setProperty("hadoop.home.dir", HADOOP_HOME_DIR)

    val fileSystem: FileSystem = FileSystem get(new URI(HADOOP_URL), conf)

    new HadoopConfig(fileSystem, HDFS_DATA_DIR, HADOOP_URL)
  }

}
