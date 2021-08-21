package app.`extension`

import app.BaseApp
import app.config.AppConfig
import org.apache.spark.sql.SparkSession

/**
 * create spark context
 */

trait SparkStarter extends BaseApp with ConfigLoader {

  var sparkSession: SparkSession = _

  override def preprocess(args: Array[String]): Unit = {
    val appName = args(1)
    val config = super.getConfig(args)

    var sparkSessionBuilder = SparkSession.builder()
      .appName(appName)
    if(config.isDefined) {
      config.get.sparkConfig.get.foreach(entry => {
        sparkSessionBuilder = sparkSessionBuilder.config(entry._1, entry._2)
      })
    }
    sparkSession = sparkSessionBuilder.getOrCreate()
  }
}
