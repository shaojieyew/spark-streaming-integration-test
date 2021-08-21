package app.`extension`

import java.io.{File, FileInputStream, InputStreamReader}
import org.json4s._
import org.json4s.jackson.JsonMethods._

import app.BaseApp
import app.config.{AppConfig, Config}
import cats.syntax.either._
import com.fasterxml.jackson.databind.DeserializationFeature
import io.circe.generic.auto._
import io.circe.{Error, yaml}
import com.fasterxml.jackson.databind.ObjectMapper

/**
 * Load AppConfig with first 2 args, configPath and appName
 */

trait ConfigLoader extends BaseApp {
  var config: Option[AppConfig] = _
  var configPath:String = _
  var appName:String = _

  def getConfig(args: Array[String]): Option[AppConfig] = {
    config = loadConfig(args)
    config
  }

  def loadConfig(args: Array[String]): Option[AppConfig] = {
    if(args.length==1){
    throw new Exception("Missing args");
  }
    val configPath = args(0)
    val appName = args(1)

    if(!new File(configPath).exists()){
      throw new Exception("Invalid config file");
    }
     Some(ConfigLoader.load(configPath, appName))
  }

}

object ConfigLoader {

  implicit val formats = DefaultFormats
  def jsonStrToMap(jsonStr: String): Config = {
    parse(jsonStr).camelizeKeys.extract[Config]
  }

  def load(path: String, appName: String): AppConfig ={
    val configFileStream = new FileInputStream(path)
    val yml = yaml.parser.parse(new InputStreamReader(configFileStream))

    val configJson = yml.leftMap(err => err: Error)
    val config:Config = jsonStrToMap(configJson.right.get.toString)
    val appConfig = config.jobs.get.filter(_.name.get.equals(appName)).head
    if(appConfig.kafkaHosts.isEmpty){
      appConfig.copy(kafkaHosts = config.kafkaHosts)
    }else{
      appConfig
    }

  }
}

