package app.config

case class Config(kafkaHosts: Option[List[String]]=None,
                  jobs: Option[List[AppConfig]]=None)

case class AppConfig(name:Option[String]=None,
                     checkpoint: Option[String]=None,
                     kafkaTopicIn: Option[List[String]]=None,
                     kafkaTopicOut: Option[List[String]]=None,
                     fileIn: Option[List[String]]=None,
                     fileOut: Option[List[String]]=None,
                     jdbcTableOut:Option[List[String]]=None,
                     jdbcTableIn:Option[List[String]]=None,
                     startingOffsets: Option[String]=None,
                     kafkaHosts: Option[List[String]]=None,
                     sparkConfig: Option[Map[String, String]]=None,
                     enrichmentUrl: Option[String] = None,
                     jdbcConnection: Option[JdbcConfig] = None)

case class JdbcConfig(url: Option[String] = None,
                      user: Option[String] = None,
                      password:  Option[String] = None)