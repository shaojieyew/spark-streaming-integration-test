package app.entity
import java.sql.Timestamp
case class KafkaRecord(key:Option[String]=None,
                   value:Option[Array[Byte]]=None,
                   topic: Option[String]=None,
                   partition:Option[Int]=None,
                   offset:Option[Long]=None,
                   timestamp:Option[Timestamp] = None,
                   timestampType:Option[Int]=None )
