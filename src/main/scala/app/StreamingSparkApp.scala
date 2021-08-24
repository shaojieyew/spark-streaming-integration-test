package app

import java.sql.DriverManager

import app.`extension`.{ConfigLoader, SparkStarter}
import app.entity.{Comment, KafkaRecord}
import org.apache.http.HttpHeaders
import org.apache.http.client.methods.HttpGet
import org.apache.http.entity.ContentType
import org.apache.spark.sql.DataFrame

object StreamingSparkApp extends BaseApp with ConfigLoader with SparkStarter {

  override def process(): Unit = {
    val spark = sparkSession

    import spark.implicits._

    val streamingDf = spark
      .readStream
      .format("kafka")
      .option("startingOffsets", config.get.startingOffsets.get)
      .option("kafka.bootstrap.servers", config.get.kafkaHosts.get.mkString(","))
      .option("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      .option("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer")
      .option("subscribePattern", config.get.kafkaTopicIn.get.mkString("|"))
      .load()
    streamingDf.writeStream.foreachBatch((d: DataFrame, batch: Long) => {


      val commentDf = d.as[KafkaRecord]
        .map(row => {
          val comment = Comment.deserialise(row.value.get)
          val result = callWebService(config.get.enrichmentUrl.get + "?rating=" + comment.rating.get).replaceAll("^[\n\r]", "").replaceAll("[\n\r]$", "")
          if (result.length == 0) {
            comment.copy(ratingText = Some("-"))
          } else {
            comment.copy(ratingText = Some(result))
          }
        })

      if (config.get.fileOut.isDefined && config.get.fileOut.get.nonEmpty) {
        commentDf.write
          .mode("append")
          .partitionBy("id")
          .parquet(config.get.fileOut.get(0))
      }

      if (config.get.kafkaTopicOut.isDefined && config.get.kafkaTopicOut.get.nonEmpty) {
        val kafkaRecordDf = commentDf.map(comment => KafkaRecord(key = comment.id, value = Some(comment.serialise()), topic = Some(config.get.kafkaTopicOut.get.head)))
        kafkaRecordDf.write
          .format("kafka")
          .option("kafka.bootstrap.servers", config.get.kafkaHosts.get.mkString(","))
          .save()
      }
    })
      .option("checkpointLocation", config.get.checkpoint.get)
      .start()
      .awaitTermination()
  }

  def callWebService(url: String): String = {
    var result = ""
    import org.apache.http.util.EntityUtils
    val request = new HttpGet(url)
    request.addHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.toString)
    import org.apache.http.impl.client.HttpClients
    val httpClient = HttpClients.createDefault
    try {
      val response = httpClient.execute(request)
      try {
        val entity = response.getEntity
        val headers = entity.getContentType
        if (entity != null) {
          result = EntityUtils.toString(entity)

        }
      } finally if (response != null)
        response.close()
    }
    result
  }

}
