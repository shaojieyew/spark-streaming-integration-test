package app

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.scalatest.{BeforeAndAfterAll, Suites}
import service.{EmbeddedKafkaService, EmbeddedPostgresService}
import service.httpServer.RatingApiServer


object GlobalServices {
  var sparkSession:SparkSession = _
  var postgres:EmbeddedPostgresService = _
  var ratingApiServer:RatingApiServer = _
  var kafka:EmbeddedKafkaService = _
}

class MasterSuite extends Suites(
  new StreamingSparkAppSuite1,
  new StreamingSparkAppSuite2,
  new StreamingSparkAppSuite3
) with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    // start kafka - zookeeper
    GlobalServices.kafka = new EmbeddedKafkaService()
    GlobalServices.kafka.start()

    // start rating api server
    GlobalServices.ratingApiServer = new RatingApiServer()
    GlobalServices.ratingApiServer.start()

    // start sparkSession
    GlobalServices.sparkSession = SparkSession.builder()
      .appName("testing")
      .master("local[*]")
      .getOrCreate()
    GlobalServices.sparkSession.sparkContext.setLogLevel("ERROR")
    GlobalServices.sparkSession.streams.addListener(new StreamingQueryListener {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
      }
      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        if(event.progress.batchId==1){
          GlobalServices.sparkSession.streams.active.foreach(s=>s.stop())
        }
      }
      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
      }
    })

    // start postgres server
    GlobalServices.postgres = new EmbeddedPostgresService()
    GlobalServices.postgres.start()
  }

  override def afterAll(): Unit = {
    GlobalServices.kafka.stop()
    GlobalServices.sparkSession.stop()
    GlobalServices.postgres.stop()
    GlobalServices.ratingApiServer.stop()
  }
}