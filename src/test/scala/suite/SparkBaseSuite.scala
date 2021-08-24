package suite

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, Suites}
import service.EmbeddedKafkaService
import service.httpServer.RatingApiServer

object SparkBaseSuite {
  var sparkSession:SparkSession = _
}

trait SparkBaseSuite extends AnyFunSuite with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    super.beforeAll()
    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    SparkBaseSuite.sparkSession = SparkSession.builder()
      .appName(this.getClass.getCanonicalName)
      .master("local[*]")
      .getOrCreate()
    SparkBaseSuite.sparkSession.sparkContext.setLogLevel("ERROR")
    SparkBaseSuite.sparkSession.streams.addListener(new StreamingQueryListener {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
      }
      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
        if(event.progress.batchId==1){
          SparkBaseSuite.sparkSession.streams.active.foreach(s=>s.stop())
        }
      }
      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
      }
    })
  }

  override def afterAll(): Unit = {
    super.afterAll()
    SparkBaseSuite.sparkSession.stop()
  }
}