package app

import java.io.File
import java.sql.Timestamp

import app.entity.Comment
import org.apache.kafka.common.serialization.{ByteArraySerializer, StringSerializer}
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.scalatest.{BeforeAndAfterAll, DoNotDiscover}
import org.scalatest.funsuite.AnyFunSuite

import scala.reflect.io.Directory

@DoNotDiscover
class StreamingSparkAppSuite3 extends AnyFunSuite with BeforeAndAfterAll{

  override def beforeAll(): Unit = {
    super.beforeAll()
    new Directory(new File("tmp/kafkaOffset")).deleteRecursively()
    new Directory(new File("tmp/file")).deleteRecursively()
    GlobalServices.kafka.clear()
    GlobalServices.postgres.executeFile("src/test/resources/pgsql/ddl/comments_create_table.sql")
    GlobalServices.postgres.truncate("comments")
  }

  test("Spark Streaming1") {
    val input = Seq(
      Comment(id = Some("123"), name = Some("John"), rating = Some(3), timestamp = Some(new Timestamp(1629510856L)),
        comment = Some("The villa is quite interesting, it's a theme park containing thousands of statues and dioramas, depicting stories of Chinese folklore.")
      ),
      Comment(id = Some("456"), name = Some("Peter"), rating = Some(4), timestamp = Some(new Timestamp(1629513856L)),
        comment = Some("Visited in 1997 and again this trip. Kitsch and cool and makes me wonder the sick mind of the founder.")
      ),
      Comment(id = Some("789"), name = Some("Mary"),rating = Some(1), timestamp =Some(new Timestamp(1629513853L)),
        comment = Some("One of the less known attractions of Singapore is Haw Par Villa, formerly known to many as the Tiger Balm Gardens, which I think is a must see venue.")
      )
    )
    val expectedOutput = Seq(
      Comment(id = Some("123"), name = Some("John"), rating = Some(3), ratingText=Some("average"), timestamp = Some(new Timestamp(1629510856L)),
        comment = Some("The villa is quite interesting, it's a theme park containing thousands of statues and dioramas, depicting stories of Chinese folklore.")
      ),
      Comment(id = Some("456"), name = Some("Peter"), rating = Some(4), ratingText=Some("good"), timestamp = Some(new Timestamp(1629513856L)),
        comment = Some("Visited in 1997 and again this trip. Kitsch and cool and makes me wonder the sick mind of the founder.")
      ),
      Comment(id = Some("789"), name = Some("Mary"),rating = Some(1), ratingText=Some("worst"), timestamp =Some(new Timestamp(1629513853L)),
        comment = Some("One of the less known attractions of Singapore is Haw Par Villa, formerly known to many as the Tiger Balm Gardens, which I think is a must see venue.")
      ))

    val spark = GlobalServices.sparkSession
    import spark.implicits._

    input.foreach(comment => GlobalServices.kafka.publish("my-topic-in",comment.id.get,comment.serialise(), new StringSerializer(),new ByteArraySerializer() ))

    StreamingSparkApp.main(Array("src/test/resources/config.yml", "my-app"))

    val jdbcConfig = StreamingSparkApp.config.get.jdbcConnection.get
    val jdbcResultDf = spark.read
      .format("jdbc")
      .option("url",jdbcConfig.url.get)
      .option("user", jdbcConfig.user.get)
      .option("password", jdbcConfig.password.get)
      .option("driver", "org.postgresql.Driver")
      .option("dbtable", StreamingSparkApp.config.get.jdbcTableOut.get(0))
      .load().as[Comment]

    jdbcResultDf.show()
    println(jdbcResultDf.collect().toSet)
    println(expectedOutput.toSet)
    assert (jdbcResultDf.collect().toSet.equals(expectedOutput.toSet))
    assert (jdbcResultDf.collect().length.equals(expectedOutput.length))
  }

}
