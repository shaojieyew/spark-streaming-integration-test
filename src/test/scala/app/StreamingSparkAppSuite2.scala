package app

import java.io.File
import java.sql.Timestamp

import app.entity.Comment
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, StringSerializer}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import suite.KafkaBaseSuite.kafka
import suite.SparkBaseSuite.sparkSession
import suite.{KafkaBaseSuite, RatingServiceSuite, SparkBaseSuite}

import scala.reflect.io.Directory

class StreamingSparkAppSuite2
  extends AnyFunSuite
    with KafkaBaseSuite
    with SparkBaseSuite
    with RatingServiceSuite
    with BeforeAndAfterAll
  {

  override def beforeAll(): Unit = {
    super.beforeAll()
    new Directory(new File("tmp/kafkaOffset")).deleteRecursively()
    new Directory(new File("tmp/test/my-app3/output")).deleteRecursively()
    kafka.clear()
  }

  test("( (kafka:my-topic-in1 -> my-app1) + (kafka:my-topic-in2 -> my-app2) ) -> kafka:my-topic-out -> my-app3 -> parquet") {

    val input1 = Seq(
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

    val input2 = Seq(
      Comment(id = Some("333"), name = Some("Mary"), rating = Some(4), timestamp = Some(new Timestamp(1629510856L)),
        comment = Some("This is a wonderful place")
      ),
      Comment(id = Some("444"), name = Some("Jane"), rating = Some(4), timestamp = Some(new Timestamp(1629510856L)),
        comment = Some("This park is worth visiting for all the wrong reasons and that's what makes it fun!")
      ),
      Comment(id = Some("555"), name = Some("May"),rating = Some(4),  timestamp =Some(new Timestamp(1629510856L)),
        comment = Some("The sculptures need some maintenance / painting, but not too bad condition.")
      ))

    val expectedOutput = Seq(
      Comment(id = Some("123"), name = Some("John"), rating = Some(3), ratingText=Some("average"), timestamp = Some(new Timestamp(1629510856L)),
        comment = Some("The villa is quite interesting, it's a theme park containing thousands of statues and dioramas, depicting stories of Chinese folklore.")
      ),
      Comment(id = Some("456"), name = Some("Peter"), rating = Some(4), ratingText=Some("good"), timestamp = Some(new Timestamp(1629513856L)),
        comment = Some("Visited in 1997 and again this trip. Kitsch and cool and makes me wonder the sick mind of the founder.")
      ),
      Comment(id = Some("789"), name = Some("Mary"),rating = Some(1), ratingText=Some("worst"), timestamp =Some(new Timestamp(1629513853L)),
        comment = Some("One of the less known attractions of Singapore is Haw Par Villa, formerly known to many as the Tiger Balm Gardens, which I think is a must see venue.")
      ),
      Comment(id = Some("333"), name = Some("Mary"), rating = Some(4), ratingText=Some("good"), timestamp = Some(new Timestamp(1629510856L)),
        comment = Some("This is a wonderful place")
      ),
      Comment(id = Some("444"), name = Some("Jane"), rating = Some(4), ratingText=Some("good"), timestamp = Some(new Timestamp(1629510856L)),
        comment = Some("This park is worth visiting for all the wrong reasons and that's what makes it fun!")
      ),
      Comment(id = Some("555"), name = Some("May"),rating = Some(4), ratingText=Some("good"), timestamp =Some(new Timestamp(1629510856L)),
        comment = Some("The sculptures need some maintenance / painting, but not too bad condition.")
      )
    )

    kafka.publishSeq("my-topic-in1",input1, (obj:Comment)=>obj.id.get ,(obj:Comment)=>Comment.serialise(obj) , new StringSerializer(),new ByteArraySerializer() )
    kafka.publishSeq("my-topic-in2",input2, (obj:Comment)=>obj.id.get ,(obj:Comment)=>Comment.serialise(obj) , new StringSerializer(),new ByteArraySerializer() )

    StreamingSparkApp.main(Array("src/test/resources/config.yml", "my-app1"))
    StreamingSparkApp.main(Array("src/test/resources/config.yml", "my-app2"))
    StreamingSparkApp.main(Array("src/test/resources/config.yml", "my-app3"))


    val spark  = sparkSession
    import spark.implicits._

    val parquetResult  = SparkBaseSuite.sparkSession.read
      .parquet("tmp/test/my-app3/output")
      .as[Comment].collect()

    assert (parquetResult.toSet.equals(expectedOutput.toSet))
    assert (parquetResult.length.equals(expectedOutput.length))

  }

}
