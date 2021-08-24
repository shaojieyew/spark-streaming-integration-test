package app

import java.io.File
import java.sql.Timestamp

import app.entity.Comment
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, StringSerializer}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import suite.KafkaBaseSuite.kafka
import suite.{KafkaBaseSuite, RatingServiceSuite, SparkBaseSuite}

import scala.reflect.io.Directory

class StreamingSparkAppSuite1
  extends AnyFunSuite
    with KafkaBaseSuite
    with SparkBaseSuite
    with RatingServiceSuite
    with BeforeAndAfterAll
  {

  override def beforeAll(): Unit = {
    super.beforeAll()
    new Directory(new File("tmp/kafkaOffset")).deleteRecursively()
  }

  test("kafka:my-topic-in1 -> my-app1 -> kafka:my-topic-out") {
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

    input.foreach(comment => kafka.publish("my-topic-in1",comment.id.get,comment.serialise(), new StringSerializer(),new ByteArraySerializer() ))

    StreamingSparkApp.main(Array("src/test/resources/config.yml", "my-app1"))

    val kafkaResult =  kafka.consume("my-topic-out", 3, new ByteArrayDeserializer()).map(Comment.deserialise)

    assert (kafkaResult.toSet.equals(expectedOutput.toSet))
    assert (kafkaResult.length.equals(expectedOutput.length))

  }

}
