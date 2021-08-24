package suite
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.{BeforeAndAfterAll, Suites}
import service.EmbeddedKafkaService

object KafkaBaseSuite {
  var kafka:EmbeddedKafkaService = _
}

trait KafkaBaseSuite extends AnyFunSuite with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    super.beforeAll()
    KafkaBaseSuite.kafka = new EmbeddedKafkaService()
    KafkaBaseSuite.kafka.start()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    KafkaBaseSuite.kafka.stop()
  }
}