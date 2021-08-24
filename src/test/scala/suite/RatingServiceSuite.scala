package suite
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import service.httpServer.RatingApiServer

object RatingServiceSuite {
  var ratingApiServer:RatingApiServer = _
}

trait RatingServiceSuite extends AnyFunSuite with BeforeAndAfterAll {

  override def beforeAll(): Unit = {
    super.beforeAll()
    RatingServiceSuite.ratingApiServer = new RatingApiServer()
    RatingServiceSuite.ratingApiServer.start()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    RatingServiceSuite.ratingApiServer.stop()
  }
}