package service.httpServer

class RatingApiServer( socketAddress: String = "localhost",
                     port: Int = 9090,
                     backlog: Int = 0) extends SimpleHttpServer( socketAddress,
  port,
  backlog) {
  get("/rating", Map[String,String]("rating"->"1")) {
    "worst"
  }
  get("/rating", Map[String,String]("rating"->"2")) {
    "bad"
  }
  get("/rating", Map[String,String]("rating"->"3")) {
    "average"
  }
  get("/rating", Map[String,String]("rating"->"4")) {
    "good"
  }
  get("/rating", Map[String,String]("rating"->"5")) {
    "awesome"
  }
}