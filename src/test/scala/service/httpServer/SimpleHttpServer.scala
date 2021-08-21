package service.httpServer

import java.net.InetSocketAddress

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}

import scala.collection.mutable.HashMap

abstract class SimpleHttpServerBase(val socketAddress: String ,
                                    val port: Int,
                                    val backlog: Int) extends HttpHandler {
  private val address = new InetSocketAddress(socketAddress, port)
  private val server = HttpServer.create(address, backlog)
  server.createContext("/", this)

  def respond(exchange: HttpExchange, code: Int = 200, body: String = "") {
    val bytes = body.getBytes
    exchange.sendResponseHeaders(code, bytes.size)
    exchange.getResponseBody.write(bytes)
    exchange.getResponseBody.write("\r\n\r\n".getBytes)
    exchange.getResponseBody.close()
    exchange.close()
  }

  def start() = server.start()

  def stop(delay: Int = 1) = server.stop(delay)
}

abstract class SimpleHttpServer( socketAddress: String ,
                                 port: Int ,
                                 backlog: Int) extends SimpleHttpServerBase( socketAddress,
  port,
  backlog) {

  def queryToMap(query: String): Map[String, String] = {
    var result = Map[String,String]()
    if(query==null || query.length==0){
      result
    }else{
      for (param <- query.split("&")) {
        val pair = param.split("=")
        if (pair.length > 1) result += (pair(0)-> pair(1))
        else result += (pair(0)-> "")
      }
      result
    }
  }

  private val mappings = new HashMap[(String, Map[String, String], String), () => Any]

  def get(path: String, param: Map[String, String])(action: => Any) = mappings += (path, param, "GET") -> (() => action)
  def post(path: String, param: Map[String, String])(action: => Any) = mappings += (path, param, "POST") -> (() => action)
  def put(path: String, param: Map[String, String])(action: => Any) = mappings += (path, param, "PUT") -> (() => action)

  def handle(exchange: HttpExchange) = mappings.get(exchange.getRequestURI.getPath,queryToMap(exchange.getRequestURI.getQuery), exchange.getRequestMethod )
  match {
    case None => respond(exchange, 404)
    case Some(action) => try {
      respond(exchange, 200, action().toString)
    } catch {
      case ex: Exception => respond(exchange, 500, ex.toString)
    }
  }
}

