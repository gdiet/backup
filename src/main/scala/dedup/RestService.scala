package dedup

import com.sun.net.httpserver.{HttpExchange, HttpServer}
import dedup.db.{Database, withDb}

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import scala.util.Using

@main def restService(opts: (String, String)*): Unit =
  //  val repo = opts.repo
  val dbDir = opts.dbDir.tap(main.checkDbDir(_, true))
  val port = opts.getOrElse("port", "8080").toInt
  val server = HttpServer.create(new InetSocketAddress(port), 0)
  withDb(dbDir, readOnly = false) { (db: Database) =>
    server.createContext("/", exchange =>
      try
        (exchange.getRequestMethod, exchange.getRequestURI.getPath) match
          case "GET" -> "/dataids/page" => dataidsPage(db, exchange)
          case "GET" -> _ => exchange.sendResponseHeaders(404, -1) // Not Found
          case _ => exchange.sendResponseHeaders(405, -1) // Method Not Allowed
      catch
        case t: Throwable =>
          println(s"Request failed: ${exchange.getRequestURI}")
          println(t)
          exchange.sendResponseHeaders(500, -1) // Not Found
    )
    server.start()
    while (true) Thread.sleep(1000) // TODO is this the way to go here?
  }

private def dataidsPage(db: Database, exchange: HttpExchange) =
  // curl -v "http://localhost:8080/dataids/page?startAfter=-1&size=100"
  val params = exchange.getRequestURI.getQuery.split("&").map { param =>
    val Array(key, value) = param.split("=")
    key -> value
  }.toMap
  val startAfter = DataId(params.getOrElse("startAfter", "-1").toLong)
  val size = params.getOrElse("size", "100").toInt
  val response: String = db.dataIds(startAfter, size).mkString("[", ",", "]")
  exchange.getResponseHeaders.set("Content-Type", "application/json; charset=utf-8")
  exchange.sendResponseHeaders(200, response.getBytes(StandardCharsets.UTF_8).length)
  Using(exchange.getResponseBody)(_.write(response.getBytes))
