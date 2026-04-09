package dedup

import com.sun.net.httpserver.{HttpExchange, HttpServer}
import dedup.db.{Database, withDb}
import dedup.store.LongTermStore

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import scala.util.Using

@main def restService(opts: (String, String)*): Unit =
  val dbDir = opts.dbDir.tap(main.checkDbDir(_, true))
  val dataDir = store.dataDir(opts.repo)
  val port = opts.getOrElse("port", "8080").toInt
  val server = HttpServer.create(new InetSocketAddress("0.0.0.0", port), 0)
  val dataRoute = """/data/(\d+)""".r
  Using(LongTermStore(dataDir, true)) { lts =>
    withDb(dbDir, readOnly = true) { (db: Database) =>
      server.createContext("/", exchange =>
        try
          (exchange.getRequestMethod, exchange.getRequestURI.getPath) match
            case "GET" -> "/data/ids" => dataidsPage(db, exchange)
            case "GET" -> dataRoute(dataId) => data(db, lts, exchange, DataId(dataId.toLong))
            case "GET" -> _ => exchange.sendResponseHeaders(404, -1) // Not Found
            case _ => exchange.sendResponseHeaders(405, -1) // Method Not Allowed
        catch
          case t: Throwable =>
            println(s"Request failed: ${exchange.getRequestURI}")
            println(t)
            exchange.sendResponseHeaders(500, -1) // Not Found
      )
      server.start()
      while(true) Thread.sleep(1000) // We could use a shutdown hook instead, without automatic resource management
    }
  }

private def dataidsPage(db: Database, exchange: HttpExchange) =
  // curl -v "http://localhost:8080/data/ids?startAfter=-1&size=100"
  val params = Option(exchange.getRequestURI.getQuery).getOrElse("").split("&").flatMap { param =>
    if param.contains("=") then
      val Array(key, value) = param.split("=", 2)
      Some(key -> value)
    else None
  }.toMap
  val startAfter = DataId(params.getOrElse("startAfter", "-1").toLong)
  val size = params.getOrElse("size", "100").toInt
  val response: String = db.dataIds(startAfter, size).mkString("[", ",", "]")
  exchange.getResponseHeaders.set("Content-Type", "application/json; charset=utf-8")
  exchange.sendResponseHeaders(200, response.getBytes(StandardCharsets.UTF_8).length)
  Using(exchange.getResponseBody)(_.write(response.getBytes))

private def data(db: Database, lts: LongTermStore, exchange: HttpExchange, dataId: DataId) =
  val parts = db.parts(dataId)
  if parts.size == 0 then
    exchange.sendResponseHeaders(404, -1) // Not Found
  else
    val size = parts.map(_._2).sum
    exchange.getResponseHeaders.set("Content-Type", "application/octet-stream")
    exchange.sendResponseHeaders(200, size)
    Using(exchange.getResponseBody) { out =>
      parts.foreach { case (start, length) =>
        lts.read(start, length, 0).foreach { case (_, data) =>  out.write(data) }
      }
    }
