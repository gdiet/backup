// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.webdav

import scala.util.control.NonFatal

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{FilterMapping, ServletHandler, FilterHolder}

import net.diet_rich.dedup.util.{init, Logging}

object ServerApp extends App with Logging {

  require(!(args isEmpty), "parameters: <repository path> [READWRITE] [DEFLATE] [port:(8080)]")

  val repositoryPath :: options = args.toList
  val writeEnabled = options contains "READWRITE"
  val deflate = options contains "DEFLATE"
  val serverPort = options find (_ startsWith "port:") map (_ substring 5) getOrElse "8080" toInt

  val maybeServer = initServer(serverPort)
  maybeServer.fold(throw _, _ join)

  def initServer(serverPort: Int): Either[Error, Server] = {
    try {
      val resourceFactory = new DedupResourceFactory(repositoryPath, writeEnabled = writeEnabled, deflate = deflate)
      val miltonConfigurator = new LocalMiltonConfigurator(resourceFactory)
      val miltonFilter = new LocalMiltonFilter(miltonConfigurator)
      val filterHolder = new FilterHolder(miltonFilter)
      val servletHandler = init(new ServletHandler()){ _ addFilterWithMapping (filterHolder, "/*", FilterMapping.REQUEST) }
      val server = init(new Server(serverPort)){ _ setHandler servletHandler }

      log info s"starting webdav server on port $serverPort"
      server start()
      Right(server)
    } catch {
      case NonFatal(e) => Left(e)
    }
  }
}
