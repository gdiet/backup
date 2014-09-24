// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.webdav

import scala.util.control.NonFatal

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{FilterMapping, ServletHandler, FilterHolder}

import net.diet_rich.dedup.core.backupDatabase
import net.diet_rich.dedup.core.values.StoreMethod
import net.diet_rich.dedup.util.{ConsoleApp, init, Logging}

object DavServerApp extends ConsoleApp with Logging {
  checkUsage("parameters: <repository path> [READWRITE] [DEFLATE] [port:(8080)]")
  val writeEnabled = options contains "READWRITE"
  if (writeEnabled) backupDatabase(repositoryDirectory)
  val storeMethod = if (options contains "DEFLATE") StoreMethod.DEFLATE else StoreMethod.STORE

  val maybeServer = initServer(option("port:", "8080") toInt)
  maybeServer.fold(throw _, _ join)

  def initServer(serverPort: Int): Either[Error, Server] = {
    try {
      val resourceFactory = new DedupResourceFactory(repositoryDirectory, writeEnabled = writeEnabled, storeMethod)
      val miltonConfigurator = new DedupMiltonConfigurator(resourceFactory)
      val miltonFilter = new DedupMiltonFilter(miltonConfigurator)
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
