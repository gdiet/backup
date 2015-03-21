package net.diet_rich.dedup.davserver

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{FilterMapping, ServletHandler, FilterHolder}

import net.diet_rich.dedup.core.Repository
import net.diet_rich.dedup.util._

object Main extends App with Logging {
  CommandLineUtils.forArgs(args) { argDetails => import argDetails._
    require(command == "davserver")
    val serverPort = port getOrElse 8080

    val resourceFactory =
      if (!writable){
        checkOptionUse()
        new DedupResourceFactoryReadOnly(Repository openReadOnly repositoryDir)
      } else {
        checkOptionUse(storeMethod, parallel, versionComment)
        new DedupResourceFactoryReadWrite(Repository openReadWrite (repositoryDir, storeMethod, parallel, versionComment))
      }
    val miltonConfigurator = new DedupMiltonConfigurator(resourceFactory)
    val miltonFilter = new DedupMiltonFilter(miltonConfigurator)
    val filterHolder = new FilterHolder(miltonFilter)
    val servletHandler = init(new ServletHandler()){_ addFilterWithMapping (filterHolder, "/*", FilterMapping.REQUEST)}
    val server = init(new Server(serverPort)){_ setHandler servletHandler}

    server start()
    log info s"Started dedup webdav server on port $serverPort"
    log info s"Write access is ${if (writable) "ENABLED" else "OFF"}"

    sys addShutdownHook {
      log info s"Dedup webdav server stopping..."
      server.stop()
      resourceFactory.close()
    }
    Thread sleep Long.MaxValue
  }
}
