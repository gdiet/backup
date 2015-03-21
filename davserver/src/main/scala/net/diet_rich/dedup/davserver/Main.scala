package net.diet_rich.dedup.davserver

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{FilterMapping, ServletHandler, FilterHolder}

import net.diet_rich.dedup.core.Repository
import net.diet_rich.dedup.core.data.StoreMethod
import net.diet_rich.dedup.util._

object Main extends App with Logging {
  CommandLineUtils.forArgs(args) { argDetails => import argDetails._
    require(command == "davserver")

    // FIXME only parse options that are really needed
    // FIXME check options use
    val resourceFactory =
      if (!writable) new DedupResourceFactoryReadOnly(Repository openReadOnly repositoryDir)
      else new DedupResourceFactoryReadWrite(Repository openReadWrite (repositoryDir, storeMethod, parallel, versionComment))
    val miltonConfigurator = new DedupMiltonConfigurator(resourceFactory)
    val miltonFilter = new DedupMiltonFilter(miltonConfigurator)
    val filterHolder = new FilterHolder(miltonFilter)
    val servletHandler = init(new ServletHandler()){_ addFilterWithMapping (filterHolder, "/*", FilterMapping.REQUEST)}
    val server = init(new Server(port getOrElse 8080)){_ setHandler servletHandler}

    server start()
    log info s"started dedup webdav server on port $port"
    log info s"write access is ${if (writable) "ENABLED" else "OFF"}"

    sys addShutdownHook {
      log info s"dedup webdav server stopping..."
      server.stop()
      resourceFactory.close()
    }
    Thread sleep Long.MaxValue
  }
}
