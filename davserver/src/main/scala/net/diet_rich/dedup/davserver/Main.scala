package net.diet_rich.dedup.davserver

import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{FilterMapping, ServletHandler, FilterHolder}

import net.diet_rich.dedup.core.Repository
import net.diet_rich.dedup.core.data.StoreMethod
import net.diet_rich.dedup.util._

object Main extends App {
  if (args.length < 2) println("arguments: <command> <repository> [key:value options]") else {
    val commandLineUtils = CommandLineUtils(args)
    import commandLineUtils._
    require(command == "davserver")

    val writable = optional("writable") getOrElse "false" toBoolean
    val davPort = intOptional("port") getOrElse 8080
    val parallel = intOptional("parallel")
    val storeMethod = optional("storeMethod") map StoreMethod.named
    val maxBytesToCache = intOptional("maxBytesToCache") getOrElse 250000000
    val versionComment = optional("versionComment")

    if (writable)
      serverLifeCycle(Repository openReadWrite (repositoryDir, storeMethod, parallel, versionComment))
    else
      serverLifeCycle(Repository openReadOnly repositoryDir)

    def serverLifeCycle[R <: Repository](repository: R) {
      val resourceFactory = new DedupResourceFactory(repository)
      val miltonConfigurator = new DedupMiltonConfigurator(resourceFactory)
      val miltonFilter = new DedupMiltonFilter(miltonConfigurator)
      val filterHolder = new FilterHolder(miltonFilter)
      val servletHandler = init(new ServletHandler()){_ addFilterWithMapping (filterHolder, "/*", FilterMapping.REQUEST)}
      val server = init(new Server(davPort)){_ setHandler servletHandler}

      server start()
      println(s"started dedup webdav server on port $davPort")
      println("write access is " + (if (writable) "ENABLED" else "OFF"))

      sys addShutdownHook {
        println("dedup ftp server stopping...")
        server.stop()
        repository.close()
      }
      Thread sleep Long.MaxValue
    }
  }
}
