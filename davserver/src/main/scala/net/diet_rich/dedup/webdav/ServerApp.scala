// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.webdav

import com.myastronomy.AstronomyResourceFactory

object ServerApp extends App {
  val maybeServer = for {
    fileSystem <- initFileSystem(if (args.isEmpty) Array("../target/playRepo") else args).right
    server <- initServer().right
  } yield server
  
  maybeServer.fold(println, _ join)
  
  
  def initFileSystem(args: Array[String]): Either[Error, FileSystem] =
    if (args.isEmpty)
      Left("missing repository argument")
    else
      FileSystemSingleton.configure(args(0), if (args.contains("DEFLATE")) 1 else 0)

  
  import org.eclipse.jetty.server.Server
  def initServer(): Either[Error, Server] = {
    try {
      val resourceFactory = new AstronomyResourceFactory
      val miltonConfigurator = new LocalMiltonConfigurator(resourceFactory)
      val miltonFilter = new LocalMiltonFilter(miltonConfigurator)
      val filterHolder = new org.eclipse.jetty.servlet.FilterHolder(miltonFilter)
          
      val servletHandler = new org.eclipse.jetty.servlet.ServletHandler()
      servletHandler.addFilterWithMapping(filterHolder, "/*", org.eclipse.jetty.servlet.FilterMapping.REQUEST);
          
      val server = new Server(8080)
      server.setHandler(servletHandler)
      server.start()
      Right(server)
    } catch {
      case e: Exception => Left(e.toString)
    }
  }
  
}
