// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.webdav

object ServerApp extends App {
  // Filesystem bei Fehler runterfahren
  val maybeServer = for {
    fileSystem <- initFileSystem(if (args.isEmpty) Array("../target/playRepo") else args).right
    server <- initServer(fileSystem).right
  } yield server
  
  maybeServer.fold(println, _ join)
  
  
  def initFileSystem(args: Array[String]): Either[Error, FileSystem] =
    if (args.isEmpty) Left("usage: <java call> <repository path> [READWRITE] [DEFLATE]")
    else FileSystem(
      repositoryPath = args(0),
      writeEnabled = args.contains("READWRITE"),
      deflate = args.contains("DEFLATE")
    )

  
  import org.eclipse.jetty.server.Server
  def initServer(fileSystem: FileSystem): Either[Error, Server] = {
    try {
      val resourceFactory = new DedupResourceFactory(fileSystem)
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
