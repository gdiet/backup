// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.webdav

import net.diet_rich.util.Logging

object ServerApp extends App {
  val writeEnabled = args.contains("READWRITE")
  val deflate = args.contains("DEFLATE")
  
  val maybeServer = for {
    repositoryPath <- repositoryPathFromArgs(args).right
    fileSystem <- initFileSystem(repositoryPath).right
    // TODO Filesystem bei Fehler runterfahren
    server <- initServer(fileSystem).right
  } yield server
  
  maybeServer.fold(println, _ join)

  
  def repositoryPathFromArgs(args: Array[String]): Either[Error, String] =
    if (args isEmpty) Left("usage: <java call> <repository path> [READWRITE] [DEFLATE]")
    else Right(args(0))
  
  
  def initFileSystem(repositoryPath: String): Either[Error, DedupFileSystem] =
    DedupFileSystem(
      repositoryPath = repositoryPath,
      writeEnabled = writeEnabled,
      deflate = deflate
    )

  
  import org.eclipse.jetty.server.Server
  def initServer(fileSystem: DedupFileSystem): Either[Error, Server] = {
    try {
      val resourceFactory = new DedupResourceFactory(fileSystem, writeEnabled)
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
