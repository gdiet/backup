// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.ftpserver

import org.apache.ftpserver.{FtpServerFactory, FtpServer}
import org.apache.ftpserver.ftplet.{User, FileSystemFactory}
import org.apache.ftpserver.listener.ListenerFactory
import org.apache.ftpserver.usermanager.PropertiesUserManagerFactory
import org.apache.ftpserver.usermanager.impl.BaseUser

import net.diet_rich.dedup.core.{backupDatabase, Repository}
import net.diet_rich.dedup.core.values.StoreMethod
import net.diet_rich.dedup.util.{init, ConsoleApp}

object FtpServerApp extends ConsoleApp {
  checkUsage("parameters: <repository path> [READWRITE] [DEFLATE] [port:(21)]")
  val writeEnabled = options contains "READWRITE"
  if (writeEnabled) backupDatabase(repositoryDirectory)
  val storeMethod = if (options contains "DEFLATE") StoreMethod.DEFLATE else StoreMethod.STORE
  val ftpPort = option("port:", "21") toInt
  val filesystem = init(Repository.fileSystem(repositoryDirectory, storeMethod, readonly = !writeEnabled))(_ setup())

  val listener = init(new ListenerFactory()){_ setPort ftpPort} createListener()
  val fileSystemFactory = new FileSystemFactory { def createFileSystemView (user: User) = new FileSysView(filesystem, writeEnabled) }
  val userManager = init(new PropertiesUserManagerFactory() createUserManager()) {
    _ save init(new BaseUser()){user => user setName "user"; user setPassword "user"}
  }

  val server = init(new FtpServerFactory()) { serverFactory =>
    serverFactory addListener ("default", listener)
    serverFactory setFileSystem fileSystemFactory
    serverFactory setUserManager userManager
  } createServer()

  server.start()
  println(s"started ftp server at ftp://localhost${if (ftpPort == 21) "" else s":$ftpPort"}")
  println("User: 'user', password: 'user'")

  val shutdownHook = sys.ShutdownHookThread {
    server.stop()
    filesystem.teardown()
  }
}
