package net.diet_rich.dedup.ftpserver

import org.apache.ftpserver.FtpServerFactory
import org.apache.ftpserver.ftplet.{User, FileSystemFactory}
import org.apache.ftpserver.listener.ListenerFactory
import org.apache.ftpserver.usermanager.PropertiesUserManagerFactory
import org.apache.ftpserver.usermanager.impl.BaseUser

import net.diet_rich.dedup.core.Repository
import net.diet_rich.dedup.core.data.StoreMethod
import net.diet_rich.dedup.util._

object Main extends App with Logging {
  CommandLineUtils.forArgs(args) { argDetails => import argDetails._
    require(command == "ftpserver")

    val ftpPort = port getOrElse 21
    val (repository, fileSystemViewFactory) =
      if (writable) {
        checkOptionUse(storeMethod, parallel, versionComment, maxBytesToCache)
        val repository = Repository openReadWrite (repositoryDir, storeMethod, parallel, versionComment)
        (repository, () => FileSysViewReadWrite(repository, maxBytesToCache getOrElse 250000000))
      }
      else {
        checkOptionUse()
        val repository = Repository openReadOnly repositoryDir
        (repository, () => FileSysViewReadOnly(repository))
      }

    val listener = init(new ListenerFactory()) { _ setPort ftpPort } createListener()
    val fileSystemFactory = new FileSystemFactory {
      override def createFileSystemView (user: User) = fileSystemViewFactory()
    }
    val userManager = init(new PropertiesUserManagerFactory() createUserManager()) {
      _ save init(new BaseUser()){user => user setName "user"; user setPassword "user"}
    }
    val server = init(new FtpServerFactory()) { serverFactory =>
      serverFactory addListener ("default", listener)
      serverFactory setFileSystem fileSystemFactory
      serverFactory setUserManager userManager
    } createServer()
    server.start()

    log info s"Started dedup ftp server at ftp://localhost${if (ftpPort == 21) "" else s":$ftpPort"}"
    log info s"Write access is ${if (writable) "ENABLED" else "OFF"}"
    log info s"User: 'user', password: 'user'"
    sys addShutdownHook {
      log info s"Dedup ftp server stopping..."
      server stop()
      repository close()
    }
    Thread sleep Long.MaxValue
  }
}
