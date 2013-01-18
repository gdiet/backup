// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.ftpserver

import net.diet_rich.dedup.repository.Repository
import org.apache.ftpserver.FtpServerFactory
import org.apache.ftpserver.usermanager.impl.BaseUser
import org.apache.ftpserver.usermanager.PropertiesUserManagerFactory
import org.apache.ftpserver.ftplet._

object MinaWrapper {

  val userManager = {
    val um = new PropertiesUserManagerFactory().createUserManager()
    val user = new BaseUser()
    user.setName("user")
    user.setPassword("user")
    um.save(user)
    um
  }

  def fileSystemFactory(repository: Repository) =
    new FileSystemFactory {
      def createFileSystemView (user: User) = new FileSysView(repository)
    }
  
  def server(repository: Repository) = {
    val serverFactory = new FtpServerFactory()
    serverFactory.setUserManager(userManager)
    serverFactory.setFileSystem(fileSystemFactory(repository))
    serverFactory.createServer()
  }
  
}