// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.webdav

trait FileSystem {
  sys.addShutdownHook{
    System.err.println("shutting down dedup file system")
    Thread.sleep(1000)
  }

}

object FileSystem {

  def apply(repositoryPath: String, writeEnabled: Boolean, deflate: Boolean): Either[Error, FileSystem] = Right(new FileSystem{})
  
}
