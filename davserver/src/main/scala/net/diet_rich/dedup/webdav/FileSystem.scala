// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.webdav

trait FileSystem {

}

object FileSystem {

  def apply(repositoryPath: String, storeMethod: Int): Either[Error, FileSystem] = Right(new FileSystem{})
  
}
