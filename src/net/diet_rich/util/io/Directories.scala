// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util.io

import java.io.File

object Directories {

  def traverseDir(file:File) : Stream[File] =
    file #:: (file.listFiles match { case null => Stream() case x => x.toStream.flatMap(traverseDir _) } )

  def traverseDirs(files:Iterable[File]) : Stream[File] =
    files.toStream.flatMap(traverseDir _)

}