// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import org.specs2.SpecificationWithJUnit
import net.diet_rich.dedup.core.values.Path

class MetaFileSystemSpec extends SpecificationWithJUnit { def is = s2"""
    The root node should be a directory $rootIsDirectory
  """

  def fileSystemFactory = new MetaFileSystem {
    override val sqlTables = new MemoryDbWithTables with SQLTables
  }

  def rootIsDirectory = {
    val fileSystem = fileSystemFactory
    val root = fileSystem.treeEntry(Path(""))
    root.get.isDirectory === true // FIXME matcher
  }

}
