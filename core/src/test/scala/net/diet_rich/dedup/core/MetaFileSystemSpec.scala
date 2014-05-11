// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import org.specs2.SpecificationWithJUnit
import net.diet_rich.dedup.core.values.Path
import net.diet_rich.dedup.util.ThreadSpecific

class MetaFileSystemSpec extends SpecificationWithJUnit { def is = s2"""
    The root node should be a directory $rootIsDirectory
  """

  def rootIsDirectory = InMemoryDatabase.withDB { database =>
    val tables = new SQLTables { override lazy val sessions = ThreadSpecific(database createSession) } // FIXME lazy should not be necessary
    val fileSystem = new MetaFileSystem { override val sqlTables = tables }
    println("********** read root entry")
    val root = fileSystem.treeEntry(Path(""))
    root.get.isDirectory === true // FIXME matcher
  }

}
