// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import org.specs2.SpecificationWithJUnit
import net.diet_rich.dedup.core.values.{TreeEntry, Path}
import net.diet_rich.dedup.util.ThreadSpecific
import org.specs2.matcher.Matcher

class MetaFileSystemSpec extends SpecificationWithJUnit with ValueMatchers { def is = s2"""
    The root node should be a directory $rootIsDirectory
  """

  def rootIsDirectory = InMemoryDatabase.withDB { database =>
    val fileSystem = new MetaFileSystem { override val sqlTables = new SQLTables(database) }
    val root = fileSystem.treeEntry(Path(""))
    root should beSomeDirectory
  }

}
