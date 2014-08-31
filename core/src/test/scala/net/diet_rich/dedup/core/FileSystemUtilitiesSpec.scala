// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import org.specs2.SpecificationWithJUnit

import net.diet_rich.dedup.core.values.{Path, StoreMethod}
import net.diet_rich.dedup.util.init
import net.diet_rich.dedup.testutil.newTestDir

class FileSystemUtilitiesSpec extends SpecificationWithJUnit { def is = s2"""$sequential
${"Tests for the file system utilities".title}

The path of the root node should be '' ${asIn pathOfRootNode}
The path of a node should be determined correctly ${asIn pathOfNode}

Cleanup after tests ${asIn cleanup}
  """

  lazy val asIn = new AsIn

  class AsIn {
    val examplePath = Path("/some/path/for/testing")
    val repository = init(newTestDir("FileSystemUtilitiesSpec")){Repository create _}
    val fileSystem = init(Repository.fileSystem(repository, storeMethod = StoreMethod.STORE)){ fileSystem =>
      fileSystem setup()
      fileSystem createWithPath examplePath
    }
    def pathOfRootNode = fileSystem.path(FileSystem.ROOTID) should beSome(Path.ROOTPATH)
    def pathOfNode = {
      val nodeId = fileSystem.entries(examplePath).head.id
      fileSystem.path(nodeId) should beSome(examplePath)
    }
    def cleanup = { fileSystem teardown(); success }
  }
}
