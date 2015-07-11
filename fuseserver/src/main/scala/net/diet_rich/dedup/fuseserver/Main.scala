package net.diet_rich.dedup.fuseserver

import fusefs.{Dir, File, Entry, FileSystem}
import net.diet_rich.dedup.core.Repository
import net.diet_rich.dedup.core.meta.TreeEntry
import net.diet_rich.dedup.util.{CommandLineUtils, Logging, writableAsBoolean}
import net.diet_rich.dedup.util.io.using

object Main extends App with Logging {
  CommandLineUtils.forArgs(args) { argDetails => import argDetails._
    require(command == "fuse")
    val repository =
      if (writable) {
        checkOptionUse(storeMethod, parallel, versionComment, target)
        Repository openReadWrite (repositoryDir, storeMethod, parallel, versionComment)
      }
      else {
        checkOptionUse(target)
        Repository openReadOnly repositoryDir
      }

    using(repository) { _ => // close repository on exceptions
      val fileSystem = new FileSystem {
        override def entryFor(path: String): Option[Entry] = {
          repository.metaBackend.entries(path).headOption.map {
            case TreeEntry(id, _, name, _, None, _) =>
              Dir(id, name)
            case TreeEntry(id, _, name, _, Some(dataid), _) =>
              File(id, name, repository.metaBackend.dataEntry(dataid).map(_.size).getOrElse(0))
          }
        }
        override def childrenOf(parent: Long): Seq[Entry] = {
          repository.metaBackend.children(parent).map {
            case TreeEntry(id, _, name, _, None, _) =>
              Dir(id, name)
            case TreeEntry(id, _, name, _, Some(dataid), _) =>
              File(id, name, repository.metaBackend.dataEntry(dataid).map(_.size).getOrElse(0))
          }
        }
        override def readFile(id: Long, offset: Long, size: Int): Array[Byte] = ???
      }
      FileSystem.mount(fileSystem, target)
    }
  }
}
