package net.diet_rich.dedup.core

import java.io.{FileOutputStream, File}

import scala.concurrent.Future

import net.diet_rich.dedup.core.meta.TreeEntry
import net.diet_rich.dedup.util.io.{RichFile, using}

class RestoreAlgorithm(repository: RepositoryReadOnly, val parallel: Option[Int] = None) extends ParallelExecution {
  import repository.metaBackend

  def restore(source: String, target: File): Unit = {
    metaBackend.entries(source) match {
      case List(entry) => awaitForever(restore(target, entry))
      case Nil => require(requirement = false, s"Source $source not found in repository")
      case list => require(requirement = false, s"Multiple entries found in repository for source $source")
    }
  }

  protected def restore(target: File, entry: TreeEntry): Future[Unit] = {
    val file = target / entry.name
    (metaBackend children entry.id, entry.data) match {
      case (Nil, Some(dataid)) => Future {
        using(new FileOutputStream(file)) { out =>
          repository read dataid foreach { b => out.write(b.data, b.offset, b.length) }
        }
        entry.changed foreach file.setLastModified
      }
      case (children, data) =>
        if (data isDefined) warn(s"Data entry for directory $file is ignored")
        if (file.mkdir()) {
          entry.changed foreach file.setLastModified
          combine(children map (restore(file, _)))
        } else {
          warn(s"Could not create directory $file and its children during restore")
          Future.successful(())
        }
    }
  }
}
