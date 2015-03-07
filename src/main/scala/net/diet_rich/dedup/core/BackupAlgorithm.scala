package net.diet_rich.dedup.core

import java.io.{IOException, File}

import scala.concurrent.Future

class BackupAlgorithm(repository: Repository, val parallel: Option[Int] = None) extends ParallelExecution {
  import repository.metaBackend

  // backup assumes that, once it starts, it has exclusive write access to its target branch in the tree
  def backup(source: File, target: String): Unit = {
    metaBackend.entries(target) match {
      case List(parent) =>
        if (metaBackend.children(parent.id, source.getName) isEmpty) awaitForever(backup(source, parent.id))
        else new IOException(s"File ${source.getName} is already present in repository directory $target")
      case Nil  =>
        awaitForever(backup(source, metaBackend createWithPath target id))
      case list => throw new IOException(s"Multiple entries found for target $target in repository: $list")
    }
  }

  protected def backup(source: File, parentid: Long): Future[Unit] =
    if (source.isDirectory) {
      val futureid = Future { metaBackend.createUnchecked(parentid, source.getName, Some(source lastModified())).id }
      futureid map { newParentid => source.listFiles().toList map (backup(_, newParentid)) } flatMap combine
    } else Future {
      repository.createUnchecked(parentid, source.getName, Some(Source from source), Some(source.lastModified()))
    }
}
