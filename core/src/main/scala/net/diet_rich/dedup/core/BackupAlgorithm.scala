package net.diet_rich.dedup.core

import java.io.{IOException, File}

import scala.concurrent.Future

import net.diet_rich.dedup.core.meta._

// backup assumes that, once it starts, it has exclusive write access to its target branch in the tree
class BackupAlgorithm(repository: RepositoryReadWrite, val parallel: Option[Int] = None) extends ParallelExecution {
  import repository.metaBackend

  def backup(source: File, target: String, reference: Option[String]): Unit =
    reference.fold (backup(source, target)) (checkReferenceThenBackup(source, target, _))

  protected def checkReferenceThenBackup(source: File, target: String, reference: String): Unit = {
    val referenceEntry = metaBackend entries reference match {
      case List(entry) => entry
      case List() => throw new IOException(s"Reference $reference does not exist in repository")
      case list => throw new IOException(s"More than one entry for reference $reference in repository: $list")
    }
    def limitedTree(elementsToFetch: Int, entry: TreeEntry): List[(String, TreeEntry)] = if (elementsToFetch < 1) List((entry.name, entry)) else {
      val children = metaBackend.children(entry.id)
      val remaining = elementsToFetch - children.size
      List((entry.name, entry)) ::: children.flatMap { child =>
        limitedTree(remaining, child).map { case (path, childEntry) => (entry.name + separator + path, childEntry) }
      }
    }
    val referencesToCheck = limitedTree(20, referenceEntry)
    referencesToCheck foreach { case (path, entry) => println(s"$path -> ${entry.name}") }
  }

  protected def backup(source: File, target: String): Unit = {
    metaBackend.entries(target) match {
      case List(parent) =>
        if (metaBackend.children(parent.id, source.getName) isEmpty) awaitForever(backup(source, parent.id))
        else new IOException(s"File ${source.getName} is already present in repository directory $target")
      case Nil  =>
        awaitForever(backup(source, metaBackend createWithPath target))
      case list => throw new IOException(s"Multiple entries found for target $target in repository: $list")
    }
  }

  protected def backup(source: File, parentid: Long): Future[Unit] =
    if (source.isDirectory) {
      val futureid = Future { metaBackend.createUnchecked(parentid, source.getName, Some(source lastModified())) }
      futureid map { newParentid => source.listFiles().toList map (backup(_, newParentid)) } flatMap combine
    } else Future {
      repository.createUnchecked(parentid, source.getName, Some(Source from source), Some(source.lastModified()))
    }
}
