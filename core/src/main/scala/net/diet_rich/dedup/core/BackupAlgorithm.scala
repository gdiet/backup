package net.diet_rich.dedup.core

import java.io.{IOException, File}

import scala.concurrent.Future

import net.diet_rich.dedup.core.data.Print
import net.diet_rich.dedup.core.meta._
import net.diet_rich.dedup.util.resultOf
import net.diet_rich.dedup.util.io._

// backup assumes that, once it starts, it has exclusive write access to its target branch in the tree
class BackupAlgorithm(repository: RepositoryReadWrite, checkPrintOption: Option[Boolean], val parallel: Option[Int]) extends ParallelExecution {
  import repository.metaBackend

  // TODO tests for backup algorithm

  val checkPrint = checkPrintOption getOrElse false

  def backup(source: File, target: String, reference: Option[String]): Unit =
    resultOf(backup(source, targetid(source, target), referenceEntry(source, target, reference)))

  protected def targetid(source: File, target: String): Long = {
    metaBackend.entries(target) match {
      case List(entry) if entry.data isEmpty =>
        if (metaBackend.children(entry.id, source.getName) isEmpty) entry.id
        else throw new IOException(s"File ${source.getName} is already present in target directory $target")
      case List(entry) => throw new IOException(s"Target $target is a file, not a directory")
      case Nil  => metaBackend createWithPath target
      case list => throw new IOException(s"Multiple entries found for target $target in repository: $list")
    }
  }

  protected def referenceEntry(source: File, target: String, reference: Option[String]): Option[TreeEntry] = reference map {
    metaBackend entries _ match {
      case List(entry) if entry.data isDefined =>
        if (source.isFile) entry
        else throw new IOException(s"Reference $reference is a file, but source $source is not")
      case List(entry) => checkChildReferences(source, entry)
      case List() => throw new IOException(s"Reference $reference does not exist in repository")
      case list => throw new IOException(s"More than one entry for reference $reference in repository: $list")
    }
  }

  protected def checkChildReferences(source: File, reference: TreeEntry): TreeEntry = {
    val referencesToCheck = limitedTree(100, reference)
    val misses = referencesToCheck filterNot { case (path, entry) =>
      if (entry.data isDefined) source / path isFile else source / path isDirectory
    }
    if (misses.size * 2 > referencesToCheck.size) {
      println(s"Of the ${referencesToCheck.size} entries checked in reference $reference,")
      println(s"${misses.size} could not be found in the source $source.")
      println(s"If a incorrect reference is given, backup performance may degrade.")
      val answer = Option(scala.io.StdIn.readLine(s"Continue backup anyway? (y/n) "))
      require(answer == Some("y"), "Aborted by user request")
    }
    reference
  }

  protected def limitedTree(elementsToFetch: Int, entry: TreeEntry): List[(String, TreeEntry)] = if (elementsToFetch < 1) Nil else {
    val children = metaBackend children entry.id
    val remaining = elementsToFetch - children.size
    children flatMap { child =>
      List((child.name, child)) ::: limitedTree(remaining, child).map { case (path, grandChild) => (child.name + separator + path, grandChild) }
    }
  }

  protected def backup(source: File, targetid: Long, reference: Option[TreeEntry]): Future[Unit] =
    if (source.isDirectory) combine {
      val newParentid = repository createUnchecked (targetid, source.getName, source lastModified())
      source.listFiles().toList.map { child =>
        val childReference = reference map (_.id) flatMap (metaBackend.childWarn(_, source.getName))
        backup(child, newParentid, childReference)
      }
    } else Future(reference.fold (backupFile(source, targetid)) (backupFile(source, targetid, _)))

  protected def backupFile(source: File, targetid: Long): Unit =
    repository createUnchecked (targetid, source getName, source lastModified(), Some(Source from source))

  protected def backupFile(source: File, targetid: Long, reference: TreeEntry): Unit =
    reference.data match {
      case Some(dataid) if reference.changed == Some(source.lastModified) =>
        metaBackend.dataEntry(dataid).fold (backupFile(source, targetid)) (backupFile(source, targetid, _))
      case _ => backupFile(source, targetid)
    }

  protected def backupFile(source: File, targetid: Long, reference: DataEntry): Unit =
    if (reference.size == source.length()) {
      if (checkPrint) using(Source from source) { byteSource =>
        val printData = byteSource read Repository.PRINTSIZE
        val print = Print(printData)
        if (reference.print != print) {
          val dataid = repository.storeLogic dataidFor (printData, print, byteSource)
          metaBackend createUnchecked(targetid, source.getName, Some(source.lastModified()), Some(dataid))
        } else { }
      } else { }
    } else backupFile(source, targetid)
}
