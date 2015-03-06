package net.diet_rich.dedup.core

import java.io.{IOException, File}

import scala.concurrent.{Await, Future, ExecutionContext}
import scala.concurrent.duration.DurationInt

import net.diet_rich.dedup.util._


class BackupAlgorithm(repository: Repository, parallel: Option[Int] = None) extends AutoCloseable {
  import repository.metaBackend

  private val executor = ThreadExecutors.blockingThreadPoolExecutor(parallel getOrElse 4)
  private implicit val executionContext = ExecutionContext fromExecutorService executor

  // backup assumes that, once it starts, it has exclusive write access to its target branch in the tree
  def backup(source: File, target: String): Unit = {
    metaBackend.entries(target) match {
      case List(parent) =>
        if (metaBackend.children(parent.id, source.getName) isEmpty) Await.result(backup(source, parent.id), 1 day)
        else new IOException(s"File ${source.getName} is already present in repository directory $target")
      case Nil  => throw new IOException(s"Target $target not found in repository")
      case list => throw new IOException(s"Multiple entries found for target $target in repository: $list")
    }
  }

  protected def backup(source: File, parentid: Long): Future[Unit] =
    if (source.isDirectory) {
      val futureid = Future { metaBackend.createUnchecked(parentid, source.getName, Some(source lastModified())).id }
      val futures = futureid map { newParentid => source.listFiles() map (backup(_, newParentid)) }
      futures flatMap { Future sequence _.toList map (_ => ()) } // FIXME utility method noFailures???
    } else Future {
      repository.createUnchecked(parentid, source.getName, Some(Source from source), Some(source.lastModified()))
    }

  override def close() = {
    executor close()
  }
}
