package net.diet_rich.dedup.core

import java.io.{IOException, File}

import net.diet_rich.dedup.util.ThreadExecutor

class BackupAlgorithm(repository: Repository, parallel: Option[Int] = None) extends AutoCloseable {
  private val executor = new ThreadExecutor(parallel getOrElse 4)
  import repository.metaBackend

  // backup assumes that, once it starts, it has exclusive write access to its target branch in the tree
  def backup(source: File, target: String): Unit = {
    metaBackend.entries(target) match {
      case List(parent) =>
        if (metaBackend.children(parent.id, source.getName) isEmpty) backup(source, parent.id)
        else throw new IOException(s"File ${source.getName} is already present in repository directory $target")
      case Nil  => throw new IOException(s"Target $target not found in repository")
      case list => throw new IOException(s"Multiple entries found for target $target in repository: $list")
    }
  }

  protected def backup(source: File, parentid: Long): Unit = executor {
    if (source.isDirectory) {
      val newParentid = metaBackend.createUnchecked(parentid, source.getName, Some(source lastModified())).id
      source.listFiles() foreach (backup(_, newParentid))
    } else repository.createUnchecked(parentid, source.getName, Some(Source from source), Some(source.lastModified()))
  }

  override def close() = executor close()
}
