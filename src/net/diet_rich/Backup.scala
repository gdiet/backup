package net.diet_rich

import java.io.File
import net.diet_rich.fdfs._

object Backup extends App {
  if (args.length < 3) throw new IllegalArgumentException("Backup needs at least source, repository and target arguments")
  if (args.length > 3) throw new IllegalArgumentException("Too many arguments")
  val source = new File(args(0))
  val repository = new File(args(1))
  val target = args(2)
  new Backup(source, repository, target)
}

class Backup(source: File, repository: File, target: String) {
  if (!target.startsWith("/")) throw new IllegalArgumentException("Target path must start with '/'.")
  if (!source.canRead) throw new IllegalArgumentException("Can't read source.")
  
  val connection = Repository.dbConnection(repository)
  val sqlExecutor = SqlDBCommon.executor(1, 100) // currently, 1 thread is best for h2 file db
  val treedb: TreeDBMethods =
    new TreeSqlDB()(connection)
    with TreeDBMethods
    with DeferredInsertTreeDB { val executor = sqlExecutor }
  val datadb: DataInfoDB = new DataInfoSqlDB()(connection)
  val storedb: ByteStoreDB = new ByteStoreSqlDB()(connection)

  // the last part of the target must not exist, the rest is created on demand
  val targetName = treedb nameFromPath target
  val targetParentId = treedb getOrMake (treedb parentPath target)
  if (treedb.childExists(targetParentId, targetName)) throw new IllegalArgumentException("Target already exists.")
  val targetId = treedb create (targetParentId, targetName)
  
  def processSource(source: File, parentId: Long): Unit = {
    if (source.isFile) {
      treedb create (parentId, source.getName, source.lastModified) // FIXME dataid
    } else {
      val id = treedb create (parentId, source.getName)
      source.listFiles.foreach(file => processSource(file, id))
    }
  }
  processSource(source, targetId)
  
  sqlExecutor.shutdownAndAwaitTermination
  
  throw new AssertionError("not yet fully implemented")
}