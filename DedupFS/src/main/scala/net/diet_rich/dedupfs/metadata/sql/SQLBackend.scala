package net.diet_rich.dedupfs.metadata.sql

import java.io.{IOException, File}
import java.sql.Connection

import net.diet_rich.common._, io._, sql._
import net.diet_rich.dedupfs.metadata._

object SQLBackend extends DirWithConfigHelper {
  override val objectName = "sql metadata store"
  override val version = "3.0"
  private val (dbDriverKey, dbUrlKey, dbUserKey, dbPasswordKey) = ("database driver", "database URL", "database user", "database password")
  private val (readonlyUrlKey, readonlyUserKey, readonlyPasswordKey) = ("database URL read-only", "database user read-only", "database password read-only")
  private val hashAlgorithmKey = "hash algorithm"
  private val onDbShutdownKey = "on database shutdown"

  def initialize(directory: File, name: String, hashAlgorithm: String): Unit = {
    val (driver, user, password, onShutdown) = (H2.driver, H2.user, H2.password, H2.onShutdown)
    val url = s"jdbc:h2:${directory.getAbsolutePath}/dedupfs;DB_CLOSE_ON_EXIT=FALSE"
    val options =  Map(
      dbDriverKey -> driver,
      dbUrlKey -> url, dbUserKey -> user, dbPasswordKey -> password,
      readonlyUrlKey -> s"$url;ACCESS_MODE_DATA=r", readonlyUserKey -> user, readonlyPasswordKey -> password,
      onDbShutdownKey -> onShutdown,
      hashAlgorithmKey -> hashAlgorithm
    )
    initialize(directory, name, options)
    setStatus(directory, isClosed = false, isClean = true)
    using(connectionFactory(driver, url, user, password, Some(onShutdown))) {
      cf => Database.create(hashAlgorithm)(cf())
    }
    setStatus(directory, isClosed = true, isClean = true)
  }

  def read(directory: File, repositoryid: String): MetadataRead = {
    val conf = settingsChecked(directory, repositoryid)
    val connections = connectionFactory(conf(dbDriverKey), conf(readonlyUrlKey), conf(readonlyUserKey), conf(readonlyPasswordKey), None)
    new SQLBackendRead(connections(), conf(hashAlgorithmKey))
  }
  def readWrite(directory: File, repositoryid: String): (Metadata, Ranges) = {
    val conf = settingsChecked(directory, repositoryid)
    val connections = connectionFactory(conf(dbDriverKey), conf(dbUrlKey), conf(dbUserKey), conf(dbPasswordKey), conf get onDbShutdownKey)
    val freeRanges = Database.freeRanges(connections())
    (new SQLBackend(directory, connections, conf(hashAlgorithmKey)), freeRanges)
  }
}

private class SQLBackendRead(val connection: Connection, override final val hashAlgorithm: String) extends MetadataRead with TreeDatabaseRead {
  override final def entry(key: Long): Option[TreeEntry] = treeEntryFor(key)
  override final def dataEntry(dataid: Long): Option[DataEntry] = ???
  override final def children(parent: Long): Seq[TreeEntry] =
    allChildren(parent).groupBy(_.name).map{ case (_, entries) => entries.head }.toSeq
  override final def storeEntries(dataid: Long): Ranges = ???
  override final def entry(path: Array[String]): Option[TreeEntry] =
    path.foldLeft(Option(TreeEntry.root)) { (nodes, name) => nodes flatMap (node => child(node.key, name)) }
  override final def sizeOf(dataid: Long): Option[Long] = ???
  override final def allChildren(parent: Long): Seq[TreeEntry] = treeChildrenOf(parent).toSeq
  override final def allChildren(parent: Long, name: String): Seq[TreeEntry] = ???
  override final def child(parent: Long, name: String): Option[TreeEntry] = treeChildrenOf(parent) find (_.name == name)
  override final def allEntries(path: Array[String]): Seq[TreeEntry] = ???
  override final def dataEntriesFor(size: Long, print: Long, hash: Array[Byte]): Seq[DataEntry] = ???
  override final def settings: Map[String, String] = ???
  override final def dataEntryExists(print: Long): Boolean = ???
  override final def dataEntryExists(size: Long, print: Long): Boolean = ???
  override final def path(key: Long): Option[String] =
    if (key == TreeEntry.root.key) Some(TreeEntry.rootPath)
    else entry(key) flatMap {entry => path(entry.parent) map (_ + "/" + entry.name)}

  override def close(): Unit = connection close()
}

private class SQLBackend(val directory: File, val connectionFactory: ConnectionFactory, hashAlgorithm: String)
              extends SQLBackendRead(connectionFactory(), hashAlgorithm) with Metadata with TreeDatabaseWrite {
  private val dirHelper = new DirWithConfig(SQLBackend, directory)
  dirHelper markOpen()
  override def createDataEntry(reservedid: Long, size: Long, print: Long, hash: Array[Byte], storeMethod: Int): Unit = ???
  override def createUnchecked(parent: Long, name: String, changed: Option[Long], dataid: Option[Long]): Long = inTransaction {
    if (parent == TreeEntry.root.parent) throw new IOException("Cannot create a sibling of the root entry")
    treeInsert(parent, name, changed, dataid)
  }
  override def replaceSettings(newSettings: Map[String, String]): Unit = ???
  override def createByteStoreEntry(dataid: Long, start: Long, fin: Long): Unit = ???
  override def delete(key: Long): Boolean = inTransaction { entry(key).map(delete).isDefined }
  override def delete(entry: TreeEntry): Unit = inTransaction { treeDelete(entry) }
  override def change(changed: TreeEntry): Boolean = ???
  override def create(parent: Long, name: String, changed: Option[Long], dataid: Option[Long]): Long = inTransaction {
    children(parent) find (_.name == name) match {
      case Some(entry) => throw new IOException(s"entry $entry already exists")
      case None => createUnchecked(parent, name, changed, dataid)
    }
  }
  override def createOrReplace(parent: Long, name: String, changed: Option[Long], dataid: Option[Long]): Long = ???
  override def nextDataid: Long = ???
  override def createWithPath(path: String, changed: Option[Long], dataid: Option[Long]): Long = ???
  // Note: Writing the tree structure is synchronized, so "create only if not exists" can be implemented.
  override def inTransaction[T](f: => T): T = synchronized(f)
  override def close(): Unit = {
    connectionFactory close()
    dirHelper markClosed()
  }
}
