package net.diet_rich.dedupfs.metadata.sql

import java.io.File
import java.sql.Connection

import net.diet_rich.common._, io._, sql._
import net.diet_rich.dedupfs.metadata._

object SQLBackend extends DirWithConfigHelper {
  override val objectName = "sql metadata store"
  override val version = "3.0"
  private val (dbDriverKey, dbUrlKey, dbUserKey, dbPasswordKey) = ("database driver", "database URL", "database user", "database password")
  private val hashAlgorithmKey = "hash algorithm"
  private val onDbShutdownKey = "on database shutdown"

  def initialize(directory: File, name: String, hashAlgorithm: String): Unit = {
    val (driver, user, password, onShutdown) = (H2.driver, H2.user, H2.password, H2.onShutdown)
    val url = s"jdbc:h2:${directory.getAbsolutePath}/dedupfs;DB_CLOSE_ON_EXIT=FALSE"
    initialize(directory, name, Map(
      dbDriverKey -> driver,
      dbUrlKey -> url,
      dbUserKey -> user,
      dbPasswordKey -> password,
      onDbShutdownKey -> onShutdown,
      hashAlgorithmKey -> hashAlgorithm
    ))
    setStatus(directory, isClosed = false, isClean = true)
    using(connectionFactory(driver, url, user, password, Some(onShutdown))) {
      cf => Database.create(hashAlgorithm)(cf())
    }
    setStatus(directory, isClosed = true, isClean = true)
  }

  def read(directory: File, repositoryid: String): MetadataRead = {
    val conf = settingsChecked(directory, repositoryid)
    val connections = connectionFactory(conf(dbDriverKey), conf(dbUrlKey), conf(dbUserKey), conf(dbPasswordKey), None)
    new SQLBackendRead(connections())
  }
  def readWrite(directory: File, repositoryid: String): Metadata = ???
}

private class SQLBackendRead(val connection: Connection) extends MetadataRead with TreeDatabaseRead {
  override final def entry(key: Long): Option[TreeEntry] = ???
  override final def dataEntry(dataid: Long): Option[DataEntry] = ???
  override final def children(parent: Long): Seq[TreeEntry] = ???
  override final def storeEntries(dataid: Long): Ranges = ???
  override final def entry(path: Array[String]): Option[TreeEntry] = ???
  override final def sizeOf(dataid: Long): Option[Long] = ???
  override final def allChildren(parent: Long): Seq[TreeEntry] = ???
  override final def allChildren(parent: Long, name: String): Seq[TreeEntry] = ???
  override final def child(parent: Long, name: String): Seq[TreeEntry] = ???
  override final def allEntries(path: Array[String]): Seq[TreeEntry] = ???
  override final def dataEntriesFor(size: Long, print: Long, hash: Array[Byte]): Seq[DataEntry] = ???
  override final def settings: Map[String, String] = ???
  override final def dataEntryExists(print: Long): Boolean = ???
  override final def dataEntryExists(size: Long, print: Long): Boolean = ???
  override final def path(id: Long): Option[String] = ???
  override final def close(): Unit = connection.close()
}

private class SQLBackend(val directory: File, val connectionFactory: ConnectionFactory) extends SQLBackendRead(connectionFactory()) with Metadata {
  override def createDataEntry(reservedid: Long, size: Long, print: Long, hash: Array[Byte], storeMethod: Int): Unit = ???
  override def createUnchecked(parent: Long, name: String, changed: Option[Long], dataid: Option[Long]): Long = ???
  override def replaceSettings(newSettings: Map[String, String]): Unit = ???
  override def createByteStoreEntry(dataid: Long, start: Long, fin: Long): Unit = ???
  override def delete(key: Long): Boolean = ???
  override def delete(entry: TreeEntry): Boolean = ???
  override def change(changed: TreeEntry): Boolean = ???
  override def create(parent: Long, name: String, changed: Option[Long], dataid: Option[Long]): Long = ???
  override def createOrReplace(parent: Long, name: String, changed: Option[Long], dataid: Option[Long]): Long = ???
  override def nextDataid: Long = ???
  override def inTransaction[T](f: => T): T = ???
  override def createWithPath(path: String, changed: Option[Long], dataid: Option[Long]): Long = ???
}