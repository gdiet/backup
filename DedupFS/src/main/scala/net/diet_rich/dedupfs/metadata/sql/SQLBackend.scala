package net.diet_rich.dedupfs.metadata.sql

import java.io.File
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
    using(ConnectionFactory(driver, url, user, password, Some(onShutdown))) { connections =>
      implicit val connection = connections()
      Database create hashAlgorithm
    }
    setStatus(directory, isClosed = true, isClean = true)
  }

  def read(directory: File, repositoryid: String): MetadataRead = {
    val conf = settingsChecked(directory, repositoryid)
    val connections = ConnectionFactory(conf(dbDriverKey), conf(readonlyUrlKey), conf(readonlyUserKey), conf(readonlyPasswordKey), None)
    new SQLBackendRead(connections(), conf(hashAlgorithmKey))
  }
  def readWrite(directory: File, repositoryid: String): (Metadata, Ranges) = {
    val conf = settingsChecked(directory, repositoryid)
    val connections = ConnectionFactory(conf(dbDriverKey), conf(dbUrlKey), conf(dbUserKey), conf(dbPasswordKey), conf get onDbShutdownKey)
    val freeRanges = Database.freeRanges(connections())
    (new SQLBackend(directory, connections, conf(hashAlgorithmKey)), freeRanges)
  }
}

private class SQLBackendRead(val connection: Connection, override final val hashAlgorithm: String) extends DatabaseRead {
  override final def close(): Unit = connection close()
}

private class SQLBackend(val directory: File, val connectionFactory: ConnectionFactory, val hashAlgorithm: String)
      extends { val connection = connectionFactory() }
      with DatabaseRead with DatabaseWrite {
  private val dirHelper = new DirWithConfig(SQLBackend, directory)
  dirHelper markOpen()
  override final def close(): Unit = {
    connectionFactory close()
    dirHelper markClosed()
  }
}
