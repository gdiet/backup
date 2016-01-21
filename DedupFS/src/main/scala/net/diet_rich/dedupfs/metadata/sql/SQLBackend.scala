package net.diet_rich.dedupfs.metadata.sql

import java.io.File

import net.diet_rich.common._, io._, sql._
import net.diet_rich.dedupfs.{hashAlgorithmKey, repositoryIdKey}
import net.diet_rich.dedupfs.metadata._

object SQLBackend extends DirWithConfigHelper {
  override val objectName = "sql metadata store"
  override val version = "3.0"
  private val (dbDriverKey, dbUrlKey, dbUserKey, dbPasswordKey) = ("database driver", "database URL", "database user", "database password")
  private val (readonlyUrlKey, readonlyUserKey, readonlyPasswordKey) = ("database URL read-only", "database user read-only", "database password read-only")
  private val onDbShutdownKey = "on database shutdown"

  private val directoryMarker = "$directory"
  private def urlForDir(url: String, directory: File) = url.replace(directoryMarker, directory.getAbsolutePath)

  def initialize(directory: File, name: String, hashAlgorithm: String): Unit = {
    val (driver, user, password, onShutdown) = (H2.driver, H2.user, H2.password, H2.onShutdown)
    val url = s"jdbc:h2:$directoryMarker/dedupfs;DB_CLOSE_ON_EXIT=FALSE"
    val settingsInFile = Map(
      dbDriverKey -> driver,
      dbUrlKey -> url, dbUserKey -> user, dbPasswordKey -> password,
      readonlyUrlKey -> s"$url;ACCESS_MODE_DATA=r", readonlyUserKey -> user, readonlyPasswordKey -> password,
      onDbShutdownKey -> onShutdown
    )
    initialize(directory, name, settingsInFile)
    setStatus(directory, isClosed = false, isClean = true)
    val settingsInDB = Map(
      repositoryIdKey -> name,
      hashAlgorithmKey -> hashAlgorithm
    )
    using(ConnectionFactory(driver, urlForDir(url, directory), user, password, Some(onShutdown))) {
      Database.create(hashAlgorithm, settingsInDB)(_)
    }
    setStatus(directory, isClosed = true, isClean = true)
  }

  def read(directory: File, repositoryid: String): MetadataRead = {
    val conf = settingsChecked(directory, repositoryid)
    val connections = ConnectionFactory(conf(dbDriverKey), urlForDir(conf(readonlyUrlKey), directory), conf(readonlyUserKey), conf(readonlyPasswordKey), None)
    new SQLBackendRead(connections, repositoryid)
  }
  def readWrite(directory: File, repositoryid: String): (Metadata, Ranges) = {
    val conf = settingsChecked(directory, repositoryid)
    val connections = ConnectionFactory(conf(dbDriverKey), urlForDir(conf(dbUrlKey), directory), conf(dbUserKey), conf(dbPasswordKey), conf get onDbShutdownKey)
    val freeRanges = Database.freeRanges(connections)
    (new SQLBackend(directory, connections, repositoryid), freeRanges)
  }
}

private class SQLBackendRead(protected val connectionFactory: ConnectionFactory, protected val repositoryId: String) extends DatabaseRead {
  override final def close(): Unit = connectionFactory close()
}

private class SQLBackend(protected val directory: File, protected val connectionFactory: ConnectionFactory, protected val repositoryId: String)
      extends DatabaseRead with DatabaseWrite {
  private val dirHelper = new DirWithConfig(SQLBackend, directory)
  dirHelper markOpen()
  override final def close(): Unit = {
    connectionFactory close()
    dirHelper markClosed()
  }
}
