package net.diet_rich.dedupfs.metadata.sql

import java.io.File

import net.diet_rich.common._, io._, sql._

object SQLBackend extends DirWithConfig {
  override val objectName = "sql metadata store"
  override val version = "3.0"
  private val (dbDriverKey, dbUrlKey, dbUserKey, dbPasswordKey) = ("database driver", "database URL", "database user", "database password")
  private val hashAlgorithmKey = "hash algorithm"
  private val onDbShutdownKey = "on database shutdown"

  def initialize(directory: File, name: String, hashAlgorithm: String): Unit = {
    val (driver, user, password, onShutdown) = (H2.driver, H2.user, H2.password, H2.onShutdown)
    val url = s"jdbc:h2:${directory.getAbsolutePath}/dedupfs;DB_CLOSE_ON_EXIT=FALSE"
    initializeDirectory(directory, name, Map(
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
}
