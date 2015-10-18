package net.diet_rich.common

import java.io.File

import net.diet_rich.common.io._

trait DirWithConfigHelper {
  private[common] val objectName: String
  private[common] val version: String
  private[common] val (configFileName, statusFileName) = ("config.txt", "status.txt")
  private[common] val (nameKey, versionKey) = ("name", "version")
  private[common] val (isClosedKey, isCleanKey) = ("isClosed", "isClean")

  protected def initialize(directory: File, name: String, additionalSettings: StringMap): Unit = {
    require(!directory.exists(), s"$objectName directory already exists: $directory")
    require(directory mkdir(), s"can't create $objectName directory $directory")
    writeSettingsFile(directory / configFileName, Map(
      versionKey -> version,
      nameKey -> name
    ) ++ additionalSettings)
    setStatus(directory, isClosed = true, isClean = true)
  }

  protected[common] def setStatus(directory: File, isClosed: Boolean, isClean: Boolean): Unit =
    writeSettingsFile(directory / statusFileName, Map(isClosedKey -> s"$isClosed", isCleanKey -> s"$isClean"))

  protected def settingsChecked(directory: File, name: String): StringMap =
    init(readSettingsFile(directory / configFileName)) { settings =>
      require(settings(versionKey) == version, s"Version mismatch in $objectName: Actual ${settings(versionKey)}, required $version")
      require(settings(nameKey) == name, s"Name mismatch in $objectName: Actual ${settings(nameKey)}, expected $name")
    }

  def forceClose(directory: File, name: String): Unit = {
    settingsChecked(directory, name)
    val status = readSettingsFile(directory / statusFileName)
    require(!status(isClosedKey).toBoolean, s"Status file $statusFileName signals that $objectName is already closed")
    setStatus(directory, isClosed = true, isClean = false)
  }
}

trait DirWithConfig {
  protected val baseObject: DirWithConfigHelper; import baseObject._
  protected def directory: File

  protected val (initiallyClosed, initiallyClean) = {
    val status = readSettingsFile(directory / statusFileName)
    (status(isClosedKey).toBoolean, status(isCleanKey).toBoolean)
  }

  protected def markOpen() = {
    require(initiallyClosed, s"Status file $statusFileName signals the $objectName is already open")
    setStatus(directory, isClosed = false, isClean = initiallyClean)
  }
  protected def markClosed(isClean: Boolean = initiallyClean) = setStatus(directory, isClosed = true, isClean = initiallyClean)
}
