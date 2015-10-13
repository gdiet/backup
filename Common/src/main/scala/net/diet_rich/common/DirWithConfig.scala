package net.diet_rich.common

import java.io.File

import net.diet_rich.common.io._

trait DirWithConfig {
  protected val objectName: String
  protected val version: String
  protected val (configFileName, statusFileName) = ("config.txt", "status.txt")
  protected val (nameKey, versionKey) = ("name", "version")
  protected val (isClosedKey, isCleanKey) = ("isClosed", "isClean")

  protected def initializeDirectory(directory: File, name: String, additionalSettings: StringMap): Unit = {
    require(!directory.exists(), s"$objectName directory already exists: $directory")
    require(directory mkdir(), s"can't create $objectName directory $directory")
    writeSettingsFile(directory / configFileName, Map(
      versionKey -> version,
      nameKey -> name
    ) ++ additionalSettings)
    setStatus(directory, isClosed = true, isClean = true)
  }

  protected def setStatus(directory: File, isClosed: Boolean, isClean: Boolean): Unit =
    writeSettingsFile(directory / statusFileName, Map(isClosedKey -> s"$isClosed", isCleanKey -> s"$isClean"))

  protected def settingsChecked(dataDirectory: File, name: String): StringMap =
    init(readSettingsFile(dataDirectory / configFileName)) { settings =>
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
