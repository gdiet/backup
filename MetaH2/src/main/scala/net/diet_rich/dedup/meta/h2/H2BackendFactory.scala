package net.diet_rich.dedup.meta.h2

import net.diet_rich.common.io.{RichFile, using, writeSettingsFile}
import net.diet_rich.common.sql.ConnectionFactory
import net.diet_rich.dedup.Settings
import net.diet_rich.dedup.meta.MetaBackendFactory

class H2BackendFactory extends MetaBackendFactory {
  override def initialize(directory: java.io.File, repositoryId: String, hashAlgorithm: String): Unit = {
    val databaseSettings = Map(
      Settings.repositoryIdKey -> repositoryId,
      Settings.hashAlgorithmKey -> hashAlgorithm,
      Database.databaseVersionSetting,
      Database.metaBackendClassSetting
    )
    using(ConnectionFactory(H2.driver, H2.url(directory), H2.user, H2.password, H2.onShutdown)) { implicit _cf =>
      Database.create(hashAlgorithm, databaseSettings)
    }
    writeSettingsFile(directory / Settings.settingsFileName, databaseSettings)
  }
  def metaBackend(directory: java.io.File, repositoryId: String): H2MetaBackend =
    new H2MetaBackend(directory: java.io.File, repositoryId: String)
}
