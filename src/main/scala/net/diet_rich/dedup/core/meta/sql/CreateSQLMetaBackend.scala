package net.diet_rich.dedup.core.meta.sql

import java.io.File

import net.diet_rich.dedup.core._
import net.diet_rich.dedup.core.meta._
import net.diet_rich.dedup.util.io._

import scala.util.control.NonFatal

import net.diet_rich.dedup.core.data.Hash

object CreateSQLMetaBackend {
  def apply(metaRoot: File, repositoryID: String, hashAlgorithm: String) = {
    try { Hash digestLength hashAlgorithm } catch { case NonFatal(e) => require(false, s"Hash for $hashAlgorithm can't be computed: $e")}
    require(metaRoot mkdir(), s"Can't create meta directory $metaRoot")
    val settings = Map(
      metaVersionKey        -> metaVersionValue,
      repositoryIDKey       -> repositoryID,
      metaHashAlgorithmKey  -> hashAlgorithm
    )
    writeSettingsFile(metaRoot / metaSettingsFile, settings)
    using(SQLSession.withH2(metaRoot, readonly = false)) { dbSessions =>
      implicit val session = dbSessions.session
      DBUtilities.createTables(hashAlgorithm)
      DBUtilities.recreateIndexes
      DBUtilities.replaceSettings(settings)
    }
  }
}
