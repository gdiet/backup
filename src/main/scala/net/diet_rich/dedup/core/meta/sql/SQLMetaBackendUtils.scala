package net.diet_rich.dedup.core.meta.sql

import java.io.File

import net.diet_rich.dedup.core._
import net.diet_rich.dedup.core.meta._
import net.diet_rich.dedup.util.io._

import scala.util.control.NonFatal

import net.diet_rich.dedup.core.data.Hash

object SQLMetaBackendUtils {
  def create(metaRoot: File, repositoryid: String, hashAlgorithm: String) = {
    try { Hash digestLength hashAlgorithm } catch { case NonFatal(e) => require(false, s"Hash for $hashAlgorithm can't be computed: $e")}
    require(metaRoot mkdir(), s"Can't create meta directory $metaRoot")
    val settings = Map(
      metaVersionKey        -> metaVersionValue,
      repositoryidKey       -> repositoryid,
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

  def use[T](metaRoot: File, readonly: Boolean)(withBody: (SQLMetaBackend, String Map String) => T): T =
    using(SQLSession.withH2(metaRoot, readonly)) { dbSessions =>
      withBody(new SQLMetaBackend(dbSessions), DBUtilities.allSettings(dbSessions.session))
    }
}
