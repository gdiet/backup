package net.diet_rich.dedup.core.meta.sql

import java.io.File

import net.diet_rich.dedup.core._
import net.diet_rich.dedup.core.meta._
import net.diet_rich.dedup.util.init
import net.diet_rich.dedup.util.io._

import scala.util.control.NonFatal

import net.diet_rich.dedup.core.data.Hash

object SQLMetaBackendManager {
  def create(metaRoot: File, repositoryid: String, hashAlgorithm: String) = {
    try { Hash digestLength hashAlgorithm } catch { case NonFatal(e) => require(false, s"Hash for $hashAlgorithm can't be computed: $e")}
    require(metaRoot mkdir(), s"Can't create meta directory $metaRoot")
    val settings = Map(
      metaVersionKey        -> metaVersionValue,
      repositoryidKey       -> repositoryid,
      metaHashAlgorithmKey  -> hashAlgorithm
    )
    writeSettingsFile(metaRoot / metaSettingsFile, settings)
    using(SessionFactory productionDB (metaRoot, readonly = false)) { sessionFactory =>
      DBUtilities.createTables(hashAlgorithm)(sessionFactory.session)
      DBUtilities.recreateIndexes(sessionFactory.session)
      new SQLMetaBackend(sessionFactory) replaceSettings settings
    }
  }

  def openInstance(metaRoot: File, readonly: Boolean): (MetaBackend, Ranges) = {
    val settings = readSettingsFile(metaRoot / metaSettingsFile)
    val sessionFactory = SessionFactory productionDB (metaRoot, readonly = false)
    val problemRanges = DBUtilities.problemDataAreaOverlaps(sessionFactory.session)
    val freeInData = if (problemRanges isEmpty) DBUtilities.freeRangesInDataArea(sessionFactory.session) else Nil
    val freeRanges = freeInData.toVector :+ DBUtilities.freeRangeAtEndOfDataArea(sessionFactory.session)
    if (problemRanges nonEmpty) warn (s"Found data area overlaps: $problemRanges")
    val metaBackend = new SQLMetaBackend(sessionFactory)
    val settingsFromDB = metaBackend.settings
    if (settings != settingsFromDB) {
      metaBackend close()
      require(requirement = false, s"The settings in the database ${metaBackend settings} did not match with the expected settings $settings")
    }
    (metaBackend, freeRanges)
  }
}
