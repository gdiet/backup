// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.backup

import net.diet_rich.dedup.vals._
import net.diet_rich.util.io.using
import net.diet_rich.util.vals.Size

sealed trait StoreStrategy {
  def execute(parent: TreeEntryID)(implicit sys: System): Unit
}

object StoreStrategy {
  def select(source: Source, reference: Reference)(implicit sys: System.ForStoreStrategySelect): StoreStrategy =
    if (source.time == reference.time && source.size == reference.size)
      if (sys checkReferencePrints) {
        source print { printedSource =>
          if (printedSource.print == reference.print) {
            printedSource.close
            LinkToReference(source, reference)
          } else select(printedSource)
        }
      } else LinkToReference(source, reference)
    else source print select

  def select(source: Source)(implicit sys: System.ForStoreStrategySelect): StoreStrategy =
    source print select
    
  def select(source: PrintedSource)(implicit sys: System.ForStoreStrategySelect): StoreStrategy =
    if (sys.backend contains (source size, source print)) StoreMaybeKnownFile(source) else StoreNewFile(source)

  case class LinkToReference(source: Source, reference: Reference) extends StoreStrategy {
    def execute(parent: TreeEntryID)(implicit sys: System): Unit =
      sys.backend addTreeEntry (parent, source name, source time, reference dataid)
  }
  
  case class StoreNewFile(source: PrintedSource) extends StoreStrategy {
    def execute(parent: TreeEntryID)(implicit sys: System): Unit = using(source) { source =>
      val (hash, size) = HashDigester(sys hashAlgorithm) updatedWith source.content result
      val dataid = sys.backend storeData (source print, hash, size, source content)
      sys.backend addTreeEntry (parent, source name, source time, dataid)
    }
  }
  
  case class StoreMaybeKnownFile(source: PrintedSource) extends StoreStrategy {
    def execute(parent: TreeEntryID)(implicit sys: System): Unit = {
      if (source.size > sys.smallCacheLimit) sys.largeCacheExecution(execute) else execute
      def execute: Unit = using(source){ source =>
        val dataid = source cache sys.cacheSource match {
          case source: CompleteSource => storeCompleteSource(source)
          case source: OpenSource => storeOpenSource(source)
        }
        sys.backend addTreeEntry (parent, source name, source time, dataid)
      }
    }
    
    private def storeCompleteSource(source: CompleteSource)(implicit sys: System): DataEntryID = {
      val (hash, size) = HashDigester(sys hashAlgorithm).updatedWith(source content).result
      sys.backend.dataEntryFor(size, source print, hash)
        .getOrElse(sys.backend storeData (source print, hash, size, source content))
    }

    private def storeOpenSource(source: OpenSource)(implicit sys: System): DataEntryID = {
      val digester = HashDigester(sys hashAlgorithm) updatedWith source.dataStart
      ???
//      val (prelimHash, prelimSize) = digester.copy.updatedWith(source dataTail).result
//      sys.backend.dataEntryFor(prelimSize, source print, prelimHash).getOrElse {
//        val data = source.dataStart ++ digester.digest(source.dataTail)
//        sys.backend storeData (source print, data, digester)
//      }
    }
  }
}
