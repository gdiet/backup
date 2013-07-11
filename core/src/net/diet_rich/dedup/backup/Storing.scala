// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.backup

import net.diet_rich.dedup.vals._
import net.diet_rich.util.!!!
import net.diet_rich.util.io.BytesSource
import net.diet_rich.util.vals._

class Storing(settings: Settings) {
  import settings._
  
  import StoreStrategy._
  def store(strategy: StoreStrategy, parent: TreeEntryID) = strategy match {
    case LinkToReference(source, reference) => linkToReference(source, parent, reference)
    case StoreNewFile(source) => storeNewFile(source, parent)
    case StoreMaybeKnownFile(source) => storeMaybeKnownFile(source, parent)
  }

  def linkToReference(source: Source, parent: TreeEntryID, reference: Reference) =
    backend addTreeEntry (parent, source name, source time, reference dataid)
  
  def storeNewFile(source: PrintedSource, parent: TreeEntryID) = {
    val dataid = backend storeData (source print, source content)
    source.close
    backend addTreeEntry (parent, source name, source time, dataid)
  }
  
  def storeMaybeKnownFile(source: PrintedSource, parent: TreeEntryID) = {
    if (source.notCached > smallCacheLimit) largeCacheExecution(execute) else execute
    def execute = {
      val cacheSize = Size(math.min(source.notCached.value + 1, cacheLimit.value))
      val dataCache = cache(cacheSize)
      source.cache(dataCache) match {
        case source: CompleteSource =>
          val (hash, size) = Hash of (source content)
          val dataid = backend.dataEntryFor(size, source print, hash)
            .getOrElse(backend storeData (source print, hash, source content))
          backend addTreeEntry (parent, source name, source time, dataid)
        case source: OpenSource =>
          // partially cached => needs either a) re-read or b) seek
          ???
      }
    }
  }
}
