// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.backup

import net.diet_rich.dedup.vals.Print
import net.diet_rich.util.io.using
import net.diet_rich.util.vals.Size
import net.diet_rich.util.vals.Time

sealed trait StoreStrategy

object StoreStrategy {
  case class LinkToReference(source: Source, reference: Reference) extends StoreStrategy
  case class StoreNewFile(source: PrintedSource) extends StoreStrategy
  case class StoreMaybeKnownFile(source: PrintedSource) extends StoreStrategy
  
  def selectWithReference(settings: Settings)(source: Source, reference: Reference): StoreStrategy =
    if (source.time == reference.time && source.size == reference.size)
      if (settings.checkReferencePrints) {
        source.print { printedSource =>
          if (printedSource.print == reference.print) {
            printedSource.close
            LinkToReference(source, reference)
          } else selectForSource(settings)(printedSource)
        }
      } else LinkToReference(source, reference)
    else source print selectForSource(settings)

  def selectForSource(settings: Settings)(source: PrintedSource): StoreStrategy =
    if (settings.backend contains (source size, source print)) StoreMaybeKnownFile(source) else StoreNewFile(source)
}
