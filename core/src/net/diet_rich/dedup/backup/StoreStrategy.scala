// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.backup

import net.diet_rich.util.vals.Size
import net.diet_rich.dedup.vals.Print
import net.diet_rich.util.vals.Time

sealed trait StoreStrategy
case object LinkToReference extends StoreStrategy
case object StoreNewFile extends StoreStrategy
case object StoreMaybeKnownFile extends StoreStrategy

trait StoreStrategySelect {
  type Database = {
    def contains(size: Size, print: Print): Boolean
  }
  type Resource = {
    def time: Time
    def size: Size
    def print: Print
  }

  val checkReferencePrints: Boolean
  val database: Database

  final def storeStrategy(source: Resource, reference: Option[Resource]): StoreStrategy =
    reference map(storeStrategy(source, _)) getOrElse storeStrategy(source)

  final def storeStrategy(source: Resource, reference: Resource): StoreStrategy =
    if (source.time == reference.time && source.size == reference.size)
      if (checkReferencePrints)
        if (source.print == reference.print) LinkToReference
        else storeStrategy(source)
      else LinkToReference
    else storeStrategy(source)

  final def storeStrategy(source: Resource): StoreStrategy =
    if (database contains (source size, source print)) StoreMaybeKnownFile else StoreNewFile
}
