// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core.values

case class DataEntry (
  id: DataEntryID,
  size: Size,
  print: Print,
  hash: Hash,
  method: StoreMethod
)
