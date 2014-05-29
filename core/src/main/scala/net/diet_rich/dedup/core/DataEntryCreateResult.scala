// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import net.diet_rich.dedup.core.values.DataEntryID

sealed trait DataEntryCreateResult { val id: DataEntryID }
case class DataEntryCreated(id: DataEntryID) extends DataEntryCreateResult
case class ExistingEntryMatches(id: DataEntryID) extends DataEntryCreateResult
