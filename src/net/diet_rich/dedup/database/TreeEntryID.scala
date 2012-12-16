// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.database

class TreeEntryID(val id: Long) extends AnyVal

object TreeEntryID { def apply(id: Long) = new TreeEntryID(id) }