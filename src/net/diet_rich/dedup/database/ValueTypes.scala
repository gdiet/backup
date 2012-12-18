// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.database

class TreeEntryID(val id: Long) extends AnyVal
object TreeEntryID { def apply(id: Long) = new TreeEntryID(id) }

class DataEntryID(val id: Long) extends AnyVal
object DataEntryID { def apply(id: Long) = new DataEntryID(id) }

class Size(val size: Long) extends AnyVal
object Size { def apply(size: Long) = new Size(size) }

class Time(val time: Long) extends AnyVal
object Time { def apply(time: Long) = new Time(time) }

class Print(val print: Long) extends AnyVal
object Print { def apply(print: Long) = new Print(print) }

class Hash(val hash: Array[Byte]) extends AnyVal
object Hash { def apply(hash: Array[Byte]) = new Hash(hash) }
