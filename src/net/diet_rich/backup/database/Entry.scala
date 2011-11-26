// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.database

sealed case class Entry(id: Long, parent: Long, name: String, typ: String) {
  private[database] def this(parent: Long) = this(-1, parent, "", "")
}
