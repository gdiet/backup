// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dfs

import DataDefinitions.TimeAndData

trait CacheForFiles {

  protected def db: SqlForFiles

  def store(id: Long, timeAndData: TimeAndData) : Unit =
    db store (id, timeAndData)

}