// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dfs

object TestFS {

  private val dbset = DBSettings.h2memoryDatabase
  private val dbcon = new DBConnection(dbset)
  private val fsset = FSSettings default
  private val sqldb = { SqlDB createTables (dbcon, dbset, fsset); new SqlDB(dbcon) }
  private val fscache = new FSDataCache(sqldb)
  
  lazy val memfs : DedupFileSystem = new DedupFileSystem(fscache)
  
}