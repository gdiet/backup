// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.sb

import java.io.File
import net.diet_rich.util.Configuration.StringMap
import org.apache.commons.io.FileUtils
import org.fest.assertions.Assertions.assertThat
import org.testng.annotations.BeforeMethod
import org.testng.annotations.Test
import TestUtil._

class SimpleFileSystemTests {

  lazy val connection = DBConnection.hsqlMemoryDB()
  
  lazy val dbSettings = Map("TreeDB.cacheSize"->"3")
  
  lazy val fs = new Tree with ReadWrite {
    TreeSqlDB createTable(connection)
    TreeSqlDB addInternalConstraints connection
    val treeDb = TreeDB(connection, dbSettings) // without cache: TreeSqlDB(connection)
  }

  lazy val testDir = new File("temp/net.diet_rich.sb.SimpleFileSystemTests")
  
  @BeforeMethod
  def prepareTest = {
    testDir.mkdirs
    FileUtils.cleanDirectory(testDir)
  }

  @Test
  def storeOneFile = {
    val folderName = randomString
    val testFile = new File(testDir, "storeOneFile")
    FileUtils.write(testFile, "file content", "UTF-8")
    
    val fileid = fs make("/"+folderName+"file")
    assertThat(fileid isDefined) isTrue()
    
    val dataid = fs write(fileid get, testFile)
    assertThat(dataid isDefined) isTrue()
  }
  
  
}