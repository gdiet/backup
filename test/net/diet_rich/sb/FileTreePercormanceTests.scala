// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.sb

import net.diet_rich.util.Configuration.StringMap
import org.testng.annotations.Test
import org.testng.annotations.BeforeClass
import org.fest.assertions.Assertions.assertThat
import TestUtil._
import org.apache.commons.io.FileUtils
import org.testng.annotations.BeforeMethod
import FileTreePerformanceTests._
import java.io.File
import java.util.zip.GZIPInputStream
import java.io.FileInputStream
import scala.io.Source

class FileTreePerformanceTests {

  lazy val connection = DBConnection.hsqlFileDB(testDir)
  lazy val tree : TreeMethods = {
    TreeSqlDB dropTable connection
    TreeSqlDB createTable connection
    TreeSqlDB addInternalConstraints connection
    val dbSettings = Map("TreeDB.cacheSize"->"100")
    TreeDBCache(connection, dbSettings)
  }

  @BeforeMethod
  def prepareTest = {
    FileUtils.cleanDirectory(testDir)
  }
  
  @Test
  def add1000elementsWithin1second = {
    val time = System.currentTimeMillis
    val source = Source.fromInputStream(new GZIPInputStream(new FileInputStream("test/filelist.gz")))
    var count = 0
    source.getLines foreach { path =>
      if (System.currentTimeMillis - time < 1000) {
        count = count + 1
        assertThat(tree create path isDefined) isTrue
      }
    }
    source.close
    assertThat(count).isGreaterThan(1000)
  }
  

}

object FileTreePerformanceTests {
  val testDir = new File("temp/net.diet_rich.sb.FileTreePerformanceTests")
  testDir.mkdirs
}
