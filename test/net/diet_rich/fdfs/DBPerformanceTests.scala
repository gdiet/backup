// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.fdfs

import org.testng.annotations.{BeforeMethod,BeforeTest,Test}
import org.fest.assertions.Assertions.assertThat
import net.diet_rich.sb.TestUtil._
import java.io.File
import org.apache.commons.io.FileUtils
import net.diet_rich.TestFileTree

class DBPerformanceTests {

  lazy val dbfile = new File("temp/net.diet_rich.fdfs.DBPerformanceTests/database")
  lazy val connection = DBConnection.h2FileDB(dbfile)
  lazy val treedb: TreeDB = new TreeSqlDB()(connection)
  lazy val datadb: DataInfoDB = new DataInfoSqlDB()(connection)
  lazy val storedb: ByteStoreDB = new ByteStoreSqlDB()(connection)
  
  @BeforeTest
  def setupTest: Unit = {
    dbfile.getParentFile.mkdirs
    FileUtils.cleanDirectory(dbfile.getParentFile)
  }

  @BeforeMethod
  def setupMethod: Unit = {
    TreeSqlDB dropTable connection
    TreeSqlDB createTable connection
    DataInfoSqlDB dropTable connection
    DataInfoSqlDB createTable (connection, "MD5")
    ByteStoreSqlDB dropTable connection
    ByteStoreSqlDB createTable connection
  }
  
  def warmUpDatabase = {
    // this speeds up execution of subsequent database commands
    for (i <- 1 to 5000) treedb.create(0, "node" + i)
  }
  
  @Test
  def create15000TreeEntriesInOneSecond = {
    def process(entry: net.diet_rich.TreeEntry, id: Long) : Unit = {
      entry.children.reverse.foreach { node =>
        if (node.timeAndSize isDefined) {
          treedb.create(id, node.name, node.time)
        } else {
          treedb.create(id, node.name)
          val childId = treedb.create(id, node.name)
          process(node, childId)
        }
      }
    }
    warmUpDatabase
    val time = System.currentTimeMillis
    for (i <- 1 to 3) process(TestFileTree.treeRoot, 0)
    assertThat(System.currentTimeMillis - time) isLessThan 1000
    println("create15000TreeEntriesInOneSecond: " + (System.currentTimeMillis - time))
  }

  def dataInfoForNode(entry: net.diet_rich.TreeEntry) =
    DataInfo(entry.size, entry.size*17, new Array[Byte](0), 0)
  
  @Test
  def create10000TreeAndDataEntriesInOneSecond = {
    def process(entry: net.diet_rich.TreeEntry, id: Long) : Unit = {
      entry.children.reverse.foreach { node =>
        if (node.timeAndSize isDefined) {
          val dataid = datadb.reserveID
          datadb.create(dataid, dataInfoForNode(node))
          treedb.create(id, node.name, node.time, dataid)
        } else {
          treedb.create(id, node.name)
          val childId = treedb.create(id, node.name)
          process(node, childId)
        }
      }
    }
    warmUpDatabase
    val time = System.currentTimeMillis
    for (i <- 1 to 2) process(TestFileTree.treeRoot, 0)
    assertThat(System.currentTimeMillis - time) isLessThan 1000
    println("create10000TreeAndDataEntriesInOneSecond: " + (System.currentTimeMillis - time))
  }
  
  @Test
  def create5000TreeAndDataEntriesAndByteStoreEntriesInOneSecond = {
    def process(entry: net.diet_rich.TreeEntry, id: Long) : Unit = {
      entry.children.reverse.foreach { node =>
        if (node.timeAndSize isDefined) {
          val dataid = if (datadb.hasMatchingPrint(node.size, node.size*17)) {
            val found = datadb.findMatch(node.size, node.size*17, new Array[Byte](0))
            found.getOrElse {
              val dataid = datadb.reserveID
              
              var sizeCount = node.size
              storedb.write(dataid){ range => 
                val oldCount = sizeCount
                sizeCount = math.max(0, sizeCount - range.length)
                if (sizeCount > 0) range.length else oldCount
              }
              
              datadb.create(dataid, dataInfoForNode(node))
              dataid
            }
          } else {
            val dataid = datadb.reserveID
            
            var sizeCount = node.size
            storedb.write(dataid){ range => 
              val oldCount = sizeCount
              sizeCount = math.max(0, sizeCount - range.length)
              if (sizeCount > 0) range.length else oldCount
            }
            
            datadb.create(dataid, dataInfoForNode(node))
            dataid
          }
          treedb.create(id, node.name, node.time, dataid)
          
        } else {
          treedb.create(id, node.name)
          val childId = treedb.create(id, node.name)
          process(node, childId)
        }
      }
    }
    // get the database already going - this speeds up the following code
    for (i <- 1 to 5000) treedb.create(0, "node" + i)
    
    val time = System.currentTimeMillis
    process(TestFileTree.treeRoot, 0)
    assertThat(System.currentTimeMillis - time) isLessThan 1000
    println("create5000TreeAndDataEntriesAndByteStoreEntriesInOneSecond: " + (System.currentTimeMillis - time))
  }
  
}