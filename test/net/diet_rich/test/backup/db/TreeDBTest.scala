// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.test.backup.db

import org.testng.annotations.Test
import org.testng.annotations.BeforeClass
import org.fest.assertions.Assertions.assertThat
import net.diet_rich.util.sql.DBConnection
import net.diet_rich.backup.db.TreeDB
import net.diet_rich.backup.db.TreeSqlDB

class SimpleFileTreeTests1 {

  lazy val connection = DBConnection.hsqlMemoryDB()
  lazy val tree = {
    TreeSqlDB.createTable(connection)
    TreeDB.standardDB(connection)
  }
    
  @Test
  def createPathWithMultipleElements = {
    val originalPath = tree.getOrMake("/create/Path")
    val originalAnother = tree.getOrMake("/create/Another")
    val idsCreate = tree.children(TreeDB.ROOTID).filter(_.name == "create")
    assertThat(idsCreate.size) isEqualTo 1
    val idCreate = idsCreate.head.id
    assertThat(tree.children(idCreate).size) isEqualTo 2
    val idsPath = tree.children(idCreate).filter(_.name == "Path")
    val idsAnother = tree.children(idCreate).filter(_.name == "Another")
    assertThat(idsPath.size) isEqualTo 1
    assertThat(idsAnother.size) isEqualTo 1
    assertThat(idsPath.head.id) isEqualTo originalPath
    assertThat(idsAnother.head.id) isEqualTo originalAnother
  }

  @Test
  def renamedNodesName = {
    val originalPath = tree.getOrMake("/rename/Path")
    val idRename = tree.children(TreeDB.ROOTID).filter(_.name == "rename").head.id
    tree.rename(originalPath, "NewName")
    assertThat(tree.children(idRename).size) isEqualTo 1
    assertThat(tree.children(idRename).head.id) isEqualTo originalPath
    assertThat(tree.children(idRename).head.name) isEqualTo "NewName"
    assertThat(tree.entry(originalPath).get.name) isEqualTo "NewName"
  }
  
  @Test
  def movedNode = {
    val source = tree.getOrMake("/source")
    val path = tree.getOrMake("/source/Path")
    val target = tree.getOrMake("/target")
    assertThat(tree.children(source).size) isEqualTo 1
    assertThat(tree.children(target).size) isEqualTo 0
    tree.move(path, target)
    assertThat(tree.children(source).size) isEqualTo 0
    assertThat(tree.children(target).size) isEqualTo 1
    assertThat(tree.getOrMake("/target/Path")) isEqualTo path
  }

  @Test
  def deletedNode = {
    val delete = tree.getOrMake("/delete")
    val path = tree.getOrMake("/delete/Path")
    val another = tree.getOrMake("/delete/Path/Another")
    tree.deleteWithChildren(path)
    assertThat(tree.children(delete).size) isZero()
    assertThat(tree.entry(path)) isEqualTo None
    assertThat(tree.entry(another)) isEqualTo None
  }

}
