package net.diet_rich.dedupfs.metadata.sql

import java.sql.Connection

import org.specs2.Specification
import org.specs2.specification.AfterAll

import net.diet_rich.common._, sql._, test._
import net.diet_rich.dedupfs.metadata.TreeEntry

class TreeStandardOpsSpec extends Specification with AfterAll with TestsHelper {
  def is = sequential ^ s2"""
General tests for the tree database, starting with a newly initialized database
  where child is TreeEntry(1, 0, "child", None, None):
  ${eg{ db.treeChildrenOf(0) === List() }}
  ${eg{ db.treeInsert(0, "child", None, None) === 1 }}
  ${eg{ db.treeInsert(1, "grandChild", None, None) === 2 }}
  ${eg{ db.treeChildrenOf(0) === List(child) }}
  ${eg{ db.treeChildrenOf(0, isDeleted = true) === List() }}
  ${eg{ db.treeDelete(child) === unit }}
  ${eg{ db.treeChildrenOf(0) === List() }}
  ${eg{ db.treeChildrenOf(0, isDeleted = true) === List(child) }}
  ${eg{ db.treeChildrenOf(0, upToId = 0) === List() }}
  ${eg{ db.treeChildrenOf(0, upToId = 2) === List(child) }}
  ${eg{ db.treeChildrenOf(0, upToId = 3) === List() }}
  ${eg{ db.treeChildrenOf(0, upToId = 3,isDeleted = true) === List(child) }}
  """

  val child = TreeEntry(1, 0, "child", None, None)

  lazy val db = new {
    override implicit final val connectionFactory: ConnectionFactory = H2.memoryFactory(className)
    override final val hashAlgorithm = "MD5"
  } with DatabaseRead with DatabaseWrite {
    override final def close() = !!!
    Database create "MD5"
  }
  def afterAll(): Unit = db.connectionFactory close()
}
