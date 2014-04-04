// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.database

import org.specs2.SpecificationWithJUnit
import net.diet_rich.util.Hash
import net.diet_rich.util.init
import net.diet_rich.util.sql._

class TreeDBSpec extends SpecificationWithJUnit { def is = s2"""
  The tree root must not have itself as child $rootIsNotRootChild
  The tree root must not have itself in its children's list $rootIsNotInRootChildren
  """
  
  implicit lazy val connectionWithTable = init (TestDB.h2mem) (TreeDB createTable _)
  lazy val tree = new ImplicitConnection() with TreeDB with TreeDBQueries
  
  def rootIsNotInRootChildren =
    tree.children(tree.ROOTID)
    .filter(_.id == tree.ROOTID)
    .aka("children of root that are root")
    .should(beEmpty)
    
  def rootIsNotRootChild =
    tree.child(tree.ROOTID, TreeDB.ROOTNAME)
    .filter(_.id == tree.ROOTID)
    .aka("child of root that is root")
    .should(beEmpty)
}
