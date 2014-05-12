// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import org.specs2.matcher.{Matcher, Matchers}
import net.diet_rich.dedup.core.values.{TreeEntryID, TreeEntry}

trait ValueMatchers { _: Matchers =>
  def beADirectory: Matcher[TreeEntry] = beTrue ^^ ((_:TreeEntry).isDirectory aka "treeEntry.isDirectory")
  def beAFile: Matcher[TreeEntry] = beTrue ^^ ((_:TreeEntry).isFile aka "treeEntry.isFile")
  def beSomeDirectory: Matcher[Option[TreeEntry]] = beSome(beADirectory) ^^ ((_:Option[TreeEntry]) aka "Option[TreeEntry]")
  def beSomeFile: Matcher[Option[TreeEntry]] = beSome(beAFile) ^^ ((_:Option[TreeEntry]) aka "Option[TreeEntry]")
  def haveTheId(id: TreeEntryID): Matcher[TreeEntry] = beTypedEqualTo(id) ^^ ((_:TreeEntry).id aka "treeEntry.id")
  def haveSomeId(id: TreeEntryID): Matcher[Option[TreeEntry]] = beSome(haveTheId(id)) ^^ ((_:Option[TreeEntry]) aka "Option[TreeEntry]")
}
