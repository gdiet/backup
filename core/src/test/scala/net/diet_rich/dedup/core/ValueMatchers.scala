// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import org.specs2.matcher.{Matcher, Matchers}
import net.diet_rich.dedup.core.values.TreeEntry

trait ValueMatchers { _: Matchers =>
  def beSomeDirectory: Matcher[Option[TreeEntry]] = beSome(beADirectory) ^^ ((_:Option[TreeEntry]) aka "Option[TreeEntry]")
  def beADirectory: Matcher[TreeEntry] = (beTrue) ^^ { (_:TreeEntry).isDirectory aka "treeEntry.isDirectory" }
  def beSomeFile: Matcher[Option[TreeEntry]] = beSome(beAFile) ^^ ((_:Option[TreeEntry]) aka "Option[TreeEntry]")
  def beAFile: Matcher[TreeEntry] = (beTrue) ^^ { (_:TreeEntry).isFile aka "treeEntry.isFile"}
}
