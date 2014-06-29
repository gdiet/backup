// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

import org.specs2.matcher.{Matcher, Matchers}
import net.diet_rich.dedup.core.values.{Bytes, TreeEntryID, TreeEntry}

trait TreeMatchers { _: Matchers =>
  def beADirectory: Matcher[TreeEntry] = beNone ^^ ((_:TreeEntry).data aka "treeEntry.data")
  def beAFile: Matcher[TreeEntry] = beSome ^^ ((_:TreeEntry).data aka "treeEntry.data")
  def haveTheId(id: TreeEntryID): Matcher[TreeEntry] = beTypedEqualTo(id) ^^ ((_:TreeEntry).id aka "treeEntry.id")
}
