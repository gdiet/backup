package net.diet_rich.dedup.core.meta

import org.specs2.matcher.{Matcher, Matchers}

trait MetaMatchers { _: Matchers =>
  def haveNoData: Matcher[TreeEntry] = beNone ^^ ((_:TreeEntry).data aka "treeEntry.data")
  def haveTheId(id: Long): Matcher[TreeEntry] = beTypedEqualTo(id) ^^ ((_:TreeEntry).id aka "treeEntry.id")
}
