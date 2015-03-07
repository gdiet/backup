package net.diet_rich.dedup.core.meta

import org.specs2.Specification

class PathsSpec extends Specification { def is = s2"""
${"Tests for the path utility methods".title}

The path of the root node should have no elements $pathOfRootNode
The path elements of a node should be determined correctly $pathOfNode
The path elements are the same whether or not a path termiates in '/' $terminatingSeparator
Paths other than the root path must start with '/' $startingSeparator
"""

  val examplePath = ""
  def pathOfRootNode = pathElements("") should beEmpty
  def pathOfNode = pathElements("/some/path/for/testing") should beEqualTo(Array("some", "path", "for", "testing"))
  def terminatingSeparator = (pathElements("/") should beEmpty) and (pathElements("/path/") should beEqualTo(Array("path")))
  def startingSeparator = pathElements("illegalPath") should throwA[IllegalArgumentException]
}
