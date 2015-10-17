package net.diet_rich.dedupfs.metadata

import org.specs2.Specification

class TreePathSpec extends Specification {
  import TreeEntry.pathElements

  def is = s2"""
General tests for tree path handling
 ${eg {pathElements("") === Array() }}
 ${eg {pathElements("/") === Array() }}
 ${eg {pathElements("/some/path") === Array("some", "path") }}
 ${eg {pathElements("/some/path/") === Array("some", "path") }}
 ${eg {pathElements("illegalPath") should throwA[IllegalArgumentException] }}
"""
}
