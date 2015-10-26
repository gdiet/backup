package net.diet_rich.dedupfs.metadata

import org.specs2.Specification

class TreePathSpec extends Specification {
  import TreeEntry.RichPath

  def is = s2"""
General tests for tree path handling
 ${eg {"".pathElements === Array() }}
 ${eg {"/".pathElements === Array() }}
 ${eg {"/some/path".pathElements === Array("some", "path") }}
 ${eg {"/some/path/".pathElements === Array("some", "path") }}
 ${eg {"illegalPath".pathElements should throwA[IllegalArgumentException] }}
General tests for tree parent handling
 ${eg {"".parent === "" }}
 ${eg {"/".parent === "" }}
 ${eg {"abc/".parent === "" }}
 ${eg {"/abc".parent === "" }}
 ${eg {"/abc/".parent === "" }}
 ${eg {"/abc//".parent === "/abc" }}
 ${eg {"/abc/def".parent === "/abc" }}
"""
}
