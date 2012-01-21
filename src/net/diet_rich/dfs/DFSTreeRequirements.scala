// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dfs

trait DFSTreeRequirements {

  /** Entry names must be non-empty and must not contain "/". */
  protected def isWellformedEntryName(name: String) : Boolean =
    !(name isEmpty) && !(name contains "/")

  /** Sub-paths start but do not end with a slash, and have no consecutive slashes. */
  protected def isWellformedSubPath(path: String) : Boolean =
    ((path matches "/.*[^/]") && !(path contains "//"))

  /** Either "" for root or a valid sub-path. */
  protected def isWellformedPath(path: String) : Boolean =
    (path equals "") || isWellformedSubPath (path) // root or well formed sub path

}