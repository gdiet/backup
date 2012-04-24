// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.sb

trait TreeCacheUpdater {
  def registerUpdateAdapter(adapter: TreeCacheUpdateAdapter)
}

trait TreeCacheUpdateAdapter {
  def created(id: Long, name: String, parent: Long)
  def renamed(id: Long, newName: String)
  def moved(id: Long, oldParent: Long, newParent: Long)
  def deleted(id: Long, oldParent: Long)
}

trait TreeDataUpdater {
  def registerUpdateAdapter(adapter: TreeDataUpdateAdapter)
}

trait TreeDataUpdateAdapter {
  def deleted(dataId: Long)
}
