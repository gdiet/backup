// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.core

trait Lifecycle {
  def setup(): Unit = {}
  def teardown(): Unit = {}
  final def inLifeCycle[T](f: => T): T = try { setup() ; f } finally teardown()
}
