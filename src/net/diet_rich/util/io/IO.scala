// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.util.io

import net.diet_rich.util.Bytes

trait Closeable {
  def close() : Unit
}

object Closeable {
  /** closes the resource after the operation */
  def using[Closeable <: {def close() : Unit}, ReturnType] (resource: Closeable)(operation: => ReturnType) : ReturnType =
    try { operation } finally { resource.close }

  /** closes the resource after the operation */
  def usingIt[Closeable <: {def close() : Unit}, ReturnType] (resource: Closeable)(operation: Closeable => ReturnType) : ReturnType =
    try { operation(resource) } finally { resource.close }
}

trait Seekable {
  def seek(position: Long) : Unit
}
