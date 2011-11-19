// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.filesystem

import net.diet_rich.util.io.{InputStream,OutputStream}

trait Filesystem[+Repr] {
  def roots : Iterable[Dir[Repr]]
  def entry(path: String) : Option[Entry[Repr]]
}

trait Entry[+Repr] {
  def path : String = 
    parent.map(p => if (p.isRoot) p.path else p.path + "/").getOrElse("") + name
  def name : String
  def parent : Option[Entry[Repr]]
  def isRoot : Boolean = parent.isEmpty
}

trait Dir[+Repr] extends Entry[Repr] {
  def children : Iterable[Entry[Repr]]
}

trait File[+Repr] extends Entry[Repr] {
  def size : Long
  def time : Long
  def input : InputStream
}

trait WriteableFile[+Repr] extends File[Repr] {
  def output : OutputStream[Any] // FIXME
}
