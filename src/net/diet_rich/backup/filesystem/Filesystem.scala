// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.filesystem

import net.diet_rich.util.io.{InputStream,BasicOutputStream}

trait Filesystem[+Repr] {
  def roots : Iterable[Entry[Repr]]
  def entry(path: String) : Entry[Repr]
}

trait IOSignal

object OtherProblem extends IOSignal
object IOError extends IOSignal
object NotAFile extends IOSignal
object NotADir extends IOSignal

case class IOError(error: Throwable) extends IOSignal

trait Entry[+Repr] {
  def name : String
  def path : String
  def parent : Option[Entry[Repr]]
  def isRoot : Boolean = parent.isEmpty
  def rename(newName: String) : Option[IOSignal]
  def move[T >: Repr](newParent: Entry[T]) : Option[IOSignal]
  def delete : Option[IOSignal]
  def deleteAll : Option[IOSignal]
  // directory
  def children : Either[IOSignal, Iterable[Entry[Repr]]]
  // file
  def size : Either[IOSignal, Long]
  def time : Either[IOSignal, Long]
  def input : Either[IOSignal, InputStream]
  def output : Either[IOSignal, BasicOutputStream]
}

//    path = parent.map(p => if (p.isRoot) p.path else p.path + "/").getOrElse("") + name
