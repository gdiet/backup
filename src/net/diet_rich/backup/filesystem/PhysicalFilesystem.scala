// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.filesystem

import net.diet_rich.util.io.{InputStream,OutputStream}

trait PhysicalFilesystem extends Filesystem[PhysicalFilesystem]

//object PhysicalFilesystem extends PhysicalFilesystem {
//  def roots : Iterable[PhysicalDir] =
//    java.io.File.listRoots().map(new PhysicalRoot(_))
//  def entry(path: String) : Option[Entry[PhysicalFilesystem]] = None // FIXME
//  def entry(file: java.io.File) : Option[Entry[PhysicalFilesystem]] = None // FIXME
//}

//class PhysicalDir(val file: java.io.File, parent : => Option[Entry[PhysicalFilesystem]]) extends Dir[PhysicalFilesystem] {
//  override def name = file.getName
//  override def children : Iterable[Entry[PhysicalFilesystem]] = 
//    file.listFiles().map(file =>
////      if (file.isDirectory())
//        new PhysicalDir(file, {Some(this)})
////      else new PhysicalFile(file, Some(this))
//    )
//}
//
//class PhysicalRoot(override val file: java.io.File) extends PhysicalDir(file, None) {
//  override def name = file.getName().replace("\\","/")
//}

//class PhysicalFile(val file: java.io.File, override val parent : Option[Entry[PhysicalFilesystem]]) extends WriteableFile[PhysicalFilesystem] {
//  override def name = file.getName
//  def size : Long = 0 // FIXME
//  def time : Long = 0 // FIXME
//  def input : InputStream = InputStream.empty // FIXME
//  def output : OutputStream = OutputStream.empty // FIXME
//}
