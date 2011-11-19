// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.filesystem

import java.io.File
import net.diet_rich.util.io.{BasicOutputStream,InputStream,RandomAccessFile,RandomAccessFileInput}
import net.diet_rich.util.Choice._

trait PhysicalFilesystem extends Filesystem[PhysicalFilesystem]

object PhysicalFilesystem extends PhysicalFilesystem {
  def roots : Iterable[PhysicalEntry] =
    java.io.File.listRoots().map(PhysicalEntry(_))
  def entry(path: String) : Entry[PhysicalFilesystem] = entry(new File(path))
  def entry(file: java.io.File) : Entry[PhysicalFilesystem] = PhysicalEntry(file)
}

class PhysicalEntry(file: File) extends Entry[PhysicalFilesystem] {
  // general
  override def name : String = file.getName
  override def path : String = file.getPath.replaceAll(File.separator,"/")
  override def parent : Option[PhysicalEntry] = 
    nullIsNone(file.getParentFile){PhysicalEntry(_)}
  
  // directory
  override def children : Either[IOSignal, Iterable[PhysicalEntry]] =
    nullIsLeft(file.listFiles, NotADir){_.map(PhysicalEntry(_))}
  
  // file
  override def size : Either[IOSignal, Long] =
    if (file.isFile) Right(file.length) else Left(NotAFile)
  override def time : Either[IOSignal, Long] =
    if (file.isFile) Right(file.lastModified) else Left(NotAFile)
  override def input : Either[IOSignal, RandomAccessFileInput] =
    if (file.isFile) Right(new RandomAccessFileInput(file))
    else Left(NotAFile)
  override def output : Either[IOSignal, RandomAccessFile] =
    if (file.isFile) Right(new RandomAccessFile(file))
    else Left(NotAFile)
}

object PhysicalEntry {
  def apply(file: File) : PhysicalEntry = new PhysicalEntry(file)
}
