// Copyright (c) 2011 Georg Dietrich
// Licensed under the MIT license: http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.backup.filesystem

import java.io.File
import net.diet_rich.util.io.{BasicOutputStream,InputStream,RandomAccessFile,RandomAccessFileInput}
import net.diet_rich.util.Choice._

object PhysicalFilesystem extends Filesystem[PhysicalEntry] {
  override def roots : Iterable[PhysicalEntry] =
    java.io.File.listRoots().map(PhysicalEntry(_))
  override def entry(path: String) : PhysicalEntry = entry(new File(path))
  def entry(file: java.io.File) : PhysicalEntry = PhysicalEntry(file)
}

final class PhysicalEntry(val file: File) extends Entry[PhysicalEntry] {
  // general
  override def name : String = file.getName
  override def path : String = file.getPath.replaceAll(File.separator,"/")
  override def parent : Option[PhysicalEntry] = 
    nullIsNone(file.getParentFile).map(PhysicalEntry(_))
  override def rename(newName: String) : Either[IOSignal, PhysicalEntry] = {
    val newFile = new File(file.getParentFile, newName)
    if (file.renameTo(newFile)) Right(PhysicalEntry(newFile))
    else Left(OtherProblem)
  }
  override def move(newParent: PhysicalEntry) : Either[IOSignal, PhysicalEntry] = {
    val newFile = new File(newParent.file, file.getName)
    if (file.renameTo(newFile)) Right(PhysicalEntry(newFile))
    else Left(OtherProblem)
  }
  override def delete : Option[IOSignal] =
    if (file.delete) None
    else Some(OtherProblem)
  override def deleteAll : Option[IOSignal] =
    throw new UnsupportedOperationException // FIXME
  override def isFile : Boolean =
    file.isFile
  override def isDir : Boolean =
    file.isDirectory
    
  // directory
  override def makedirs : Option[IOSignal] =
    throw new UnsupportedOperationException // FIXME
  override def children : Either[IOSignal, Iterable[PhysicalEntry]] =
    mapRight(nullIsLeft(NotADir, file.listFiles)){_.map(PhysicalEntry(_))}
  
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
