package net.diet_rich.testutils

import java.io.File
import org.testng.annotations.{BeforeClass, AfterClass}

trait TempFileProvider {

  type Closeable = {def close() : Unit}
  
  private val files = new collection.mutable.HashSet[File] with collection.mutable.SynchronizedSet[File]

  private val closeables = new collection.mutable.HashSet[Closeable] with collection.mutable.SynchronizedSet[Closeable]

  /** provide a new temp file that will be deleted when the class' tests are finished. */
  def newTempFile = { val file = File.createTempFile("testng_", "tmp"); files += file; file }

  /** schedule a closeable for later closing */
  def closeLater[T <: Closeable](closeable : T) : T = { closeables += closeable; closeable }
  
  @AfterClass
  private[testutils] def afterClass = { closeables.foreach(item => item.close); files.foreach(file => file.delete) }

}