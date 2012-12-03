// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.test.backup.alg

import org.mockito.Mockito._
import org.testng.annotations.Test
import org.fest.assertions.Assertions.assertThat
import net.diet_rich.backup.alg.StandardFileSourceTreeProcessor
import net.diet_rich.backup.alg.SourceEntry
import net.diet_rich.backup.alg.TreeDB

class StandardFileSourceTreeProcessorTest {

  @Test
  def test = {
    val processor = new StandardFileSourceTreeProcessor {
      override final type BackupFS = TreeDB
      override final val fs: BackupFS = mock(classOf[TreeDB])
      override final def executeTreeOperation(source: SourceEntry)(command: => Unit): Unit = command
      override final def evaluateReference(src: SourceEntry, dst: Long, ref: Option[Long]): Unit = Unit
    }

    // FIXME zu aufwändig
    val child = mock(classOf[SourceEntry])
    when(child.hasData).thenReturn(true)
    
    val source = mock(classOf[SourceEntry])
    when(source.hasData).thenReturn(false)
    when(source.children).thenReturn(List(child))
    
  }

}
