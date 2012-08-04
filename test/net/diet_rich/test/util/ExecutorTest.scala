package net.diet_rich.test.util

import org.fest.assertions.Assertions.assertThat
import org.testng.annotations.Test
import net.diet_rich.util.Executor

class ExecutorTest {

  @Test
  def inlineExecution: Unit = {
    val thread = Thread.currentThread
    val isExecuted = new java.util.concurrent.atomic.AtomicBoolean(false)
    val isSameThread = new java.util.concurrent.atomic.AtomicBoolean(false)
    val executor = Executor(0,0)
    executor.execute{
      isExecuted.set(true)
      isSameThread.set(Thread.currentThread == thread)
    }
    executor.shutdownAndAwaitTermination
    assertThat(isExecuted.get).isTrue
    assertThat(isSameThread.get).isTrue
  }

  @Test
  def deferredExecution: Unit = {
    val thread = Thread.currentThread
    val isExecuted = new java.util.concurrent.atomic.AtomicBoolean(false)
    val isSameThread = new java.util.concurrent.atomic.AtomicBoolean(false)
    val executor = Executor(4,4)
    executor.execute{
      isExecuted.set(true)
      isSameThread.set(Thread.currentThread == thread)
    }
    executor.shutdownAndAwaitTermination
    assertThat(isExecuted.get).isTrue
    assertThat(isSameThread.get).isFalse
  }
  
}