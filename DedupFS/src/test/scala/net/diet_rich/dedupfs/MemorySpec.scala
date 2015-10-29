package net.diet_rich.dedupfs

import java.lang.Runtime.{getRuntime => runtime}

import org.specs2.Specification

import net.diet_rich.common._, test._

class MemorySpec extends Specification { def is = s2"""
Working with massive amounts of memory should be possible
using the pattern employed in this test
 $theMemoryTest
  """

  def theMemoryTest = {
    val availableMemory = runtime.maxMemory() - runtime.totalMemory() + runtime.freeMemory()
//    memStats()
//    println(s"available = $availableMemory")
    run(availableMemory * 7 / 10, 6) === unit
  }

  def memStats() = println(s"max: ${runtime.maxMemory()} - total: ${runtime.totalMemory()} - free: ${runtime.freeMemory()}")

  val parts = 5000

  def run(memoryToUse: Long, stage: Int): Unit = if (stage > 0) {
//    println(s"memory test stage $stage with $memoryToUse bytes")
    val data = Array.fill(parts){Bytes zero (memoryToUse / parts).toInt}
    val sum = (Bytes consumingIterator data).map(_.length.toLong).sum
//    println(s"calculated memory use: $sum")
    run(memoryToUse, stage-1)
  }
}
