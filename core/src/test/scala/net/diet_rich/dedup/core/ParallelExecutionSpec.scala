package net.diet_rich.dedup.core

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.Future

import org.specs2.Specification

import net.diet_rich.dedup.util.io.using

class ParallelExecutionSpec extends Specification { def is = s2"""
${"Tests for the multi-threading utilities".title}

The "combine" utility should wait for all futures to finish, even if one fails $combineWaitsForAll
"""

  def combineWaitsForAll = {
    val count = new AtomicInteger(0)
    class Parallel(val parallel: Option[Int]) extends ParallelExecution {
      def futureEval() = awaitForever(combine(List.fill(20)(Future {
        Thread sleep 1
        count.incrementAndGet()
        throw new IllegalStateException
      })))
    }
    using(new Parallel(Some(1))) { parallel =>
      parallel.futureEval() should throwA[IllegalStateException] and count.get() === 20
    }
  }
}
