// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.util

import java.util.concurrent.atomic.AtomicLong
import Runtime.{getRuntime => runtime}

sealed trait MemoryReserveResult
case class MemoryReserved(size: Long) extends MemoryReserveResult
case class MemoryNotAvailable(maxAvailable: Long) extends MemoryReserveResult

object Memory {
  require((runtime maxMemory) != Long.MaxValue)

  private val memory = new AtomicLong(runtime maxMemory)

  def reserve(size: Long): MemoryReserveResult =
    if ((memory addAndGet -size) >= 0) MemoryReserved(size) else { free(size); MemoryNotAvailable(memory get) }

  def free(size: Long): Unit = memory addAndGet size

  def reserved[T](size: Long)(pf: PartialFunction[MemoryReserveResult, T]): T = reserve(size) match {
    case reserved: MemoryReserved => try { pf(reserved) } finally { free(size) }
    case notAvailable: MemoryNotAvailable => pf(notAvailable)
  }
}
