// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.util

import java.lang.Runtime.{getRuntime => runtime}

object Memory {
  require((runtime maxMemory) != Long.MaxValue)

  private var memory = runtime maxMemory

  def reserve(size: Long): ReserveResult = synchronized {
    if (memory >= size) { memory -= size; Reserved(size) } else NotAvailable(memory)
  }

  def free(size: Long): Unit = synchronized { memory += size }

  def reserved[T](size: Long)(pf: PartialFunction[ReserveResult, T]): T = reserve(size) match {
    case reserved: Reserved => try { pf(reserved) } finally { free(size) }
    case notAvailable: NotAvailable => pf(notAvailable)
  }

  sealed trait ReserveResult
  final case class Reserved(size: Long) extends ReserveResult
  final case class NotAvailable(maxAvailable: Long) extends ReserveResult
}
