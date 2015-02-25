// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.util

import java.lang.Runtime.{getRuntime => runtime}
import java.util.concurrent.atomic.AtomicLong

object Memory {
  require((runtime maxMemory) != Long.MaxValue)

  private val memory = new AtomicLong(runtime maxMemory)

  def reserve(size: Long): ReserveResult =
    if ((memory addAndGet -size) >= 0) Reserved(size) else { free(size); NotAvailable(memory get) }

  def free(size: Long): Unit = memory addAndGet size

  def reserved[T](size: Long)(pf: PartialFunction[ReserveResult, T]): T = reserve(size) match {
    case reserved: Reserved => try { pf(reserved) } finally { free(size) }
    case notAvailable: NotAvailable => pf(notAvailable)
  }

  sealed trait ReserveResult
  final case class Reserved(size: Long) extends ReserveResult
  final case class NotAvailable(maxAvailable: Long) extends ReserveResult
}
