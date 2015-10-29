package net.diet_rich.common

import java.lang.Runtime.{getRuntime => runtime}

object Memory {
  require(runtime.maxMemory != Long.MaxValue)

  private var memory = runtime.maxMemory

  def available = synchronized(memory)

  def reserve(size: Long): ReserveResult = synchronized {
    if (memory >= size) { memory -= size; Reserved(size) } else NotAvailable(memory)
  }

  def free(size: Long): Unit = synchronized { memory += size }

  // FIXME why PartialFunction and not simply ReserveResult => T?
  def reserved[T](size: Long)(pf: PartialFunction[ReserveResult, T]): T = reserve(size) match {
    case reserved: Reserved => try { pf(reserved) } finally { free(size) }
    case notAvailable: NotAvailable => pf(notAvailable)
  }

  sealed trait ReserveResult
  final case class Reserved(size: Long) extends ReserveResult
  final case class NotAvailable(maxAvailable: Long) extends ReserveResult
}
