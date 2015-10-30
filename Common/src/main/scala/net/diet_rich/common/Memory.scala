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

  def reserved[T](size: Long)(f: ReserveResult => T): T = reserve(size) match {
    case reserved: Reserved => try { f(reserved) } finally { free(size) }
    case notAvailable: NotAvailable => f(notAvailable)
  }

  sealed trait ReserveResult
  final case class Reserved(size: Long) extends ReserveResult
  final case class NotAvailable(maxAvailable: Long) extends ReserveResult
}
