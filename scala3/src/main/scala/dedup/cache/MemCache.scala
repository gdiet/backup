package dedup
package cache

import dedup.util.ClassLogging
import java.util.concurrent.atomic.AtomicLong

object MemCache extends ClassLogging:
  private val cacheLimit: Long = math.max(0, (Runtime.getRuntime.maxMemory - 64000000) * 7 / 10)
  log.info(s"Memory cache size: ${readableBytes(cacheLimit)}")
  val availableMem = new AtomicLong(cacheLimit)
end MemCache
