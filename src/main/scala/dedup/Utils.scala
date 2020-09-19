package dedup

import dedup.Server.log
import org.slf4j.LoggerFactory

object Utils {
  private val log = LoggerFactory.getLogger("dedup.Utils")

  import Runtime.{getRuntime => rt}
  def freeMemory: Long = rt.maxMemory - rt.totalMemory + rt.freeMemory

  def memoryCheck(): Unit = {
    // https://stackoverflow.com/questions/58506337/java-byte-array-of-1-mb-or-more-takes-up-twice-the-ram
    log.info("Checking memory management for byte arrays now.")
    System.gc(); Thread.sleep(1000)

    val freeBeforeNormalDataCheck = freeMemory
    val normallyHandledData = Vector.fill(100)(new Array[Byte](memChunk))
    System.gc(); Thread.sleep(1000)
    val usedByNormalData = freeBeforeNormalDataCheck - freeMemory
    val deviationWithNormalData = (memChunk*100 - usedByNormalData).abs / memChunk
    log.info(s"${normallyHandledData.size} Byte arrays of size $memChunk used $usedByNormalData bytes of RAM.")
    log.info(s"This is a deviation of $deviationWithNormalData% from the expected value.")
    log.info(s"This software assumes that the deviation is near to 0%.")

    val freeBeforeExceptionalDataCheck = freeMemory
    val exceptionallyHandledData = Vector.fill(100)(new Array[Byte](1048576))
    System.gc(); Thread.sleep(1000)
    val usedByExceptionalData = freeBeforeExceptionalDataCheck - freeMemory
    val deviationWithExceptionalData = (104857600 - usedByExceptionalData).abs / 1000000
    log.info(s"${exceptionallyHandledData.size} Byte arrays of size 1048576 used $usedByExceptionalData bytes of RAM.")
    log.info(s"This is a deviation of $deviationWithExceptionalData% from the expected value.")
    log.info(s"This software assumes that the deviation is near to 100%.")

    require(deviationWithNormalData < 15, "High deviation of memory usage for normal data.")
    require(deviationWithExceptionalData > 80, "Low deviation of memory usage for exceptional data.")
    require(deviationWithExceptionalData < 120, "High deviation of memory usage for exceptional data.")
  }

  def asyncLogFreeMemory(): Unit = concurrent.ExecutionContext.global.execute { () =>
    var lastFree = 0L
    while(true) {
      Thread.sleep(5000)
      val free = freeMemory
      if ((free-lastFree).abs * 10 > lastFree) {  lastFree = free; log.debug(s"Free memory: ${free/1000000} MB") }
    }
  }
}
