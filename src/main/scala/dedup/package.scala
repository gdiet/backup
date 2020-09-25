import java.util.concurrent.atomic.AtomicBoolean

import org.slf4j.Logger

package object dedup extends scala.util.ChainingSyntax {
  // https://stackoverflow.com/questions/58506337/java-byte-array-of-1-mb-or-more-takes-up-twice-the-ram
  val memChunk = 524200 // a bit less than 0.5 MiB to avoid problems with humongous objects in G1GC

  val copyWhenMoving = new AtomicBoolean(false)

  type Chunk = (Long, Long)

  implicit class ChunkDecorator(chunk: Chunk) {
    def start: Long = chunk._1
    def stop: Long = chunk._2
    def size: Long = chunk.pipe { case (start, stop) => stop - start }
  }
  def combinedSize(chunks: Seq[Chunk]): Long = chunks.map(_.size).sum

  // TODO replace "require" with assumeLogged/assertLogged?
  def assumeLogged(condition: Boolean, message: => String)(implicit log: Logger): Unit =
    if (!condition) log.error(s"Assumption failed: $message", new IllegalStateException(""))

  def assertLogged(condition: Boolean, message: => String)(implicit log: Logger): Unit =
    if (!condition) throw new IllegalStateException(message).tap(log.error(s"Assertion failed: $message", _))
}
