package dedup

import java.util
import java.util.concurrent.atomic.AtomicLong

package object cache {
  object Entry { def unapply[K,V](e: util.Map.Entry[K,V]): Option[(K,V)] = Some(e.getKey -> e.getValue) }

  implicit class LongDecorator(val long: Long) extends AnyVal {
    def asInt: Int = { assert(long >= 0 && long <= Int.MaxValue, s"Illegal value $long"); long.toInt }
  }

  trait DataSink[D] {
    def write(d: D, offset: Long, data: Array[Byte]): Unit
  }

  implicit class DataSinkOps[D](val d: D) {
    def write(offset: Long, data: Array[Byte])(implicit dataSink: DataSink[D]): Unit = dataSink.write(d, offset, data)
  }

  implicit object PointerSink extends DataSink[jnr.ffi.Pointer] {
    override def write(dataSink: jnr.ffi.Pointer, offset: Long, data: Array[Byte]): Unit =
      dataSink.put(offset, data, 0, data.length)
  }

  trait MemArea[M] {
    def length(t: M): Long
    def drop(t: M, distance: Long): M
    def dropRight(t: M, distance: Long): M
    def take(t: M, distance: Long): M
  }

  implicit class MemAreaOps[M](val m: M) extends AnyVal {
    def length(implicit memArea: MemArea[M]): Long = memArea.length(m)
    def drop()(implicit memArea: MemArea[M]): Unit = drop(length)
    def drop(distance: Long)(implicit memArea: MemArea[M]): M = {
      assert(distance <= length && distance > 0, s"Distance: $distance")
      memArea.drop(m, distance)
    }
    def dropRight(distance: Long)(implicit memArea: MemArea[M]): M = {
      assert(distance < length && distance > 0, s"Distance: $distance")
      memArea.dropRight(m, distance)
    }
    def take(distance: Long)(implicit memArea: MemArea[M]): M = {
      assert(distance < length && distance > 0, s"Distance: $distance")
      memArea.take(m, distance)
    }
  }

  class ByteArrayArea(available: AtomicLong) extends MemArea[Array[Byte]] {
    override def length(t: Array[Byte]): Long = t.length
    override def drop(t: Array[Byte], distance: Long): Array[Byte] = {
      available.addAndGet(distance)
      t.drop(distance.asInt)
    }
    override def dropRight(t: Array[Byte], distance: Long): Array[Byte] = {
      available.addAndGet(distance)
      t.dropRight(distance.asInt)
    }
    override def take(t: Array[Byte], distance: Long): Array[Byte] = {
      available.addAndGet(t.length - distance)
      t.take(distance.asInt)
    }
  }

  implicit object LongArea extends MemArea[Long] {
    override def length(t: Long): Long = t
    override def drop(t: Long, distance: Long): Long = t - distance
    override def dropRight(t: Long, distance: Long): Long = t - distance
    override def take(t: Long, distance: Long): Long = distance
  }

  implicit object IntArea extends MemArea[Int] {
    override def length(t: Int): Long = t
    override def drop(t: Int, distance: Long): Int = t - distance.asInt
    override def dropRight(t: Int, distance: Long): Int = t - distance.asInt
    override def take(t: Int, distance: Long): Int = distance.asInt
  }
}
