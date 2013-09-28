// Copyright (c) Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.dedup.vals

import java.security.MessageDigest
import net.diet_rich.util.vals._

case class Hash(value: Array[Byte]) extends ByteArrayValue { import java.util.Arrays
  override def equals(a: Any) = a match {
    case null => false
    case Hash(other) => Arrays.equals(value, other)
    case _ => false
  }
  override def hashCode() = Arrays.hashCode(value)
}

class HashDigester(md: MessageDigest, private var bytesDigested: Long) {
  def updatedWith(data: Traversable[Bytes]) = {
    digest(data).foreach{_=>}
    this
  }
  def copy = new HashDigester(md.clone.asInstanceOf[MessageDigest], bytesDigested)
  def result: (Hash, Size) = (Hash(md.digest()), Size(bytesDigested))
  def digest(data: Traversable[Bytes]) = {
    data.map { b =>
      md.update(b.data, b.offset, b.length)
      bytesDigested = bytesDigested + b.length
      b
    }
  }
}
object HashDigester {
  def apply(algorithm: String): HashDigester =
    new HashDigester(MessageDigest.getInstance(algorithm), 0)
}

//object Hash {
//  def of(algorithm: String, source: Traversable[Bytes]): (Hash, Size) = ???
//  def digester(algorithm: String, source: Traversable[Bytes]): HashDigester =
//    HashDigester(MessageDigest.getInstance(algorithm), source)
//}
//
//case class HashDigester(md: MessageDigest, source: Traversable[Bytes], queued: Traversable[Bytes]) {
//  def cloned: HashDigester =
//    copy(md = md.clone.asInstanceOf[MessageDigest])
//  def attach(bytes: Traversable[Bytes]): HashDigester =
//    copy(queued = bytes)
//  def data: Traversable[Bytes] =
//    source ++ queued.map{b => md.update(b.data, b.offset, b.length); b}
//  def digest: (Hash, Size) = ???
//}
//object HashDigester {
//  def apply(algorithm: String, source: Traversable[Bytes]): HashDigester = {
//    val md = MessageDigest.getInstance(algorithm)
//    source.foreach(b => md.update(b.data, b.offset, b.length))
//    HashDigester(md, source, Nil)
//  }
//}


//trait HashedSource extends Traversable[Bytes] {
//  /** consume all remaining bytes, then calculate digest */
//  def digest: (Hash, Size)
//  /** clone  */
//  def and(data: Traversable[Bytes]): HashedSource = ???
//  def after(data: Traversable[Bytes]): HashedSource = ???
//}
//
//case class HashDigest(algorithm: String, data: Seq[Bytes]) {
//  def digest: (Hash, Size) = ???
//  def and(data: Traversable[Bytes]): HashDigest = ???
//}

//object Hash {
//  def of(algorithm: String, source: BytesSource): { def digest: (Hash, Size) } = {
//    ???
//  }
//  def digest(algorithm: String, data: Seq[Bytes]) = {
//    ???
//  }
//  
//  def filter[U](algorithm: String)(source: BytesSource)(sink: BytesSource => U): (Hash, U) = {
//    val digest = MessageDigest getInstance algorithm
//    val u = sink(new BytesSource {
//      def next(numberOfBytes: Int): Bytes = {
//        val bytes = source next numberOfBytes
//        digest update (bytes data, bytes offset, bytes length)
//        bytes
//      }
//    })
//    (Hash(digest.digest()), u)
//  }
//}
