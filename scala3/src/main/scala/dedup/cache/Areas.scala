package dedup.cache

trait MemArea[M]:
  extension(m: M) def drop     (distance: Long): M
  extension(m: M) def dropRight(distance: Long): M
  extension(m: M) def length                   : Long
  extension(m: M) def split    (distance: Long): (M, M)
  extension(m: M) def take     (distance: Long): M

// given MemArea[Array[Byte]] with
//   extension(m: Array[Byte]) def drop     (distance: Long): M
//   extension(m: Array[Byte]) def dropRight(distance: Long): M
//   extension(m: Array[Byte]) def length                   : Long   = m.length
//   extension(m: Array[Byte]) def split    (distance: Long): (M, M)
//   extension(m: Array[Byte]) def take     (distance: Long): M
