package dedup.server

trait DataSink[D]:
  extension(d: D) def write(offset: Long, data: Array[Byte]): Unit

given DataSink[jnr.ffi.Pointer] with
  extension(d: jnr.ffi.Pointer) def write(offset: Long, data: Array[Byte]): Unit =
    d.put(offset, data, 0, data.length)
