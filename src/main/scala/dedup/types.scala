package dedup

type DataId   = types.DataId;   val DataId   = types.DataId
type Time     = types.Time;     val Time     = types.Time
type DataArea = types.DataArea; val DataArea = types.DataArea

object types:

  opaque type DataArea = (Long, Long)
  object DataArea { def apply(start: Long, stop: Long): DataArea = (start, stop) }
  extension (a: DataArea)
    def start: Long = a._1
    def stop: Long = a._2
    def size: Long = stop - start
    def drop(difference: Long): DataArea = (start + difference, stop)
    def take(newSize: Long): DataArea = (start, start + newSize)

  opaque type DataId = Long
  object DataId { def apply(d: Long): DataId = d }
  extension (d: DataId) { def toLong: Long = d }

  opaque type Time = Long
  object Time { def apply(t: Long): Time = t }
  extension (t: Time)
    def nonZero: Time = if t == 0 then 1 else t
    @annotation.targetName("timeToLong")
    def toLong: Long = t
