package dedup

type DataId = types.DataId
val  DataId = types.DataId
type Time   = types.Time
val  Time   = types.Time

object types:
  opaque type DataId = Long
  object DataId { def apply(d: Long): DataId = d }
  extension (d: DataId) { def toLong: Long = d }

  opaque type Time = Long
  object Time { def apply(t: Long): Time = t }
  extension (t: Time)
    def nonZero: Time = if t == 0 then 1 else t
    @annotation.targetName("timeToLong")
    def toLong: Long = t
end types
