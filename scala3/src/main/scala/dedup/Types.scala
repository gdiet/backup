package dedup

// TODO use more type aliases
type Time = types.Time
val  Time = types.Time

object types:
  opaque type Time = Long
  object Time { def apply(t: Long): Time = t }
  extension (t: Time)
    def nonZero: Time = if t == 0 then 1 else t
    def toLong: Long = t
end types
