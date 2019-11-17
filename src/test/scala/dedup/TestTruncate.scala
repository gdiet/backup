package dedup

object TestTruncate extends App {
  val ds = new DataStore("data", "dedupfs-temp", false)
  ds.truncate(5, 6, None, 509742941)
  ds.truncate(3, 4, None, 4000)
  ds.truncate(1, 2, Some(2000L -> 4000L), 4000)
  ds.truncate(1, 2, Some(2000L -> 4000L), 3000)
  ds.truncate(1, 2, Some(2000L -> 4000L), 1000)
  ds.truncate(1, 2, Some(2000L -> 4000L), 2000)
}
