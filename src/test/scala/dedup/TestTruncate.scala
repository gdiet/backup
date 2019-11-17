package dedup

object TestTruncate extends App {
  val ds = new DataStore("data", "dedupfs-temp", false)
  ds.truncate(1, 2, Some(2000L -> 4000L), 4000)
}
