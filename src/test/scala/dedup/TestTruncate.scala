package dedup

object TestTruncate extends App {
  val ds = new DataStore("data", "dedupfs-temp", false)
  ds.truncate(5, 6, 0, 509742941)
  ds.truncate(3, 4, 0, 4000)
  ds.truncate(1, 2, 2000, 4000)
  ds.truncate(1, 2, 2000, 3000)
  ds.truncate(1, 2, 2000, 1000)
  ds.truncate(1, 2, 2000, 2000)
}
