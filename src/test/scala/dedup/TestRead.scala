package dedup

object TestRead extends App {
  val ds = new DataStore("data", "dedupfs-temp", false)
  ds.write(1, 2, 0)(4, Array[Byte](1,2,3,4))
  ds.truncate(1, 2, None, 10)
  val out = ds.read(1, 2, None)(0, 20)
  println(out.mkString(" "))
}
