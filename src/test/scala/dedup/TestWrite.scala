package dedup

object TestWrite extends App {
  val ds = new DataStore("data", "dedupfs-temp", false)
  ds.truncate(1, 2, None, 4)
  ds.write(1, 2, 0)(0, Array[Byte](1,2,3,4))
  val out = ds.read(1, 2, None)(0, 20)
  println(out.mkString(" "))
}
