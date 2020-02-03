package dedup

object TestWrite extends App {
  val ds = new DataStore("data", "dedupfs-temp", false)
  ds.truncate(1, 2, 0, 8)
  ds.write(1, 2, 0)(2, Array[Byte](1,2,3,4))
  val out = ds.read(1, 2, Parts(Seq()))(0, 20)
  println(out.mkString(" "))
}
