package dedup.cache

object LazyAreasInSection extends App {
  val a = new Allocation()
  println("start")
  (0 until 10000000 by 10).foreach(p => a.allocate(p, 10))
  println("allocated")
  println(a.read(0, 10000000).size)
}
