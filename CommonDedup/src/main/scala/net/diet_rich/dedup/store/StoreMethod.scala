package net.diet_rich.dedup.store

object StoreMethod {
  val STORE = 0
  val DEFLATE = 1
  val named = Map(
    "store" -> STORE,
    "deflate" -> DEFLATE
  )
  val names = named map (_.swap)
  assert(named.size == names.size)
}
