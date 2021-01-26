package dedup2

// TODO use in LevelThree
/** A simple size limited LRU cache. */
class Cache[K, V](limit: Int) {
  private val map = new java.util.LinkedHashMap[K, V]() {
    override def removeEldestEntry(eldest: java.util.Map.Entry[K, V]): Boolean = size() > limit
  }
  // Remove then insert to update insertion order.
  def put    (key: K, value: V): Unit      = { map.remove(key); map.put(key, value) }
  def get    (key: K          ): Option[V] = Option(map.get(key))
  def delete (key: K          ): Unit      = map.remove(key)
}
