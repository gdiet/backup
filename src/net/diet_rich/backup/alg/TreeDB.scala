package net.diet_rich.backup.alg

trait TreeDB {
  /** @return The child ID.
   *  @throws Exception if the child was not created correctly. */
  def createAndGetId(parentId: Long, name: String): Long
  /** @return The child ID if any. */
  def child(parentId: Long, childName: String): Option[Long]
  /** @throws Exception if the node was not updated correctly. */
  def setData(id: Long, time: Long, dataid: Long): Unit
}
