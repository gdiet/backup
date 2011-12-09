package net.diet_rich.backup.fs

/** Database wrapper to reducing the number of needed database methods. */
class DedupDb(db: DedupSqlDb) {

  // EVENTUALLY consider synchronization that allows to "unget" a new entry on failure to insert it
  val nextEntry = new java.util.concurrent.atomic.AtomicLong( db.maxEntryID + 1 )
  
  
  def getName(id: Long) : Option[String] = db.dbGetParentAndName(id) map (_ name)

  def getParent(id: Long) : Option[Long] = db.dbGetParentAndName(id) map (_ parent)

  def getChildren(id: Long) : List[Long] = db.dbGetChildrenIdAndName(id) map (_ id)

  def getChild(id: Long, childName: String) : Option[Long] =
    db.dbGetChildrenIdAndName(id) find (_.name == childName) map (_.id)

    def mkChild(id: Long, childName: String) : Option[Long] = {
    val childId = nextEntry.getAndIncrement()
    if (db.dbAddEntry(childId, id, childName)) Some(childId) else None
  }

  def rename(id: Long, newName: String) : Boolean = db rename (id, newName)

  def delete(id: Long) : Boolean = {
    val children = getChildren(id)
    val result = db delete id
    scala.actors.Futures.future {
      def recursiveDelete(children: List[Long]) : Unit = {
        children foreach (child =>
          try {
            recursiveDelete(getChildren (child))
            db delete child
          } catch { case e => println (e) }
        )
      }
      recursiveDelete (children)
    }
    result
  }

}
