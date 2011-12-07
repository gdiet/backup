package net.diet_rich.backup.fs

class DedupDb(db: DedupSqlDb) {

  // FIXME initialize with largest value in database + 1
  // EVENTUALLY consider synchronization that allows to "unget" a new entry on failure to insert it
  val nextEntry = new java.util.concurrent.atomic.AtomicLong(1)
  
  def getName(id: Long) : Option[String] = db.dbGetParentAndName(id) map (_ name)
  def getParent(id: Long) : Option[Long] = db.dbGetParentAndName(id) map (_ parent)
  def getChildren(id: Long) : List[Long] = db.dbGetChildrenIdAndName(id) map (_ id)
  def getChild(id: Long, childName: String) : Option[Long] =
    db.dbGetChildrenIdAndName(id) find (_.name == childName) map (_.id)
  def mkChild(id: Long, childName: String) : Option[Long] = {
    val childId = nextEntry.getAndIncrement()
    if (db.dbAddEntry(childId, id, childName)) Some(childId) else None
  }

}