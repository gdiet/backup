package net.diet_rich.backup.fs

/** Database wrapper to reducing the number of needed database methods.
 *  Possibly, this would also be the best place to add caching.
 */
class DedupDb(db: DedupSqlDb) {

  // EVENTUALLY consider synchronization that allows to "unget" a new entry on failure to insert it
  val nextEntry = new java.util.concurrent.atomic.AtomicLong( db.maxEntryID + 1 )
  
  
  def getFileId(path: String) : Option[Long] =
    path.split("/").tail
    .foldLeft(Option(0L))((parent, name) => parent flatMap ( getChild(_, name) ))
  
  def getPathString(id: Long) : Option[String] =
    getName(id) flatMap (name => getParent(id) 
      .flatMap (parent => getPathString(parent)
      .map (parentPath => parentPath + "/" + name)
    ))
    
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
      recursiveDelete (getChildren(id))
    }
    result
  }

  def setFileData(id: Long, time: Long, data: Long) : Boolean =
    db setFileData (id, time, data)
  def getLastModified(id: Long) : Option[Long] = db getFileData id map (_.time)
  def getFileDataId(id: Long) : Option[Long] = db getFileData id map (_.data)
  def clearFileData(id: Long) : Unit =
    db clearFileData id
    
}
