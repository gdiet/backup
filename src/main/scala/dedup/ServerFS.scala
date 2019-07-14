package dedup

class ServerFS(connection: java.sql.Connection, ds: DataStore) {
  private val db = new Database(connection)

  def entryAt(path: String): Option[FSEntry] = sync(lookup(path, db).map {
    _.as[FSEntry](new FSDir(db, _))((_, lastModified, start, stop) => new FSFile(ds, start, stop, lastModified))
  })

  def close(): Unit = db.close()
}

sealed trait FSEntry

class FSDir(db: Database, id: Long) extends FSEntry {
  def childNames: Seq[String] = sync(db.children(id)).map(_._2)
}

class FSFile(ds: DataStore, start: Long, stop: Long, val lastModifiedMillis: Long) extends FSEntry {
  def size: Long = stop - start
  def bytes(offset: Long, size: Int): Array[Byte] = {
    val bytesToRead = math.min(this.size - offset, size).toInt
    ds.read(start, bytesToRead)
  }
}
