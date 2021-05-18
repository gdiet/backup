package dedup
package server

class Level2(settings: Settings) extends AutoCloseable with util.ClassLogging {

  private val con = db.H2.connection(settings.dbDir, settings.readonly)
  private val database = db.Database(con)
  export database.child

  override def close(): Unit = ???
  
}
