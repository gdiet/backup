package dedup
package server

class Level2(settings: Settings) extends AutoCloseable with util.ClassLogging {

  private val con = db.H2.connection(settings.dbDir, settings.readonly)
  private val database = db.Database(con)
  export database.child

  override def close(): Unit =
    // TODO see original
    // TODO warn about unclosed data entries and non-empty temp dir
    // TODO delete empty temp dir
    con.close()
  
}
