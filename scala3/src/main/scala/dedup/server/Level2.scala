package dedup
package server

class Level2(settings: Settings) extends AutoCloseable with util.ClassLogging:

  private val con = db.H2.connection(settings.dbDir, settings.readonly)
  val database = db.Database(con)
  export database.{child, children, delete, mkDir, mkFile, setTime, update}

  override def close(): Unit =
    // TODO see original
    // TODO warn about unclosed data entries and non-empty temp dir
    // TODO delete empty temp dir
    con.close()

  /** Reads bytes from the referenced file.
    *
    * Note: The caller must make sure that no read beyond end-of-entry takes place
    * here because the behavior in that case is undefined. TODO define it.
    *
    * @param id           id of the file to read from.
    * @param dataId       dataId of the file to read from in case the file is not cached.
    * @param offset       offset in the file to start reading at.
    * @param size         number of bytes to read, NOT limited by the internal size limit for byte arrays.
    *
    * @return A contiguous LazyList(position, bytes) where data chunk size is limited to [[dedup.memChunk]].
    */
  def read(id: Long, dataId: Long, offset: Long, size: Long): LazyList[(Long, Array[Byte])] =
    ???
    // synchronized(files.get(id)) match {
    //   case None =>
    //     readFromLts(db.parts(dataId), offset, size)
    //   case Some(entry) =>
    //     lazy val ltsParts = db.parts(entry.baseDataId.get())
    //     entry.readUnsafe(offset, size)._2.flatMap {
    //       case Right(value) => LazyList(value)
    //       case Left(holeOffset -> holeSize) => readFromLts(ltsParts, holeOffset, holeSize)
    //     }
    // }
    
end Level2
