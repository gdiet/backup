// Copyright (c) 2012 Georg Dietrich
// Licensed under the MIT license:
// http://www.opensource.org/licenses/mit-license.php
package net.diet_rich.sb

import com.google.common.cache.CacheLoader
import com.google.common.cache.LoadingCache
import com.google.common.cache.CacheBuilder
import java.lang.{Long => JLong}
import net.diet_rich.util.Configuration._
import scala.collection.immutable.Iterable
import df.DataInfo

class DataInfoDBCache protected(infoDB: DataInfoDB, config: StringMap) extends DataInfoDB {
  protected val cacheSize = config.long("DataInfoDB.cacheSize")
  
  protected val cache : LoadingCache[JLong, Option[DataInfo]] =
    CacheBuilder.newBuilder()
    .maximumSize(cacheSize)
    .build(new CacheLoader[JLong, Option[DataInfo]] {
      override def load(key: JLong) : Option[DataInfo] = infoDB readOption key
    })

  override def readOption(id: Long) : Option[DataInfo] = cache get id
  override def write(info: DataInfo) : Long = infoDB write info
}

object DataInfoDBCache {
  def apply(connection: java.sql.Connection, config: StringMap) : DataInfoDB =
    new DataInfoDBCache(DataInfoSqlDB(connection), config)
}
