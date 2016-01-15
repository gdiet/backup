package net.diet_rich.dedupfs.metadata.sql

import net.diet_rich.common._
import net.diet_rich.common.sql.{H2, ConnectionFactory}
import net.diet_rich.common.test._

trait PackageHelper { _: TestsHelper =>
  lazy val db = new {
    override final val connectionFactory: ConnectionFactory =
      init(H2.memoryFactory(className)) {
        Database.create("MD5", Map(SQLBackend.hashAlgorithmKey -> "MD5"))(_)
      }
  } with DatabaseRead with DatabaseWrite {
    override final def close() = !!!
  }
  def afterAll(): Unit = db.connectionFactory close()
}
