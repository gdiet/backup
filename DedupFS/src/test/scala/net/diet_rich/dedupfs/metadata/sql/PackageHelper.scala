package net.diet_rich.dedupfs.metadata.sql

import net.diet_rich.common._
import net.diet_rich.common.sql.{H2, ConnectionFactory}
import net.diet_rich.common.test._
import net.diet_rich.dedupfs._

class TestDatabase(val connectionFactory: ConnectionFactory, val repositoryId: String)
  extends DatabaseRead with DatabaseWrite {
  override final def close() = !!!
}

trait PackageHelper { _: TestsHelper =>
  private val dbSettings = Map(hashAlgorithmKey -> "MD5", repositoryIdKey -> "testrepo")
  private def connections = init(H2.memoryFactory(className)) { Database.create("MD5", dbSettings)(_) }
  val db = new TestDatabase(connections, "testrepo")

  def afterAll(): Unit = db.connectionFactory close()
}
